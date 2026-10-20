"""Bytes per record, same 10M rows, every format in this project.

Row formats are written record by record the way a service or a Kafka producer
would. Column formats get the whole table. That asymmetry is the point: the
column formats are allowed to see the data before deciding how to encode it.

    .venv/bin/python bench/01_size.py [--rows N]
"""

import argparse
import shutil
import io
import json
import struct

import fastavro
import lance
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

from common import guard, DATA, Timer, fmt_bytes, load, save_result

BATCH = 65_536


# ------------------------------------------------------------------ schemas

AVRO_SCHEMA = fastavro.parse_schema({
    "type": "record", "name": "Reading", "namespace": "bench",
    "fields": [
        {"name": "device_id", "type": "int"},
        {"name": "timestamp_us", "type": "long"},
        {"name": "location", "type": "string"},
        {"name": "firmware_version", "type": "string"},
        {"name": "status", "type": "string"},
        {"name": "temp_c", "type": "float"},
    ],
})


def proto_class():
    F = descriptor_pb2.FieldDescriptorProto
    fdp = descriptor_pb2.FileDescriptorProto(name="telemetry.proto", package="bench", syntax="proto3")
    msg = fdp.message_type.add(name="Reading")
    for i, (name, typ) in enumerate([
        ("device_id", F.TYPE_INT32), ("timestamp_us", F.TYPE_INT64),
        ("location", F.TYPE_STRING), ("firmware_version", F.TYPE_STRING),
        ("status", F.TYPE_STRING), ("temp_c", F.TYPE_FLOAT)], start=1):
        msg.field.add(name=name, number=i, type=typ, label=F.LABEL_OPTIONAL)
    pool = descriptor_pool.DescriptorPool()
    pool.Add(fdp)
    return message_factory.GetMessageClass(pool.FindMessageTypeByName("bench.Reading"))


# ------------------------------------------------------------------ records

def wire_records(table: pa.Table):
    """Dicts with an int64 microsecond timestamp, as Avro and Protobuf carry it."""
    for b in table.to_batches(BATCH):
        yield from b.set_column(1, "timestamp_us", b.column("timestamp").cast(pa.int64())).to_pylist()


def records(table: pa.Table):
    """Yield (dict for Avro/Protobuf, dict for JSON) per row, batch by batch."""
    for b in table.to_batches(BATCH):
        ts = b.column("timestamp")
        wire = b.set_column(1, "timestamp_us", ts.cast(pa.int64())).to_pylist()
        iso = pc.binary_join_element_wise(
            pc.replace_substring(ts.cast(pa.string()), " ", "T"), "Z", "").to_pylist()
        for r, s in zip(wire, iso):
            j = {k: v for k, v in r.items() if k != "timestamp_us"}
            j["timestamp"] = s
            yield r, j


def varint(n: int) -> bytes:
    out = bytearray()
    while n > 0x7F:
        out.append((n & 0x7F) | 0x80)
        n >>= 7
    out.append(n)
    return bytes(out)


# ---------------------------------------------------------------- measures

def measure_row_formats(table: pa.Table) -> dict[str, int]:
    Reading = proto_class()
    sizes = {"JSON lines": 0, "Protobuf (length-delimited)": 0,
             "Avro binary (Kafka wire, 5B registry header)": 0}
    with Timer() as t:
        for r, j in records(table):
            sizes["JSON lines"] += len(json.dumps(j, separators=(",", ":")).encode()) + 1
            m = Reading(**r)
            n = m.ByteSize()
            sizes["Protobuf (length-delimited)"] += len(varint(n)) + n
            buf = io.BytesIO()
            fastavro.schemaless_writer(buf, AVRO_SCHEMA, r)
            sizes["Avro binary (Kafka wire, 5B registry header)"] += 5 + buf.tell()
    print(f"  row formats encoded in {t.seconds:.0f}s")

    for codec in ("null", "deflate", "snappy", "zstandard"):
        path = DATA / f"telemetry.{codec}.avro"
        with Timer() as t, open(path, "wb") as fo:
            fastavro.writer(fo, AVRO_SCHEMA, wire_records(table), codec=codec)
        sizes[f"Avro container ({codec})"] = path.stat().st_size
        print(f"  avro/{codec}: {t.seconds:.0f}s")
    return sizes


def measure_column_formats(table: pa.Table) -> dict[str, int]:
    sizes = {}
    for codec in ("none", "snappy", "zstd"):
        path = DATA / f"telemetry.{codec}.parquet"
        pq.write_table(table, path, compression=codec)
        sizes[f"Parquet ({codec})"] = path.stat().st_size
    path = DATA / "telemetry.lance"
    shutil.rmtree(path, ignore_errors=True)  # Lance keeps old versions; size must be one version
    lance.write_dataset(table, path)
    sizes["Lance"] = sum(p.stat().st_size for p in path.rglob("*") if p.is_file())
    return sizes


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=None)
    args = ap.parse_args()
    guard(4, args.rows)

    table = load(args.rows)
    n = table.num_rows
    print(f"{n:,} rows")

    sizes = measure_row_formats(table) | measure_column_formats(table)
    json_bytes = sizes["JSON lines"]

    print(f"\n{'format':<48} {'total':>12} {'B/record':>10} {'vs JSON':>8}")
    for name, total in sizes.items():
        print(f"{name:<48} {fmt_bytes(total):>12} {total / n:>10.1f} {total / json_bytes:>7.1%}")

    save_result("01_size", {"rows": n, "bytes": sizes,
                            "bytes_per_record": {k: v / n for k, v in sizes.items()}}, partial=args.rows is not None)
