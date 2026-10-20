# /// script
# requires-python = ">=3.11"
# dependencies = ["fastavro", "pyarrow", "protobuf", "pyiceberg"]
# ///
"""Read the files written by generate.mjs with the reference libraries."""

import json
import shutil
import subprocess
from pathlib import Path

import fastavro
import pyarrow.compute as pc
import pyarrow.ipc as ipc
import pyarrow.parquet as pq
from fastavro.schema import fingerprint, to_parsing_canonical_form
from google.protobuf import descriptor_pb2, descriptor_pool, message_factory

OUT = Path(__file__).parent / "out"


def check_protobuf() -> None:
    """Rebuild the same message with the protobuf library and compare bytes."""
    fd = descriptor_pb2.FileDescriptorProto(name="reading.proto", syntax="proto3")
    F = descriptor_pb2.FieldDescriptorProto
    loc = fd.message_type.add(name="Location")
    loc.field.add(name="site", number=1, type=F.TYPE_STRING, label=F.LABEL_OPTIONAL)
    loc.field.add(name="floor", number=2, type=F.TYPE_INT32, label=F.LABEL_OPTIONAL)
    status = fd.enum_type.add(name="Status")
    for i, n in enumerate(["OK", "WARN", "FAULT"]):
        status.value.add(name=n, number=i)
    msg = fd.message_type.add(name="Reading")
    fields = [
        ("device_id", 1, F.TYPE_UINT64, F.LABEL_OPTIONAL, None),
        ("location", 2, F.TYPE_MESSAGE, F.LABEL_OPTIONAL, ".Location"),
        ("temps", 3, F.TYPE_FLOAT, F.LABEL_REPEATED, None),
        ("tags", 4, F.TYPE_STRING, F.LABEL_REPEATED, None),
        ("status", 5, F.TYPE_ENUM, F.LABEL_OPTIONAL, ".Status"),
        ("delta", 6, F.TYPE_SINT32, F.LABEL_OPTIONAL, None),
        ("neg", 7, F.TYPE_INT32, F.LABEL_OPTIONAL, None),
        ("d", 8, F.TYPE_DOUBLE, F.LABEL_OPTIONAL, None),
        ("on", 9, F.TYPE_BOOL, F.LABEL_OPTIONAL, None),
        ("zero", 10, F.TYPE_INT32, F.LABEL_OPTIONAL, None),
    ]
    for name, number, typ, label, type_name in fields:
        f = msg.field.add(name=name, number=number, type=typ, label=label)
        if type_name:
            f.type_name = type_name
    pool = descriptor_pool.DescriptorPool()
    pool.Add(fd)
    cls = message_factory.GetMessageClass(pool.FindMessageTypeByName("Reading"))
    ref = cls(device_id=4711, temps=[23.5, 19.25], tags=["a", "bb"], status=1, delta=-3, neg=-1, d=1.5, on=True, zero=0)
    ref.location.site = "hall-b"
    ref.location.floor = 2
    assert ref.SerializeToString() == (OUT / "reading.pb").read_bytes(), "protobuf bytes differ"
    print("protobuf: byte-identical to the protobuf library")


def check_protobuf_many() -> None:
    """Delimited stream and repeated wrapper message, parsed with the protobuf library."""
    from google.protobuf.internal.decoder import _DecodeVarint

    fd = descriptor_pb2.FileDescriptorProto(name="many.proto", syntax="proto3")
    F = descriptor_pb2.FieldDescriptorProto
    reading = fd.message_type.add(name="Reading")
    reading.field.add(name="device_id", number=1, type=F.TYPE_UINT64, label=F.LABEL_OPTIONAL)
    reading.field.add(name="location", number=2, type=F.TYPE_STRING, label=F.LABEL_OPTIONAL)
    reading.field.add(name="temp_c", number=3, type=F.TYPE_FLOAT, label=F.LABEL_OPTIONAL)
    wrapper = fd.message_type.add(name="Readings")
    wrapper.field.add(name="readings", number=1, type=F.TYPE_MESSAGE, label=F.LABEL_REPEATED, type_name=".Reading")
    pool = descriptor_pool.DescriptorPool()
    pool.Add(fd)
    reading_cls = message_factory.GetMessageClass(pool.FindMessageTypeByName("Reading"))
    wrapper_cls = message_factory.GetMessageClass(pool.FindMessageTypeByName("Readings"))
    expected = [reading_cls(**r) for r in json.loads((OUT / "pb_many.json").read_text())]

    data = (OUT / "readings.delimited.pb").read_bytes()
    got, pos = [], 0
    while pos < len(data):
        n, pos = _DecodeVarint(data, pos)
        got.append(reading_cls.FromString(data[pos : pos + n]))
        pos += n
    assert got == expected, "delimited stream differs"
    assert data == b"".join(m.ByteSize().to_bytes(1, "little") + m.SerializeToString() for m in expected)

    data = (OUT / "readings.wrapper.pb").read_bytes()
    assert list(wrapper_cls.FromString(data).readings) == expected, "wrapper message differs"
    assert data == wrapper_cls(readings=expected).SerializeToString(), "wrapper bytes differ"
    print(f"protobuf: delimited stream and repeated wrapper of {len(expected)} readings byte-identical")


def check_decode_raw() -> None:
    """Schema-less decoding compared with protoc --decode_raw, including where it fails."""
    protoc = shutil.which("protoc")
    if not protoc:
        print("protobuf decode_raw: SKIPPED, protoc not on PATH")
        return
    expected = json.loads((OUT / "decode_raw.json").read_text())
    for name, text in expected.items():
        res = subprocess.run([protoc, "--decode_raw"], input=(OUT / f"raw_{name}.bin").read_bytes(), capture_output=True)
        got = res.stdout.decode() if res.returncode == 0 else None
        assert got == text, f"decode_raw {name}: explorer {text!r} vs protoc {got!r}"
    print(f"protobuf decode_raw: {len(expected)} inputs identical to protoc --decode_raw, failures included")


def check_iceberg_bounds(table, expected: list) -> None:
    """Manifest lower/upper bounds: pyiceberg reads the same bytes its own serializer writes for each file's min/max."""
    from pyiceberg.conversions import to_bytes
    from pyiceberg.types import FloatType, IntegerType, LongType, StringType

    types = {"int": IntegerType(), "long": LongType(), "float": FloatType(), "string": StringType()}
    files = {r["file_path"]: r for r in table.inspect.files().to_pylist()}
    assert len(files) == len(expected), "iceberg data file count differs"
    for b in expected:
        got = files[b["path"]]
        lower, upper = dict(got["lower_bounds"]), dict(got["upper_bounds"])
        for f in b["fields"]:
            t = types[f["type"]]
            assert lower[f["id"]] == to_bytes(t, f["min"]), f"lower bound of field {f['id']} in {b['path']}"
            assert upper[f["id"]] == to_bytes(t, f["max"]), f"upper bound of field {f['id']} in {b['path']}"
    print(f"iceberg: manifest bounds of {len(expected)} data files match pyiceberg's serialization of their min/max")


def check_iceberg() -> None:
    """Open the table with pyiceberg: schema evolution resolved by field id, every file read through the metadata."""
    from pyiceberg.table import StaticTable

    exp = json.loads((OUT / "iceberg_expected.json").read_text())
    table = StaticTable.from_metadata(exp["metadata"])
    assert [f.name for f in table.schema().fields] == exp["columns"], "iceberg schema differs"
    assert [str(f.field_type) for f in table.schema().fields] == exp["types"], "iceberg types differ"
    got = table.scan().to_arrow()
    assert got.column_names == exp["columns"]
    rows = [list(r.values()) for r in got.to_pylist()]
    assert sorted(map(json.dumps, rows)) == sorted(map(json.dumps, exp["rows"])), "iceberg rows differ"
    assert len(table.snapshots()) == exp["snapshots"]
    assert table.scan(snapshot_id=exp["firstSnapshot"]).to_arrow().num_rows == exp["firstRows"]
    assert exp["rejected"], "string to int must be rejected"
    for path in (OUT / "iceberg").rglob("*.avro"):
        with open(path, "rb") as f:
            list(fastavro.reader(f))
    check_iceberg_bounds(table, exp["bounds"])
    print(f"iceberg: pyiceberg reads {len(rows)} rows across {exp['snapshots']} snapshots, "
          f"columns {exp['columns']} after rename, add, widen and drop; time travel to the snapshot before the schema changes sees {exp['firstRows']} rows")

    for run in json.loads((OUT / "iceberg_growth.json").read_text()):
        for kind, v in run["kinds"].items():
            err = v["model"] / v["real"] - 1
            assert abs(err) < 0.03 and v["realFiles"] == v["modelFiles"], f"growth model {kind} merge={run['merge']}: {err:+.2%}"
        worst = max(abs(v["model"] / v["real"] - 1) for v in run["kinds"].values())
        print(f"iceberg growth model, merge={run['merge']}: within {worst:.2%} of {run['commits']} real commits")


def check_avro() -> None:
    """Read both container files and compare the schema fingerprint."""
    expected = json.loads((OUT / "avro_records.json").read_text())
    for codec in ["null", "deflate"]:
        with open(OUT / f"readings.{codec}.avro", "rb") as f:
            reader = fastavro.reader(f)
            got = list(reader)
        for rec in got:
            rec["id"] = rec["id"].decode()
        assert got == expected, f"avro {codec}: records differ"
        print(f"avro {codec}: fastavro reads {len(got)} records")
    fp = json.loads((OUT / "fingerprint.json").read_text())
    with open(OUT / "readings.null.avro", "rb") as f:
        schema = fastavro.reader(f).writer_schema
    canonical = to_parsing_canonical_form(fastavro.parse_schema(schema))
    assert canonical == fp["canonical"], "parsing canonical form differs"
    little_endian_hex = fingerprint(canonical, "CRC-64-AVRO")
    assert int.from_bytes(bytes.fromhex(little_endian_hex), "little") == int(fp["fingerprint"]), "fingerprint differs"
    print("avro: parsing canonical form and CRC-64-AVRO fingerprint match")


def check_arrow() -> None:
    """Open the IPC file, validate every batch and compare the rows."""
    expected = json.loads((OUT / "arrow_rows.json").read_text())
    with ipc.open_file(OUT / "readings.arrow") as reader:
        batches = [reader.get_batch(i) for i in range(reader.num_record_batches)]
    for b in batches:
        b.validate(full=True)
    import pyarrow as pa

    table = pa.Table.from_batches(batches)
    assert table.to_pylist() == expected, "arrow rows differ"
    with pa.memory_map(str(OUT / "readings.arrow")) as source:
        mapped = ipc.open_file(source).read_all()
    assert mapped.equals(table)
    print(f"arrow: {len(batches)} record batches, {table.num_rows} rows, validated, memory-mapped read matches")


def check_parquet() -> None:
    """Read every variant, compare rows, statistics and row group pruning."""
    rows = json.loads((OUT / "parquet_rows.json").read_text())
    nullable = json.loads((OUT / "nullable_rows.json").read_text())
    f = pq.ParquetFile(OUT / "nullable.parquet")
    assert f.read().to_pylist() == nullable, "nullable parquet differs"
    stats = f.metadata.row_group(0).column(2).statistics
    assert stats.null_count == sum(1 for r in nullable[:128] if r["temp_c"] is None)
    print(f"parquet nullable: {f.metadata.num_rows} rows with nulls, {f.schema_arrow}".replace("\n", ", "))
    for path in sorted(OUT.glob("*.parquet")):
        if path.stem == "nullable":
            continue
        f = pq.ParquetFile(path)
        table = f.read()
        assert table.num_rows == len(rows)
        got = sorted(table.drop_columns(["ts"]).to_pylist(), key=lambda r: json.dumps(r, sort_keys=True))
        want = sorted(({k: v for k, v in r.items() if k != "ts"} for r in rows), key=lambda r: json.dumps(r, sort_keys=True))
        assert got == want, f"{path.name}: rows differ"
        assert pc.min(table["ts"]).value == min(r["ts"] for r in rows)
        plan = json.loads(path.with_suffix(".plan.json").read_text())
        temp = f.schema_arrow.get_field_index("temp_c")
        kept = [f.metadata.row_group(i).column(temp).statistics.max > 40 for i in range(f.num_row_groups)]
        assert kept == plan["read"], f"{path.name}: row group pruning differs"
        filtered = pq.read_table(path, filters=[("temp_c", ">", 40)]).num_rows
        assert filtered == plan["matched"], f"{path.name}: filter result differs"
        rg = f.metadata.row_group(0)
        print(f"parquet {path.stem}: {f.num_row_groups} row groups, encodings {rg.column(1).encodings}, "
              f"sorting {[s.column_index for s in rg.sorting_columns]}, row groups read for temp_c > 40: {sum(kept)}")


if __name__ == "__main__":
    check_protobuf()
    check_protobuf_many()
    check_decode_raw()
    check_avro()
    check_iceberg()
    check_parquet()
    check_arrow()
    print("all checks passed")
