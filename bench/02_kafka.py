"""Kafka broker disk and produce throughput, JSON vs Avro, same 10M records.

Needs a broker. Both topics use the same partition count, replication and
compression, so the only variable is the serialisation. The Avro messages are
framed exactly as the Confluent/Apicurio serializers frame them: a magic byte,
a 4-byte schema id, then the bare Avro datum. No registry needs to be running
for the bytes to be right.

Broker disk is read via `kafka-log-dirs --describe`, which asks the broker for
the on-disk size of every partition (log segments plus indexes). Pass the
command that gets you that JSON, for example:

  --log-dirs-cmd "kubectl -n kafka exec simple-kafka-broker-default-0 -c kafka -- \\
      bin/kafka-log-dirs.sh --describe --bootstrap-server localhost:9092 --topic-list {topics}"

    .venv/bin/python bench/02_kafka.py --bootstrap HOST:PORT [--rows N] [--compression zstd] \\
        --log-dirs-cmd "..."
"""

import argparse
import io
import json
import subprocess
import time

import fastavro
import pyarrow as pa
import pyarrow.compute as pc
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

from common import guard, Timer, fmt_bytes, load, save_result

BATCH = 65_536
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
REGISTRY_HEADER = b"\x00" + (1).to_bytes(4, "big")


def encoded(table: pa.Table, fmt: str):
    for b in table.to_batches(BATCH):
        ts = b.column("timestamp")
        if fmt == "json":
            iso = pc.binary_join_element_wise(
                pc.replace_substring(ts.cast(pa.string()), " ", "T"), "Z", "")
            for r in b.set_column(1, "timestamp", iso).to_pylist():
                yield json.dumps(r, separators=(",", ":")).encode()
        else:
            for r in b.set_column(1, "timestamp_us", ts.cast(pa.int64())).to_pylist():
                buf = io.BytesIO()
                fastavro.schemaless_writer(buf, AVRO_SCHEMA, r)
                yield REGISTRY_HEADER + buf.getvalue()


def produce(bootstrap: str, topic: str, payloads, compression: str) -> dict:
    p = Producer({"bootstrap.servers": bootstrap, "compression.type": compression,
                  "linger.ms": 50, "batch.size": 1_000_000, "queue.buffering.max.messages": 500_000,
                  "acks": "all"})
    count = payload_bytes = 0
    with Timer() as t:
        for msg in payloads:
            while True:
                try:
                    p.produce(topic, msg)
                    break
                except BufferError:
                    p.poll(0.05)
            count += 1
            payload_bytes += len(msg)
            if count % 10_000 == 0:
                p.poll(0)
        p.flush()
    return {"records": count, "payload_bytes": payload_bytes, "seconds": t.seconds,
            "records_per_s": count / t.seconds, "payload_MiB_per_s": payload_bytes / t.seconds / 2**20}


def log_dir_bytes(cmd_template: str, topics: list[str]) -> dict[str, int]:
    out = subprocess.run(cmd_template.format(topics=",".join(topics)), shell=True,
                         check=True, capture_output=True, text=True).stdout
    # kafka-log-dirs prints a banner line before the JSON.
    doc = json.loads(out[out.index("{"):])
    sizes = {t: 0 for t in topics}
    for broker in doc["brokers"]:
        for ld in broker["logDirs"]:
            for part in ld.get("partitions", []):
                topic = part["partition"].rsplit("-", 1)[0]
                if topic in sizes:
                    sizes[topic] += part["size"]
    return sizes


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", required=True)
    ap.add_argument("--rows", type=int, default=None)
    ap.add_argument("--compression", default="zstd", choices=["none", "gzip", "snappy", "lz4", "zstd"])
    ap.add_argument("--partitions", type=int, default=1)
    ap.add_argument("--log-dirs-cmd", default=None,
                    help="shell command printing kafka-log-dirs --describe JSON; {topics} is substituted")
    ap.add_argument("--settle", type=float, default=10.0, help="seconds to wait before reading log dirs")
    args = ap.parse_args()
    guard(4, args.rows)

    table = load(args.rows)
    n = table.num_rows
    topics = {"json": f"bench-json-{args.compression}", "avro": f"bench-avro-{args.compression}"}

    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    existing = set(admin.list_topics(timeout=10).topics)
    for f in admin.delete_topics([t for t in topics.values() if t in existing]).values():
        f.result()
    time.sleep(2)
    for f in admin.create_topics([NewTopic(t, num_partitions=args.partitions, replication_factor=1,
                                           config={"compression.type": "producer"})
                                  for t in topics.values()]).values():
        f.result()

    results = {}
    for fmt, topic in topics.items():
        print(f"producing {n:,} {fmt} records to {topic} ...")
        results[fmt] = produce(args.bootstrap, topic, encoded(table, fmt), args.compression)
        r = results[fmt]
        print(f"  {r['seconds']:.0f}s, {r['records_per_s']:,.0f} rec/s, {r['payload_MiB_per_s']:.1f} MiB/s payload")

    if args.log_dirs_cmd:
        time.sleep(args.settle)
        sizes = log_dir_bytes(args.log_dirs_cmd, list(topics.values()))
        for fmt, topic in topics.items():
            results[fmt]["broker_bytes"] = sizes[topic]

    print(f"\n{n:,} records, producer compression = {args.compression}")
    print(f"{'':<6} {'payload':>12} {'B/rec':>7} {'broker disk':>12} {'B/rec':>7} {'rec/s':>10}")
    for fmt, r in results.items():
        disk = r.get("broker_bytes")
        print(f"{fmt:<6} {fmt_bytes(r['payload_bytes']):>12} {r['payload_bytes'] / n:>7.1f} "
              f"{fmt_bytes(disk) if disk else '-':>12} {f'{disk / n:.1f}' if disk else '-':>7} "
              f"{r['records_per_s']:>10,.0f}")
    if results["json"].get("broker_bytes"):
        print(f"\nbroker disk, JSON / Avro: {results['json']['broker_bytes'] / results['avro']['broker_bytes']:.2f}x")
    else:
        print("\nno --log-dirs-cmd given: broker disk not measured")

    save_result("02_kafka", {"rows": n, "compression": args.compression, "topics": topics, "results": results}, partial=args.rows is not None)
