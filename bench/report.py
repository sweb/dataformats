"""One summary table from all results/*.json.

    .venv/bin/python bench/report.py
"""

import json

from common import RESULTS, fmt_bytes


def load(name):
    p = RESULTS / f"{name}.json"
    return json.loads(p.read_text()) if p.exists() else None


if __name__ == "__main__":
    rows = []
    if r := load("01_size"):
        b = r["bytes_per_record"]
        rows += [("Protobuf", "bytes/record vs JSON", f"{b['Protobuf (length-delimited)']:.0f} vs {b['JSON lines']:.0f}",
                  f"{b['JSON lines'] / b['Protobuf (length-delimited)']:.1f}x smaller"),
                 ("Avro", "bytes/record, container (deflate) vs JSON", f"{b['Avro container (deflate)']:.0f} vs {b['JSON lines']:.0f}",
                  f"{b['JSON lines'] / b['Avro container (deflate)']:.0f}x smaller"),
                 ("Parquet", "bytes/record (zstd) vs JSON", f"{b['Parquet (zstd)']:.1f} vs {b['JSON lines']:.0f}",
                  f"{b['JSON lines'] / b['Parquet (zstd)']:.0f}x smaller")]
    if r := load("02_kafka"):
        j, a = r["results"]["json"], r["results"]["avro"]
        if j.get("broker_bytes"):
            rows.append(("Avro", f"Kafka broker disk, {r['rows']:,} records ({r['compression']})",
                         f"{fmt_bytes(j['broker_bytes'])} vs {fmt_bytes(a['broker_bytes'])}",
                         f"{j['broker_bytes'] / a['broker_bytes']:.1f}x"))
    if r := load("03_scan"):
        q1 = r["q1"]
        first = next(iter(q1.values()))["bytes"]
        best = min(v["bytes"] for v in q1.values())
        fk = next(iter(q1)); bk = min(q1, key=lambda k: q1[k]["bytes"])
        cpu = (f", {q1[fk]['cpu_s'] / q1[bk]['cpu_s']:.0f}x CPU" if q1[fk].get("cpu_s") else "")
        rows.append(("Parquet", "bytes fetched, avg(temp_c) WHERE location=…, JSON vs best layout",
                     f"{fmt_bytes(first)} vs {fmt_bytes(best)}", f"{first / best:.0f}x bytes{cpu}"))
        q2 = r["q2"]
        rows.append(("Parquet", "same table, WHERE temp_c > 40: sorted by location vs by temp_c",
                     f"{fmt_bytes(q2['sorted by location']['bytes'])} vs {fmt_bytes(q2['sorted by temp_c']['bytes'])}",
                     f"{q2['sorted by location']['bytes'] / q2['sorted by temp_c']['bytes']:.0f}x  (one sort order)"))
    if r := load("04_evolution_cost"):
        ice = r["iceberg"]["add column humidity (no default)"]
        pl = r["parquet_rewrite"]
        rows.append(("Iceberg", f"add a column to {r['rows']:,} rows: data bytes rewritten",
                     f"Iceberg {fmt_bytes(ice['data_bytes_written'])} vs Parquet {fmt_bytes(pl['data_bytes_written'])}",
                     f"{ice['seconds']:.2f}s vs {pl['seconds']:.1f}s"))
    if r := load("06_load"):
        res = r["results"]
        j, a, p_ = res["JSON -> pandas (chunked)"], res["JSON -> Arrow (pyarrow.json, multithreaded)"], res["Parquet -> Arrow"]
        rows.append(("Arrow", "load to memory, JSON->Arrow vs Parquet->Arrow, CPU seconds",
                     f"{a['cpu_s']:.1f}s vs {p_['cpu_s']:.2f}s", f"{a['cpu_s'] / p_['cpu_s']:.0f}x"))
        rows.append(("Arrow", "  same, via pandas' JSON parser, wall",
                     f"{j['seconds']:.1f}s vs {p_['seconds']:.2f}s", f"{j['seconds'] / p_['seconds']:.0f}x"))
    if r := load("05_random_rows"):
        s = r["results"]
        rows.append(("Lance", f"{r['lookups']:,} random rows by position, Parquet vs Lance",
                     f"{s['Parquet point']['seconds'] * 1000:.0f} ms vs {s['Lance point']['seconds'] * 1000:.0f} ms",
                     f"{s['Parquet point']['seconds'] / s['Lance point']['seconds']:.0f}x wall, "
                     f"{s['Parquet point']['cpu_s'] / s['Lance point']['cpu_s']:.0f}x CPU"))

    w = [max(len(x[i]) for x in rows) for i in range(4)]
    for fmt, what, val, delta in rows:
        print(f"{fmt:<{w[0]}}  {what:<{w[1]}}  {val:<{w[2]}}  {delta}")
