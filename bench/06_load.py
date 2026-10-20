"""Time to get 10M rows into memory for analysis: JSON vs Parquet, into pandas
and into Arrow. Also what they weigh once they are there.

Arrow is not competing with Parquet here. Parquet minimises bytes at rest,
Arrow minimises cycles at runtime; `pq.read_table` is the pipe between them.

    .venv/bin/python bench/06_load.py [--rows N]
"""

import argparse
import json

import pandas as pd
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.json
import pyarrow.parquet as pq

from common import guard, warm_cache, DATA, Timer, fmt_bytes, load, save_result

BATCH = 65_536


def write_jsonl(table: pa.Table, path) -> None:
    with open(path, "w") as fo:
        for b in table.to_batches(BATCH):
            ts = b.column("timestamp")
            iso = pc.binary_join_element_wise(
                pc.replace_substring(ts.cast(pa.string()), " ", "T"), "Z", "")
            b = b.set_column(1, "timestamp", iso)
            fo.writelines(json.dumps(r, separators=(",", ":")) + "\n" for r in b.to_pylist())


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=None)
    args = ap.parse_args()
    guard(8, args.rows)

    table = load(args.rows)
    n = table.num_rows
    jsonl = DATA / f"load.{n}.jsonl"
    parquet = DATA / "load.parquet"
    if not jsonl.exists():
        with Timer() as t:
            write_jsonl(table, jsonl)
        print(f"wrote {jsonl.name} in {t.seconds:.0f}s")
    pq.write_table(table, parquet, compression="zstd")
    del table
    print(f"{n:,} rows. JSON lines {fmt_bytes(jsonl.stat().st_size)}, "
          f"Parquet (zstd) {fmt_bytes(parquet.stat().st_size)}")

    warm_cache(jsonl, parquet)  # this measures parsing, not the disk

    results = {}

    with Timer() as t:
        # Chunked, because parsing 10M rows of JSON into Python objects in one go
        # needs more memory than most laptops have. That is part of the finding.
        df = pd.concat(pd.read_json(jsonl, lines=True, chunksize=500_000), ignore_index=True)
    results["JSON -> pandas (chunked)"] = {"seconds": t.seconds, "cpu_s": t.cpu, "in_memory": int(df.memory_usage(deep=True).sum())}
    del df

    with Timer() as t:
        at = pa.json.read_json(jsonl)
    results["JSON -> Arrow (pyarrow.json, multithreaded)"] = {"seconds": t.seconds, "cpu_s": t.cpu, "in_memory": at.nbytes}
    del at

    with Timer() as t:
        at = pq.read_table(parquet)
    results["Parquet -> Arrow"] = {"seconds": t.seconds, "cpu_s": t.cpu, "in_memory": at.nbytes}

    with Timer() as t:
        df = at.to_pandas()
    results["  Arrow -> pandas (conversion only)"] = {"seconds": t.seconds, "cpu_s": t.cpu,
                                                     "in_memory": int(df.memory_usage(deep=True).sum())}
    del df

    with Timer() as t:
        df = at.to_pandas(types_mapper=pd.ArrowDtype)
    results["  Arrow -> pandas (ArrowDtype, no copy)"] = {"seconds": t.seconds, "cpu_s": t.cpu,
                                                         "in_memory": int(df.memory_usage(deep=True).sum())}
    del df, at

    base = results["JSON -> pandas (chunked)"]["seconds"]
    print(f"\n{'path':<48} {'wall':>9} {'CPU':>9} {'vs JSON->pandas':>16} {'in memory':>12}")
    for name, r in results.items():
        print(f"{name:<48} {r['seconds']:>7.2f} s {r['cpu_s']:>7.2f} s {base / r['seconds']:>15.0f}x "
              f"{fmt_bytes(r['in_memory']):>12}")

    save_result("06_load", {"rows": n, "jsonl_bytes": jsonl.stat().st_size,
                            "parquet_bytes": parquet.stat().st_size, "results": results}, partial=args.rows is not None)
