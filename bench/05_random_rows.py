"""Fetch 1000 random rows by position: Parquet vs Lance.

This is the access pattern of ML training (shuffle, then fetch a batch of
row ids) and of vector search (the ANN index returns row ids, then you need
the rows). Parquet must decode a whole column chunk to yield one value.
Lance is built to fetch by row id.

Two shapes, because both happen in practice:

  point   1000 separate lookups, one row each      (a DataLoader, a serving path)
  batch   one call with all 1000 ids               (a training step)

    .venv/bin/python bench/05_random_rows.py [--rows N] [--lookups N]
"""

import argparse
import shutil
import random

import lance
import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from common import guard, warm_cache, DATA, CountingFS, Timer, fmt_bytes, load, save_result

ROW_GROUP = 100_000
CODEC = "zstd"


def parquet_point(path, indices) -> pa.Table:
    pf = pq.ParquetFile(path)
    # take, not slice: a slice is a view that keeps the whole decoded row
    # group alive, and 1000 of those is gigabytes.
    return pa.concat_tables(
        pf.read_row_group(i // ROW_GROUP).take([i % ROW_GROUP]) for i in indices)


def parquet_batch(path, indices) -> pa.Table:
    return ds.dataset(path, format="parquet").take(sorted(indices))


def lance_point(path, indices) -> pa.Table:
    d = lance.dataset(path)
    return pa.concat_tables(d.take([i]) for i in indices)


def lance_batch(path, indices) -> pa.Table:
    return lance.dataset(path).take(sorted(indices))


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=None)
    ap.add_argument("--lookups", type=int, default=1000)
    args = ap.parse_args()
    guard(4, args.rows)

    table = load(args.rows)
    n = table.num_rows
    pq_path = DATA / "rows.parquet"
    lance_path = DATA / "rows.lance"
    pq.write_table(table, pq_path, compression=CODEC, row_group_size=ROW_GROUP)
    shutil.rmtree(lance_path, ignore_errors=True)  # Lance keeps old versions; size must be one version
    lance.write_dataset(table, lance_path)
    pq_size = pq_path.stat().st_size
    lance_size = sum(p.stat().st_size for p in lance_path.rglob("*") if p.is_file())
    print(f"{n:,} rows. Parquet {fmt_bytes(pq_size)} (row groups of {ROW_GROUP:,}), "
          f"Lance {fmt_bytes(lance_size)}")

    indices = random.Random(7).sample(range(n), args.lookups)
    expected = table.take(sorted(indices))

    warm_cache(pq_path, lance_path)  # this measures decoding, not the disk

    results = {}
    for name, fn, path in [("Parquet point", parquet_point, pq_path),
                           ("Lance point", lance_point, lance_path),
                           ("Parquet batch", parquet_batch, pq_path),
                           ("Lance batch", lance_batch, lance_path)]:
        with Timer() as t:
            got = fn(path, indices)
        assert got.num_rows == args.lookups
        if "batch" in name:
            assert got.equals(expected), name
        results[name] = {"seconds": t.seconds, "cpu_s": t.cpu}

    # How many bytes did Parquet have to touch for the point lookups?
    fs = CountingFS()
    fs.reset()
    pf = pq.ParquetFile(fs.arrow_fs.open_input_file(str(pq_path)))
    for i in indices:
        pf.read_row_group(i // ROW_GROUP).take([i % ROW_GROUP])
    pq_point_bytes = fs.bytes_read

    print(f"\n{args.lookups:,} random rows")
    print(f"{'':<16} {'wall':>10} {'per row':>10} {'CPU':>10}")
    for name, r in results.items():
        print(f"{name:<16} {r['seconds'] * 1000:>8.0f} ms {r['seconds'] / args.lookups * 1e6:>7.0f} us "
              f"{r['cpu_s'] * 1000:>7.0f} ms")
    print(f"\nParquet point lookups decoded {fmt_bytes(pq_point_bytes)} "
          f"to return {args.lookups:,} rows ({fmt_bytes(pq_point_bytes / args.lookups)} per row). "
          f"The file is {fmt_bytes(pq_size)}.")
    pp, lp = results["Parquet point"], results["Lance point"]
    pb, lb = results["Parquet batch"], results["Lance batch"]
    print(f"Lance vs Parquet, wall: point {pp['seconds'] / lp['seconds']:.0f}x, batch {pb['seconds'] / lb['seconds']:.1f}x;  "
          f"CPU: point {pp['cpu_s'] / lp['cpu_s']:.0f}x, batch {pb['cpu_s'] / lb['cpu_s']:.1f}x")

    save_result("05_random_rows", {"rows": n, "lookups": args.lookups, "row_group_size": ROW_GROUP,
                                   "results": results, "parquet_point_bytes_read": pq_point_bytes,
                                   "parquet_bytes": pq_size, "lance_bytes": lance_size}, partial=args.rows is not None)
