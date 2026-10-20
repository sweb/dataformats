"""Bytes fetched from object storage for one query, four physical layouts of
the same 10M rows. Object storage bills per byte fetched and per request,
and nothing else you do matters as much as this.

  Q1  SELECT avg(temp_c) FROM t WHERE location = 'hall-a'
  Q2  SELECT avg(temp_c) FROM t WHERE temp_c > 40

Q1 climbs the ladder: full read -> column pruning -> sorted -> partitioned.
Q2 shows the bill for the ladder: the layout that is perfect for Q1 is useless
for Q2, and you only get one sort order per table.

    .venv/bin/python bench/03_scan.py [--rows N] [--eur-per-tb X] [--queries-per-day N]
"""

import argparse
import json
import shutil

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pyarrow.parquet as pq

from common import guard, DATA, RESULTS, CountingFS, Timer, fmt_bytes, load, save_result

ROW_GROUP = 100_000
CODEC = "zstd"
Q1 = ds.field("location") == "hall-a"
Q2 = ds.field("temp_c") > 40.0


# ------------------------------------------------------------------ layouts

def write_layouts(table: pa.Table) -> dict[str, str]:
    opts = dict(compression=CODEC, row_group_size=ROW_GROUP)
    paths = {}

    p = DATA / "scan_time_ordered.parquet"
    pq.write_table(table, p, **opts)
    paths["time-ordered"] = str(p)

    p = DATA / "scan_sorted_location.parquet"
    pq.write_table(table.sort_by([("location", "ascending"), ("timestamp", "ascending")]), p, **opts)
    paths["sorted by location"] = str(p)

    p = DATA / "scan_sorted_temp.parquet"
    pq.write_table(table.sort_by([("temp_c", "ascending")]), p, **opts)
    paths["sorted by temp_c"] = str(p)

    p = DATA / "scan_partitioned"
    shutil.rmtree(p, ignore_errors=True)
    ds.write_dataset(table, p, format="parquet",
                     partitioning=ds.partitioning(pa.schema([("location", pa.string())]), flavor="hive"),
                     file_options=ds.ParquetFileFormat().make_write_options(compression=CODEC),
                     min_rows_per_group=ROW_GROUP, max_rows_per_group=ROW_GROUP,
                     max_rows_per_file=ROW_GROUP * 100)
    paths["partitioned by location"] = str(p)
    return paths


# ------------------------------------------------------------------ measure

def scan(path: str, columns, filt, fs: CountingFS, partitioned=False) -> dict:
    """Run the query the way a lakehouse engine would: projection + predicate
    pushdown into the Parquet reader, aggregate what comes back."""
    fs.reset()
    dataset = ds.dataset(path, format="parquet", filesystem=fs.arrow_fs,
                         partitioning="hive" if partitioned else None)
    groups_total = sum(f.num_row_groups for f in dataset.get_fragments())
    groups_read = sum(f.subset(filt, schema=dataset.schema).num_row_groups
                      for f in dataset.get_fragments(filter=filt))
    with Timer() as tm:
        t = dataset.to_table(columns=columns, filter=filt)
        avg = pc.mean(t.column("temp_c")).as_py()
    return {"bytes": fs.bytes_read, "requests": fs.requests, "rows": t.num_rows,
            "row_groups": f"{groups_read}/{groups_total}", "avg": avg, "cpu_s": tm.cpu}


def full_read_then_filter(path: str, filt, fs: CountingFS) -> dict:
    """No pushdown at all: read every column of every row group, filter in memory.
    This is what a `SELECT *` into pandas does, and what a row format forces."""
    fs.reset()
    with Timer() as tm:
        t = pq.read_table(path, filesystem=fs.arrow_fs).filter(filt)
        avg = pc.mean(t.column("temp_c")).as_py()
    return {"bytes": fs.bytes_read, "requests": fs.requests, "rows": t.num_rows,
            "row_groups": "all", "avg": avg, "cpu_s": tm.cpu}


def json_scan(path, fs: CountingFS) -> dict:
    """The JSON baseline, actually executed: parse the whole file (pyarrow's
    multithreaded parser, the fastest honest choice), filter, aggregate."""
    import pyarrow.json
    with Timer() as tm:
        t = pa.json.read_json(path)
        t = t.filter(pc.equal(t.column("location"), "hall-a"))
        avg = pc.mean(t.column("temp_c")).as_py()
    return {"bytes": path.stat().st_size, "requests": "-", "rows": t.num_rows,
            "row_groups": "-", "avg": avg, "cpu_s": tm.cpu}


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=None)
    ap.add_argument("--eur-per-tb", type=float, default=None,
                    help="price per TB scanned; state its source whenever you quote the result")
    ap.add_argument("--queries-per-day", type=int, default=1000)
    args = ap.parse_args()
    guard(6, args.rows)

    table = load(args.rows)
    n = table.num_rows
    print(f"{n:,} rows, row groups of {ROW_GROUP:,}, {CODEC}")
    paths = write_layouts(table)
    fs = CountingFS()

    json_bytes = None
    if (RESULTS / "01_size.json").exists():
        r = json.loads((RESULTS / "01_size.json").read_text())
        if r["rows"] == n:
            json_bytes = r["bytes"]["JSON lines"]

    q1 = {}
    jsonl = DATA / f"load.{n}.jsonl"
    if jsonl.exists():
        q1["JSON lines, any engine (pyarrow.json)"] = json_scan(jsonl, fs)
    elif json_bytes:
        q1["JSON lines, any engine"] = {"bytes": json_bytes, "requests": "-", "rows": "-",
                                        "row_groups": "-", "avg": None, "cpu_s": None}
    q1["Parquet, read everything, filter in memory"] = full_read_then_filter(paths["time-ordered"], Q1, fs)
    q1["+ column pruning (time-ordered)"] = scan(paths["time-ordered"], ["temp_c", "location"], Q1, fs)
    q1["+ sorted by location (row-group skipping)"] = scan(paths["sorted by location"], ["temp_c", "location"], Q1, fs)
    q1["+ partitioned by location"] = scan(paths["partitioned by location"], ["temp_c"], Q1, fs, partitioned=True)

    q2 = {
        "sorted by location": scan(paths["sorted by location"], ["temp_c"], Q2, fs),
        "partitioned by location": scan(paths["partitioned by location"], ["temp_c"], Q2, fs, partitioned=True),
        "sorted by temp_c": scan(paths["sorted by temp_c"], ["temp_c"], Q2, fs),
    }

    def show(title, rows):
        base = next(iter(rows.values()))["bytes"]
        print(f"\n{title}")
        print(f"{'layout':<48} {'fetched':>12} {'vs first':>9} {'row groups':>11} {'reads':>6} {'CPU':>8}")
        for name, r in rows.items():
            cpu = f"{r['cpu_s'] * 1000:,.0f} ms" if r.get("cpu_s") is not None else "-"
            print(f"{name:<48} {fmt_bytes(r['bytes']):>12} {r['bytes'] / base:>8.1%} "
                  f"{r['row_groups']:>11} {str(r['requests']):>6} {cpu:>8}")
        avgs = {round(r["avg"], 4) for r in rows.values() if r["avg"] is not None}
        assert len(avgs) == 1, f"layouts disagree on the answer: {avgs}"
        print(f"  identical answer in every row: avg = {avgs.pop():.4f}")

    show("Q1  avg(temp_c) WHERE location = 'hall-a'", q1)
    show("Q2  avg(temp_c) WHERE temp_c > 40", q2)

    if args.eur_per_tb:
        print(f"\nAt {args.queries_per_day:,} queries/day for a year, {args.eur_per_tb} EUR/TB scanned:")
        for name, r in q1.items():
            tb = r["bytes"] * args.queries_per_day * 365 / 1e12
            print(f"  {name:<48} {tb:>8.2f} TB/yr  {tb * args.eur_per_tb:>10,.2f} EUR/yr")

    save_result("03_scan", {"rows": n, "row_group_size": ROW_GROUP, "codec": CODEC,
                            "q1": q1, "q2": q2}, partial=args.rows is not None)
