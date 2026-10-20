"""What schema evolution costs on a 10M-row table: Iceberg, for real, against
plain Parquet.

Iceberg's answer to silent schema drift (a reader that matches columns by name
gets nulls or wrong data without an error) is field IDs: every column has a
number, files are resolved by number, names are labels. So a rename or an
added column is a metadata commit that rewrites zero data bytes. This script
checks that claim with the real library and a real catalog (SQLite, local
filesystem) rather than quoting the spec.

    .venv/bin/python bench/04_evolution_cost.py [--rows N]
"""

import argparse
import shutil

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from pyiceberg.catalog.sql import SqlCatalog
from pyiceberg.types import FloatType, LongType

from common import guard, DATA, Timer, fmt_bytes, load, save_result

WAREHOUSE = DATA / "iceberg"


def tree_bytes(root) -> tuple[int, int]:
    """(data bytes, metadata bytes) under the warehouse."""
    data = meta = 0
    for p in root.rglob("*"):
        if p.is_file():
            if "data" in p.relative_to(WAREHOUSE).parts:
                data += p.stat().st_size
            else:
                meta += p.stat().st_size
    return data, meta


class Step:
    def __init__(self, log: dict, name: str):
        self.log, self.name = log, name

    def __enter__(self):
        self.before = tree_bytes(WAREHOUSE)
        self.t = Timer().__enter__()
        return self

    def __exit__(self, *exc):
        self.t.__exit__(*exc)
        after = tree_bytes(WAREHOUSE)
        r = {"seconds": self.t.seconds,
             "data_bytes_written": after[0] - self.before[0],
             "metadata_bytes_written": after[1] - self.before[1]}
        self.log[self.name] = r
        print(f"  {self.name:<40} {r['seconds']:>7.2f} s   data {fmt_bytes(r['data_bytes_written']):>10}   "
              f"metadata {fmt_bytes(r['metadata_bytes_written']):>10}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=None)
    args = ap.parse_args()
    guard(6, args.rows)

    table = load(args.rows)
    n = table.num_rows
    shutil.rmtree(WAREHOUSE, ignore_errors=True)
    WAREHOUSE.mkdir(parents=True)
    catalog = SqlCatalog("bench", uri=f"sqlite:///{WAREHOUSE}/catalog.db",
                         warehouse=f"file://{WAREHOUSE}/warehouse")
    catalog.create_namespace("bench")
    log = {}

    print(f"{n:,} rows\n\nIceberg (pyiceberg, SQLite catalog, local warehouse)")
    with Step(log, "create table + append all rows"):
        tbl = catalog.create_table("bench.telemetry", schema=table.schema)
        tbl.append(table)
    expected_avg = pc.mean(table.column("temp_c")).as_py()

    with Step(log, "add column humidity (no default)"):
        with tbl.update_schema() as u:
            u.add_column("humidity", FloatType())

    with Step(log, "rename temp_c -> temperature_c"):
        with tbl.update_schema() as u:
            u.rename_column("temp_c", "temperature_c")

    with Step(log, "widen device_id int -> long"):
        with tbl.update_schema() as u:
            u.update_column("device_id", field_type=LongType())

    with Step(log, "drop firmware_version"):
        with tbl.update_schema() as u:
            u.delete_column("firmware_version")

    with Step(log, "reorder: status first"):
        with tbl.update_schema() as u:
            u.move_first("status")

    # Now the proof. Write a batch of rows under the NEW schema, then read the
    # whole table. Old files say `temp_c`, new files say `temperature_c`;
    # both carry field id 6, so the reader sees one column.
    new_rows = table.slice(0, 1000).drop_columns(["firmware_version"]) \
        .rename_columns({"temp_c": "temperature_c"}) \
        .append_column("humidity", pa.array([55.0] * 1000, pa.float32()))
    new_rows = new_rows.set_column(0, "device_id", new_rows.column("device_id").cast(pa.int64()))
    with Step(log, "append 1000 rows under new schema"):
        tbl.append(new_rows.select([f.name for f in tbl.schema().fields]))

    scanned = tbl.scan(selected_fields=("temperature_c", "humidity")).to_arrow()
    old_rows = scanned.filter(pc.is_null(scanned.column("humidity")))
    got_avg = pc.mean(old_rows.column("temperature_c")).as_py()
    nulls = old_rows.num_rows
    assert scanned.num_rows == n + 1000
    assert abs(got_avg - expected_avg) < 1e-3, (got_avg, expected_avg)
    assert nulls == n, nulls
    print(f"\n  read back {scanned.num_rows:,} rows across old and new files: "
          f"avg(temperature_c) over the old rows = {got_avg:.4f} (was {expected_avg:.4f}), "
          f"humidity null in exactly the {nulls:,} old rows.")
    print("  Iceberg resolves old and new files by field id: verified.")

    # The comparison: doing the same thing to a plain Parquet file means
    # rewriting the file. Every byte, every time.
    print("\nPlain Parquet, same operation")
    src = DATA / "evo_plain.parquet"
    dst = DATA / "evo_plain_v2.parquet"
    pq.write_table(table, src, compression="zstd")
    with Timer() as t:
        t2 = pq.read_table(src).append_column("humidity", pa.nulls(n, pa.float32()))
        pq.write_table(t2, dst, compression="zstd")
    plain = {"seconds": t.seconds, "data_bytes_written": dst.stat().st_size, "metadata_bytes_written": 0}
    print(f"  {'add column = rewrite the file':<40} {plain['seconds']:>7.2f} s   "
          f"data {fmt_bytes(plain['data_bytes_written']):>10}")
    print("  And the rename? Parquet has no operation for it. The next reader"
          " resolves by name and finds nothing. SILENT.")

    save_result("04_evolution_cost", {"rows": n, "iceberg": log, "parquet_rewrite": plain}, partial=args.rows is not None)
