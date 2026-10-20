"""Generate the canonical dataset: 10M rows of sensor telemetry, written once
as Parquet. Every other script reads this file, so all numbers are measured
on identical rows.

    .venv/bin/python bench/generate.py [--rows N]
"""

import argparse

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from common import (CANONICAL, DATA, FIRMWARE, FIRMWARE_WEIGHTS, LOCATIONS,
                    LOCATION_WEIGHTS, SCHEMA, STATUS_WEIGHTS, STATUSES, Timer,
                    fmt_bytes)


def generate(rows: int, seed: int = 42) -> pa.Table:
    rng = np.random.default_rng(seed)

    # Readings arrive in time order over 30 days, with a little jitter. This is
    # the realistic case and also the honest one: it means the file is sorted
    # by timestamp and by nothing else.
    base_us = 1_756_684_800_000_000  # 2025-09-01T00:00:00Z
    step_us = 30 * 86_400 * 1_000_000 // rows
    ts = base_us + np.arange(rows, dtype=np.int64) * step_us \
        + rng.integers(0, step_us, rows, dtype=np.int64)

    def pick(values, weights):
        idx = rng.choice(len(values), size=rows, p=weights).astype(np.int32)
        return pa.DictionaryArray.from_arrays(pa.array(idx), pa.array(values)) \
                 .cast(pa.string())

    return pa.table({
        "device_id": pa.array(rng.integers(10_000, 60_000, rows, dtype=np.int32)),
        "timestamp": pa.array(ts, type=pa.timestamp("us")),
        "location": pick(LOCATIONS, LOCATION_WEIGHTS),
        "firmware_version": pick(FIRMWARE, FIRMWARE_WEIGHTS),
        "status": pick(STATUSES, STATUS_WEIGHTS),
        "temp_c": pa.array(np.round(rng.uniform(15.0, 45.0, rows), 2).astype(np.float32)),
    }, schema=SCHEMA)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--rows", type=int, default=10_000_000)
    args = ap.parse_args()

    DATA.mkdir(exist_ok=True)
    with Timer() as t:
        table = generate(args.rows)
        pq.write_table(table, CANONICAL, compression="zstd")
    print(f"{table.num_rows:,} rows -> {CANONICAL}  "
          f"({fmt_bytes(CANONICAL.stat().st_size)}, {t.seconds:.1f}s)")
    print(table.slice(0, 5).to_pandas().to_string(index=False))
