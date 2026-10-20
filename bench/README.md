# bench/: the measurement rig

Every absolute number in this project comes from here, measured on the same
10M rows of sensor telemetry. The explorer shows the mechanisms at toy scale
in the browser; this directory puts real sizes and seconds on them.

```
uv venv .venv && uv pip install --python .venv -r bench/requirements.txt
cd bench
../.venv/bin/python generate.py              # 10M rows -> data/telemetry.parquet, once
../.venv/bin/python 01_size.py               # bytes/record, every format
../.venv/bin/python 03_scan.py --eur-per-tb X  # bytes fetched per query, four layouts
../.venv/bin/python 04_evolution_cost.py     # Iceberg schema changes, for real
../.venv/bin/python 05_random_rows.py        # point lookups, Parquet vs Lance
../.venv/bin/python 06_load.py               # JSON vs Parquet into pandas / Arrow
../.venv/bin/python 02_kafka.py --bootstrap HOST:PORT --log-dirs-cmd "..."   # needs a broker
../.venv/bin/python report.py                # the consolidated table
```

Every script accepts `--rows N` for a quick run on a prefix of the data, and
writes `results/NN.json`. `report.py` folds those into one summary table.

| # | Measures | Format | Needs |
|---|---|---|---|
| 01 | size/record: JSON, Protobuf, Avro ×4 codecs, Parquet ×3, Lance | all | local |
| 02 | Kafka broker disk + produce throughput, Avro vs JSON | Avro | a broker |
| 03 | bytes fetched for one query, 4 layouts + JSON baseline; second query for the sort-order trade-off | Parquet | local |
| 04 | add/rename/widen/drop/reorder on a 10M-row Iceberg table: seconds, data bytes rewritten; read-back across old and new files | Iceberg | local (PyIceberg, SQLite catalog) |
| 05 | 1000 random rows by position, point and batch: Parquet vs Lance | Lance, quoted on the explorer's Lance page | local |
| 06 | JSON→pandas, JSON→Arrow, Parquet→Arrow, Arrow→pandas: seconds and bytes in memory | Arrow, quoted on the explorer's Arrow page | local |

## Caveats

- The dataset is synthetic and shaped to make mechanisms visible. Skewed
  low-cardinality columns, time-ordered arrival, floats with two decimals.
  Real telemetry will differ in degree, not in kind.
- `temp_c` has 3001 distinct values, so Parquet dictionary-encodes it. A
  sensor with more precision would compress worse.
- Page cache is warmed before timing in 05 and 06. They measure decoding, not
  the disk.
- 04 runs against a SQLite catalog on the local filesystem. The commit cost on
  a REST catalog over a network adds latency but no data bytes; that is the
  claim being tested.
- The € figure in 03 needs a price per TB scanned. There is no single S3 number
  for that; state the engine and price you assume whenever you quote the result.

