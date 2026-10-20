# explorer/: interactive format explorers

Static pages, one per format, that write real files in the browser and explain every byte.
Open `index.html` in a browser (`file://` works).

| Page | What it shows |
|---|---|
| `protobuf.html` | tags and varints bit by bit, proto3 omitting defaults, reader-schema scenarios: rename free, renumber/type change silent, unknown fields skipped, no .proto at all (decoded like `protoc --decode_raw`, every value with its possible types); "500 readings" as delimited stream or repeated wrapper message |
| `avro.html` | Object Container File with the schema readable in the ASCII column, sync markers, blocks, deflate; the same records as raw datum, single-object encoding (fingerprint) and Confluent framing; schema resolution scenarios: reorder ok, add without default BREAKS, rename BREAKS, alias ok, widen ok; a split reader starting at byte N scans for the sync marker (raw datums for contrast) |
| `arrow.html` | validity bitmaps, offsets and values as buffers, reading row i by address arithmetic, slices as (offset, length); a real Arrow IPC file with its FlatBuffers metadata annotated down to vtables, Buffer structs that jump to the body bytes they describe; measured load times from bench/06 |
| `lance.html` | random row lookups replayed on the layout metadata of the real 10M-row files from bench/05: Parquet row groups vs Lance mini-block chunks, bytes and values decoded, the model checked against the measured bytes, field ids, file size cost. Writes no Lance bytes |
| `parquet.html` | rows to columns, a real Parquet file written in the browser (row groups, dictionary pages, RLE/bit-packed runs, Thrift footer with min/max statistics), per-column encoding, sort order, and a query planner showing which byte ranges a reader fetches |
| `iceberg.html` | a real format v2 table written in the browser: metadata.json (JSON), manifest lists and manifests (Avro, with Iceberg's header metadata; manifests uncompressed so the per-column lower/upper bounds are readable and labelled by field id), data files (Parquet with field ids), every file clickable down to the byte; schema changes as metadata-only commits read back by field id vs by name, including a rename that happens upstream (drop + add); bytes on storage over up to 10,080 commits with no maintenance, with manifest merging and metadata deletion as toggles |

## Files

- `common.js`: byte writer that records a span tree while writing, varint/zigzag helpers
- `protobuf-codec.js`, `avro-codec.js`, `parquet-codec.js`, `arrow-codec.js`: the formats, pure JS, run in Node too
- `lance-model.js`: the lookup model for the Lance page
- `iceberg-model.js`: the Iceberg table writer (create, append, schema changes, scan by id or by name) and the growth model, calibrated on files it writes
- `data/bench.js`: layouts and results of the bench files, written by `uv run explorer/data/export_bench.py` (rerun after bench/05 or bench/06)
- `ui.js`, `ui.css`: structure diagram, file map, virtualized hex view, inspector, source lists
- `sources.js`: every external source the pages cite, the benchmark runs they quote, and the repository URL (`REPO`)

## Scope

Deliberately not feature complete: proto3 subset (no maps, oneofs, groups),
Parquet only with flat columns, data page v1, UNCOMPRESSED or GZIP, no BOOLEAN.
Arrow without dictionary arrays, nested types or compression.
Iceberg format v2 only, unpartitioned, append only, no delete files; Parquet data files use gzip
because the browser has no zstd (Iceberg's default). Manifest merging follows the Java
library's rule for small manifests (merge everything once 100 are listed), not its bin packing.
The sizes are honest relative to each other; quote absolute numbers from
`bench/`, not from here.
