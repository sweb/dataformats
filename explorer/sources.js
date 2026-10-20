"use strict";

/**
 * Where the explorers' facts come from: the specifications and reference
 * implementations the codecs follow, the benchmark runs the Arrow and Lance
 * pages quote, and the repository that holds the code and its verification.
 * Pure data, no DOM access. ui.js renders it.
 */
(function (g) {
  const DF = g.DF || (g.DF = {});

  const REPO = "https://github.com/sweb/dataformats";
  const BRANCH = "main";

  /** Link to a file in this project's repository, by path from the repository root. */
  function repoUrl(path) {
    return `${REPO}/blob/${BRANCH}/${path}`;
  }

  const SOURCES = {
    "pb-encoding": {
      title: "Protocol Buffers: Encoding", by: "protobuf.dev",
      url: "https://github.com/protocolbuffers/protocolbuffers.github.io/blob/main/content/programming-guides/encoding.md",
      used: "tags, wire types, varints, ZigZag, packed repeated fields, embedded messages",
    },
    "pb-proto3": {
      title: "Protocol Buffers: Language Guide (proto3)", by: "protobuf.dev",
      url: "https://github.com/protocolbuffers/protocolbuffers.github.io/blob/main/content/programming-guides/proto3.md",
      used: "default values, field presence and optional, updating a message type",
    },
    "pb-techniques": {
      title: "Protocol Buffers: Techniques, streaming multiple messages", by: "protobuf.dev",
      url: "https://github.com/protocolbuffers/protocolbuffers.github.io/blob/main/content/programming-guides/techniques.md",
      used: "the length-delimited convention for a stream of messages",
    },
    "pb-protoc": {
      title: "protoc --decode_raw", by: "protocolbuffers/protobuf",
      url: "https://github.com/protocolbuffers/protobuf/blob/main/src/google/protobuf/compiler/command_line_interface.cc",
      used: "reference output for the schema-less view, compared byte for byte by the verify script",
    },
    "avro-spec": {
      title: "Apache Avro 1.12.0 Specification", by: "Apache Avro",
      url: "https://github.com/apache/avro/blob/main/doc/content/en/docs/1.12.0/Specification/_index.md",
      used: "binary encoding, object container files, sync markers, single-object encoding, Parsing Canonical Form, CRC-64-AVRO fingerprints, schema resolution and promotions",
    },
    "confluent-wire": {
      title: "Schema Registry: wire format", by: "Confluent documentation",
      url: "https://docs.confluent.io/platform/current/schema-registry/fundamentals/serdes-develop/index.html#wire-format",
      used: "magic byte and 4-byte schema id in front of every Kafka message",
    },
    "parquet-format": {
      title: "Apache Parquet file format", by: "apache/parquet-format",
      url: "https://github.com/apache/parquet-format/blob/master/README.md",
      used: "file layout: magic, row groups, column chunks, pages, footer",
    },
    "parquet-encodings": {
      title: "Parquet encodings", by: "apache/parquet-format",
      url: "https://github.com/apache/parquet-format/blob/master/Encodings.md",
      used: "PLAIN, dictionary encoding, the RLE / bit-packing hybrid, definition levels",
    },
    "parquet-thrift": {
      title: "parquet.thrift", by: "apache/parquet-format",
      url: "https://github.com/apache/parquet-format/blob/master/src/main/thrift/parquet.thrift",
      used: "FileMetaData, PageHeader, Statistics, SortingColumn, field_id: every footer field and its number",
    },
    "thrift-compact": {
      title: "Thrift compact protocol", by: "apache/thrift",
      url: "https://github.com/apache/thrift/blob/master/doc/specs/thrift-compact-protocol.md",
      used: "field headers with id deltas, booleans in the type nibble, list headers",
    },
    "arrow-columnar": {
      title: "Arrow Columnar Format", by: "Apache Arrow",
      url: "https://github.com/apache/arrow/blob/main/docs/source/format/Columnar.rst",
      used: "validity bitmaps, offsets and values buffers, padding, the IPC file format and its encapsulated messages",
    },
    "arrow-fbs": {
      title: "Arrow FlatBuffers schemas (Schema.fbs, Message.fbs, File.fbs)", by: "Apache Arrow",
      url: "https://github.com/apache/arrow/tree/main/format",
      used: "Schema, Field, Message, RecordBatch, FieldNode, Buffer, Footer, Block: field ids and union tags",
    },
    "flatbuffers-internals": {
      title: "FlatBuffers internals", by: "google/flatbuffers",
      url: "https://github.com/google/flatbuffers/blob/master/docs/source/internals.md",
      used: "vtables, soffset and uoffset, alignment, strings",
    },
    "iceberg-spec": {
      title: "Apache Iceberg table spec", by: "Apache Iceberg",
      url: "https://github.com/apache/iceberg/blob/main/format/spec.md",
      used: "table metadata, snapshots, manifest lists, manifests and their Avro schemas, field ids, schema evolution and type promotion, single-value serialization of bounds",
    },
    "iceberg-config": {
      title: "Iceberg table properties", by: "Apache Iceberg",
      url: "https://github.com/apache/iceberg/blob/main/docs/docs/configuration.md",
      used: "commit.manifest-merge.enabled, commit.manifest.min-count-to-merge, write.metadata.previous-versions-max, write.metadata.delete-after-commit.enabled",
    },
    "iceberg-java-defaults": {
      title: "TableProperties.java", by: "Apache Iceberg (Java)",
      url: "https://github.com/apache/iceberg/blob/main/core/src/main/java/org/apache/iceberg/TableProperties.java",
      used: "Java defaults: manifest merging on, merge at 100 manifests, 100 previous metadata versions, zstd for Parquet since 1.4.0",
    },
    "pyiceberg-defaults": {
      title: "PyIceberg TableProperties", by: "apache/iceberg-python",
      url: "https://github.com/apache/iceberg-python/blob/main/pyiceberg/table/__init__.py",
      used: "PyIceberg defaults: manifest merging off, the other limits as in Java",
    },
    "iceberg-maintenance": {
      title: "Iceberg maintenance", by: "Apache Iceberg",
      url: "https://github.com/apache/iceberg/blob/main/docs/docs/maintenance.md",
      used: "expire_snapshots, rewrite_data_files, removing old metadata files",
    },
    "avro-java-sync": {
      title: "DataFileConstants.java", by: "Apache Avro (Java)",
      url: "https://github.com/apache/avro/blob/main/lang/java/avro/src/main/java/org/apache/avro/file/DataFileConstants.java",
      used: "the 64,000-byte default sync interval, used here as the block size of manifests and manifest lists",
    },
    "lance-encoding": {
      title: "Lance file format: encoding", by: "lancedb/lance",
      url: "https://github.com/lancedb/lance/blob/main/docs/src/format/file/encoding.md",
      used: "the mini-block page layout and its metadata buffer of two bytes per mini-block",
    },
    "lance-proto": {
      title: "encodings_v2_1.proto", by: "lancedb/lance",
      url: "https://github.com/lancedb/lance/blob/main/protos/encodings_v2_1.proto",
      used: "MiniBlockLayout as stored in the page metadata the model reads",
    },
    "arrow-parquet-defaults": {
      title: "parquet/properties.h", by: "Apache Arrow (C++)",
      url: "https://github.com/apache/arrow/blob/main/cpp/src/parquet/properties.h",
      used: "the 1 MiB default data page size of the Parquet writer pyarrow uses",
    },
  };

  const DATASET = {
    script: "bench/generate.py",
    text: "10,000,000 rows of synthetic sensor telemetry (seed 42): device_id, timestamp, location, firmware_version, status, temp_c. Skewed low-cardinality columns, time-ordered arrival, temperatures with two decimals.",
  };

  const BENCH = {
    "05_random_rows": {
      title: "bench/05: 1000 random rows by position, Parquet vs Lance",
      script: "bench/05_random_rows.py",
      results: "bench/results/05_random_rows.json",
      method: [
        "Parquet: zstd, row groups of 100,000 rows, read with pyarrow. Lance: written with lance.write_dataset, file format 2.1.",
        "1000 row positions drawn with random.Random(7). Point: one call per row (ParquetFile.read_row_group, then take; Lance dataset.take). Batch: one take call with all 1000 sorted ids.",
        "Page cache warmed before timing: the numbers measure decoding, not the disk.",
        "Wall seconds from time.perf_counter, CPU seconds from time.process_time, summed over all threads.",
        "Parquet bytes read for the point lookups counted by a filesystem wrapper around every read() pyarrow issues.",
      ],
    },
    "06_load": {
      title: "bench/06: 10M rows into memory, JSON vs Parquet, pandas vs Arrow",
      script: "bench/06_load.py",
      results: "bench/results/06_load.json",
      method: [
        "The same rows written as JSON lines and as Parquet (zstd, default row groups).",
        "JSON to pandas with pandas.read_json in chunks of 500,000 lines, JSON to Arrow with pyarrow.json.read_json, Parquet to Arrow with pyarrow.parquet.read_table, then Arrow to pandas with and without ArrowDtype.",
        "Page cache warmed before timing: the numbers measure parsing and decoding, not the disk.",
        "Wall seconds from time.perf_counter, CPU seconds from time.process_time, summed over all threads. In memory: Table.nbytes for Arrow, DataFrame.memory_usage(deep=True) for pandas.",
      ],
    },
  };

  const VERIFY = { check: "explorer/verify/check.py", generate: "explorer/verify/generate.mjs" };

  DF.SRC = { REPO, repoUrl, SOURCES, DATASET, BENCH, VERIFY };
})(globalThis);
