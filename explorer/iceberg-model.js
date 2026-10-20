"use strict";

/**
 * Iceberg: a small but real table writer, format version 2, unpartitioned,
 * append only. Data files are Parquet with field ids, manifests and manifest
 * lists are Avro container files, table metadata is JSON. Plus a growth
 * model for many commits, calibrated on files written by this writer.
 */
(function (g) {
  const DF = g.DF;
  const IB = (DF.IB = {});

  const PARQUET = {
    int: { ptype: "INT32", encoding: "PLAIN" },
    long: { ptype: "INT64", encoding: "PLAIN" },
    float: { ptype: "FLOAT", encoding: "PLAIN" },
    string: { ptype: "BYTE_ARRAY", converted: "UTF8", encoding: "DICT" },
  };
  const PROMOTIONS = { int: ["long"], float: ["double"] };
  const START = Date.UTC(2026, 9, 1, 8, 0, 0);
  const AVRO_SYNC_INTERVAL = 64000;

  function intMap(name, fid, kid, vid, vtype) {
    return {
      name, "field-id": fid, default: null,
      type: ["null", { type: "array", logicalType: "map", items: { type: "record", name: `k${kid}_v${vid}`, fields: [{ name: "key", type: "int", "field-id": kid }, { name: "value", type: vtype, "field-id": vid }] } }],
    };
  }

  const DATA_FILE = {
    type: "record", name: "r2", fields: [
      { name: "content", type: "int", "field-id": 134 },
      { name: "file_path", type: "string", "field-id": 100 },
      { name: "file_format", type: "string", "field-id": 101 },
      { name: "partition", type: { type: "record", name: "r102", fields: [] }, "field-id": 102 },
      { name: "record_count", type: "long", "field-id": 103 },
      { name: "file_size_in_bytes", type: "long", "field-id": 104 },
      intMap("column_sizes", 108, 117, 118, "long"),
      intMap("value_counts", 109, 119, 120, "long"),
      intMap("null_value_counts", 110, 121, 122, "long"),
      intMap("nan_value_counts", 137, 138, 139, "long"),
      intMap("lower_bounds", 125, 126, 127, "bytes"),
      intMap("upper_bounds", 128, 129, 130, "bytes"),
      { name: "key_metadata", type: ["null", "bytes"], default: null, "field-id": 131 },
      { name: "split_offsets", type: ["null", { type: "array", items: "long", "element-id": 133 }], default: null, "field-id": 132 },
      { name: "equality_ids", type: ["null", { type: "array", items: "int", "element-id": 136 }], default: null, "field-id": 135 },
      { name: "sort_order_id", type: ["null", "int"], default: null, "field-id": 140 },
    ],
  };

  const MANIFEST_ENTRY = {
    type: "record", name: "manifest_entry", fields: [
      { name: "status", type: "int", "field-id": 0 },
      { name: "snapshot_id", type: ["null", "long"], default: null, "field-id": 1 },
      { name: "sequence_number", type: ["null", "long"], default: null, "field-id": 3 },
      { name: "file_sequence_number", type: ["null", "long"], default: null, "field-id": 4 },
      { name: "data_file", type: DATA_FILE, "field-id": 2 },
    ],
  };

  const MANIFEST_FILE = {
    type: "record", name: "manifest_file", fields: [
      { name: "manifest_path", type: "string", "field-id": 500 },
      { name: "manifest_length", type: "long", "field-id": 501 },
      { name: "partition_spec_id", type: "int", "field-id": 502 },
      { name: "content", type: "int", "field-id": 517 },
      { name: "sequence_number", type: "long", "field-id": 515 },
      { name: "min_sequence_number", type: "long", "field-id": 516 },
      { name: "added_snapshot_id", type: "long", "field-id": 503 },
      { name: "added_files_count", type: "int", "field-id": 504 },
      { name: "existing_files_count", type: "int", "field-id": 505 },
      { name: "deleted_files_count", type: "int", "field-id": 506 },
      { name: "added_rows_count", type: "long", "field-id": 512 },
      { name: "existing_rows_count", type: "long", "field-id": 513 },
      { name: "deleted_rows_count", type: "long", "field-id": 514 },
      {
        name: "partitions", "field-id": 507, default: null,
        type: ["null", { type: "array", "element-id": 508, items: { type: "record", name: "r508", fields: [
          { name: "contains_null", type: "boolean", "field-id": 509 },
          { name: "contains_nan", type: ["null", "boolean"], default: null, "field-id": 518 },
          { name: "lower_bound", type: ["null", "bytes"], default: null, "field-id": 510 },
          { name: "upper_bound", type: ["null", "bytes"], default: null, "field-id": 511 },
        ] } }],
      },
      { name: "key_metadata", type: ["null", "bytes"], default: null, "field-id": 519 },
    ],
  };

  const INITIAL_FIELDS = [
    { id: 1, name: "device_id", required: true, type: "int" },
    { id: 2, name: "location", required: false, type: "string" },
    { id: 3, name: "temp_c", required: false, type: "float" },
  ];

  function uuid(r) {
    const h = Array.from({ length: 32 }, () => r.int(0, 16).toString(16));
    h[12] = "4";
    h[16] = "89ab"[r.int(0, 4)];
    const s = h.join("");
    return `${s.slice(0, 8)}-${s.slice(8, 12)}-${s.slice(12, 16)}-${s.slice(16, 20)}-${s.slice(20)}`;
  }

  function snapshotId(r) {
    return r.int(1, 2 ** 21) * 2 ** 32 + r.int(0, 2 ** 32);
  }

  function create({ location = "s3://lake/warehouse/telemetry", seed = 7, merge = false, minCountToMerge = 100, deleteAfterCommit = false, previousVersionsMax = 100, manifestCodec = "deflate" } = {}) {
    const r = DF.rng(seed);
    const t = {
      r, location, uuid: uuid(r), time: START, lastSeq: 0, version: -1,
      schemas: [{ schemaId: 0, fields: INITIAL_FIELDS.map((f) => ({ ...f })) }],
      currentSchemaId: 0, lastColumnId: 3,
      snapshots: [], snapshotLog: [], metadataLog: [],
      manifests: [], dataFiles: [], files: [],
      props: { merge, minCountToMerge, deleteAfterCommit, previousVersionsMax, manifestCodec },
    };
    writeMetadata(t);
    return t;
  }

  function schemaOf(t, id = t.currentSchemaId) {
    return t.schemas.find((s) => s.schemaId === id);
  }

  function schemaJson(s) {
    return { type: "struct", "schema-id": s.schemaId, fields: s.fields.map((f) => ({ id: f.id, name: f.name, required: f.required, type: f.type })) };
  }

  function currentSnapshot(t) {
    return t.snapshots[t.snapshots.length - 1] || null;
  }

  function metadataObject(t) {
    const cur = currentSnapshot(t);
    return {
      "format-version": 2,
      "table-uuid": t.uuid,
      location: t.location,
      "last-sequence-number": t.lastSeq,
      "last-updated-ms": t.time,
      "last-column-id": t.lastColumnId,
      "current-schema-id": t.currentSchemaId,
      schemas: t.schemas.map(schemaJson),
      "default-spec-id": 0,
      "partition-specs": [{ "spec-id": 0, fields: [] }],
      "last-partition-id": 999,
      "default-sort-order-id": 0,
      "sort-orders": [{ "order-id": 0, fields: [] }],
      properties: {},
      "current-snapshot-id": cur ? cur["snapshot-id"] : -1,
      refs: cur ? { main: { "snapshot-id": cur["snapshot-id"], type: "branch" } } : {},
      snapshots: t.snapshots,
      statistics: [],
      "partition-statistics": [],
      "snapshot-log": t.snapshotLog,
      "metadata-log": t.metadataLog,
    };
  }

  const JSON_DESC = {
    "format-version": "Iceberg table spec version. v2 adds row-level deletes and sequence numbers.",
    location: "Base location. Data files go under data/, everything else under metadata/.",
    "last-column-id": "The highest field id ever assigned. A new column gets the next one, so an id is never reused, not even after a drop.",
    "current-schema-id": "Which of the schemas below readers use. Old schemas stay, because old snapshots refer to them.",
    schemas: "Every schema this table ever had. Fields are identified by id; the name is a label.",
    "current-snapshot-id": "The table's current state. A reader starts here, finds this snapshot below and follows its manifest-list.",
    snapshots: "One entry per commit, forever, until snapshots are expired. This array is why the file grows with every commit.",
    "snapshot-log": "When each snapshot became current. Also one entry per commit.",
    "metadata-log": "Previous metadata files. Capped at write.metadata.previous-versions-max (100) entries, but the files themselves stay on storage unless write.metadata.delete-after-commit.enabled is set.",
  };

  function preview(v) {
    if (Array.isArray(v)) return `${v.length} item(s)`;
    if (v && typeof v === "object") return `${Object.keys(v).length} key(s)`;
    return DF.shorten(JSON.stringify(v), 60);
  }

  /** Compact JSON, as Iceberg writes it, with a span per top-level key and per array item. */
  function encodeJson(obj, title) {
    const w = new DF.ByteWriter();
    w.leaf("{", DF.utf8("{"), { kind: "magic", type: "JSON", value: "{" });
    Object.keys(obj).forEach((k, i) => {
      const v = obj[k];
      const head = (i ? "," : "") + JSON.stringify(k) + ":";
      const meta = { kind: k === "schemas" ? "schema" : k === "snapshots" || k === "snapshot-log" || k === "metadata-log" ? "count" : "meta", type: Array.isArray(v) ? "JSON array" : typeof v === "object" && v ? "JSON object" : `JSON ${typeof v}`, value: preview(v), desc: JSON_DESC[k] };
      if (Array.isArray(v) && v.length) {
        w.group(k, meta, () => {
          w.leaf("key", DF.utf8(head + "["), { kind: "meta", type: "JSON key", value: JSON.stringify(k) });
          v.forEach((item, j) => w.leaf(`[${j}]`, DF.utf8((j ? "," : "") + JSON.stringify(item)), { kind: meta.kind === "schema" ? "schema" : "value", type: "JSON object", value: DF.shorten(JSON.stringify(item), 80), desc: meta.desc }));
          w.leaf("]", DF.utf8("]"), { kind: "meta", type: "JSON", value: "]" });
        });
      } else {
        w.leaf(k, DF.utf8(head + JSON.stringify(v)), meta);
      }
    });
    w.leaf("}", DF.utf8("}"), { kind: "magic", type: "JSON", value: "}" });
    return w.finish(title);
  }

  function addFile(t, file) {
    t.files.push(file);
    return file;
  }

  function fileName(path) {
    return path.slice(path.lastIndexOf("/") + 1);
  }

  function writeMetadata(t) {
    const prev = t.files.filter((f) => f.kind === "metadata").pop();
    if (prev) {
      t.metadataLog.push({ "timestamp-ms": prev.time, "metadata-file": prev.path });
      while (t.metadataLog.length > t.props.previousVersionsMax) t.metadataLog.shift();
    }
    t.version++;
    const path = `${t.location}/metadata/${String(t.version).padStart(5, "0")}-${uuid(t.r)}.metadata.json`;
    const built = encodeJson(metadataObject(t), fileName(path));
    built.root.meta = { type: "table metadata (JSON)", value: `version ${t.version}, ${t.snapshots.length} snapshot(s)`, desc: "The root of the table. A catalog stores nothing but a pointer to the current one of these files. Every commit writes a new one." };
    addFile(t, { kind: "metadata", format: "JSON", path, name: fileName(path), bytes: built.bytes, built, time: t.time, version: t.version, snapshotId: currentSnapshot(t)?.["snapshot-id"] ?? null });
    if (t.props.deleteAfterCommit) {
      const meta = t.files.filter((f) => f.kind === "metadata");
      const drop = new Set(meta.slice(0, Math.max(0, meta.length - 1 - t.props.previousVersionsMax)));
      t.files = t.files.filter((f) => !drop.has(f));
    }
  }

  function boundBytes(type, v) {
    switch (type) {
      case "int": return DF.le32(v);
      case "long": return DF.le64(v);
      case "float": return DF.f32(v);
      case "string": return DF.utf8(v);
    }
    throw new Error(`no bound encoding for ${type}`);
  }

  function boundValue(type, bytes) {
    const v = DF.view(bytes, 0);
    switch (type) {
      case "int": return String(v.getInt32(0, true));
      case "long": return String(v.getBigInt64(0, true));
      case "float": return String(v.getFloat32(0, true));
      case "string": return JSON.stringify(DF.decodeUtf8(bytes));
    }
    throw new Error(`no bound encoding for ${type}`);
  }

  const BOUND_RULE = { int: "4 bytes, little endian", long: "8 bytes, little endian", float: "IEEE 754 binary32, little endian", string: "UTF-8, no length inside the value" };

  /**
   * Label the lower_bounds / upper_bounds entries of an uncompressed manifest
   * with their field id and decoded value, so a click on the bytes explains them.
   */
  function annotateBounds(t, built, entries) {
    for (const node of DF.walk(built.root)) {
      const m = /^record (\d+)$/.exec(node.label);
      if (!m) continue;
      const file = entries[Number(m[1])].data_file;
      const written = t.dataFiles.find((d) => d.path === file.file_path);
      const schema = schemaOf(t, written ? written.schemaId : t.currentSchemaId);
      const dataFile = node.children.find((c) => c.label === "data_file");
      for (const side of ["lower_bounds", "upper_bounds"]) {
        const union = dataFile.children.find((c) => c.label === side);
        const what = side === "lower_bounds" ? "minimum" : "maximum";
        union.meta = { ...union.meta, desc: `Per-column ${what} over the whole data file, as a map from field id to bytes. The same min/max as in a Parquet footer, one level up: a query planner compares the WHERE clause against it and skips the data file without opening it.` };
        const items = union.children.find((c) => c.label === "value").children.filter((c) => /^\[\d+\]$/.test(c.label));
        items.forEach((item, i) => {
          const { key, value } = file[side][i];
          const f = schema.fields.find((x) => x.id === key);
          const shown = boundValue(f.type, value);
          item.label = `field id ${key}`;
          item.meta = {
            ...item.meta, type: `${what}, field id ${key}`, value: shown,
            desc: `The ${what} of field id ${key} in this data file: ${shown}. The column was called ${f.name} when the file was written, but no name is stored here, only the id. After a rename the planner still finds this bound under id ${key}.`,
          };
          const bytes = item.children.find((c) => c.label === "value");
          bytes.meta = { ...bytes.meta, type: `bytes, ${f.type} single-value serialization`, value: shown, rule: BOUND_RULE[f.type] };
        });
      }
    }
  }

  const LOCATIONS = ["hall-a", "hall-b", "yard", "cold-storage"];
  const FIRMWARE = ["1.4.2", "1.4.3", "2.0.0"];

  /** Sensor readings for the given schema, generated by field id so a rename keeps the values. */
  function readings(t, n, schema = schemaOf(t)) {
    return Array.from({ length: n }, () => {
      const row = {};
      for (const f of schema.fields) {
        if (f.id === 1) row[f.id] = t.r.int(1000, 9999);
        else if (f.id === 2) row[f.id] = t.r.pick(LOCATIONS);
        else if (f.id === 3) row[f.id] = Math.round((15 + t.r.next() * 30) * 4) / 4;
        else if (f.type === "string") row[f.id] = t.r.pick(FIRMWARE);
        else if (f.type === "float") row[f.id] = Math.round(t.r.next() * 400) / 4;
        else row[f.id] = t.r.int(0, 1000);
      }
      return row;
    });
  }

  async function writeDataFile(t, rowsById, commitUuid, fileNo) {
    const schema = schemaOf(t);
    const cols = schema.fields.map((f) => ({ name: f.name, ...PARQUET[f.type], nullable: !f.required, fieldId: f.id }));
    const named = rowsById.map((r) => Object.fromEntries(schema.fields.map((f) => [f.name, r[f.id] ?? null])));
    const built = await DF.PQ.build(named, cols, { codec: "gzip" });
    const path = `${t.location}/data/00000-${fileNo}-${commitUuid}-00001.parquet`;
    built.root.label = fileName(path);
    const chunks = built.groups.flatMap((rg) => rg.chunks);
    const stat = (fn) => schema.fields.map((f, i) => fn(f, chunks.filter((c) => c.index === i))).filter(Boolean);
    const record = {
      content: 0,
      file_path: path,
      file_format: "PARQUET",
      partition: {},
      record_count: rowsById.length,
      file_size_in_bytes: built.bytes.length,
      column_sizes: stat((f, cs) => ({ key: f.id, value: cs.reduce((s, c) => s + c.compressed, 0) })),
      value_counts: stat((f, cs) => ({ key: f.id, value: cs.reduce((s, c) => s + c.numValues, 0) })),
      null_value_counts: stat((f, cs) => ({ key: f.id, value: cs.reduce((s, c) => s + c.nulls, 0) })),
      nan_value_counts: stat((f) => (f.type === "float" ? { key: f.id, value: 0 } : null)),
      lower_bounds: stat((f, cs) => (cs.length && cs[0].min !== undefined ? { key: f.id, value: boundBytes(f.type, cs.reduce((m, c) => (c.min < m ? c.min : m), cs[0].min)) } : null)),
      upper_bounds: stat((f, cs) => (cs.length && cs[0].max !== undefined ? { key: f.id, value: boundBytes(f.type, cs.reduce((m, c) => (c.max > m ? c.max : m), cs[0].max)) } : null)),
      key_metadata: null,
      split_offsets: built.groups.map((rg) => rg.start),
      equality_ids: null,
      sort_order_id: 0,
    };
    return { path, built, record, rows: rowsById, schemaId: schema.schemaId };
  }

  async function writeManifest(t, entries, path, schema = schemaOf(t)) {
    const built = await DF.AV.build(MANIFEST_ENTRY, entries, {
      codec: t.props.manifestCodec, blockBytes: AVRO_SYNC_INTERVAL,
      meta: [["schema", JSON.stringify(schemaJson(schema))], ["schema-id", String(schema.schemaId)], ["partition-spec", "[]"], ["partition-spec-id", "0"], ["format-version", "2"], ["content", "data"]],
    });
    if (t.props.manifestCodec === "null") annotateBounds(t, built, entries);
    built.root.label = fileName(path);
    built.root.meta = { type: "manifest (Avro)", value: `${entries.length} data file(s)`, desc: "Lists data files with their row counts and per-column min/max. A query planner prunes files here, without opening them." };
    return built;
  }

  function manifestFileRecord(path, length, { seq, minSeq, sid, added = 0, existing = 0, addedRows = 0, existingRows = 0 }) {
    return {
      manifest_path: path, manifest_length: length, partition_spec_id: 0, content: 0,
      sequence_number: seq, min_sequence_number: minSeq, added_snapshot_id: sid,
      added_files_count: added, existing_files_count: existing, deleted_files_count: 0,
      added_rows_count: addedRows, existing_rows_count: existingRows, deleted_rows_count: 0,
      partitions: [], key_metadata: null,
    };
  }

  async function writeManifestList(t, records, path, { sid, parent, seq }) {
    const built = await DF.AV.build(MANIFEST_FILE, records, {
      codec: "deflate", blockBytes: AVRO_SYNC_INTERVAL,
      meta: [["snapshot-id", String(sid)], ["parent-snapshot-id", parent === null ? "null" : String(parent)], ["sequence-number", String(seq)], ["format-version", "2"]],
    });
    built.root.label = fileName(path);
    built.root.meta = { type: "manifest list (Avro)", value: `${records.length} manifest(s)`, desc: "One per snapshot. Lists every manifest that makes up the table at that snapshot, with file and row counts." };
    return built;
  }

  function snapshotObject({ seq, sid, parent, time, summary, manifestList, schemaId }) {
    const s = { "sequence-number": seq, "snapshot-id": sid };
    if (parent !== null) s["parent-snapshot-id"] = parent;
    Object.assign(s, { "timestamp-ms": time, summary, "manifest-list": manifestList, "schema-id": schemaId });
    return s;
  }

  function summaryFor(op, { addedFiles, addedRows, addedSize, totalRows, totalSize, totalFiles }) {
    return {
      operation: op,
      "added-data-files": String(addedFiles), "added-records": String(addedRows), "added-files-size": String(addedSize),
      "changed-partition-count": "1",
      "total-records": String(totalRows), "total-files-size": String(totalSize), "total-data-files": String(totalFiles),
      "total-delete-files": "0", "total-position-deletes": "0", "total-equality-deletes": "0",
    };
  }

  /** One append commit: a data file, a manifest, a manifest list, a metadata file. */
  async function append(t, rowsById) {
    const sid = snapshotId(t.r);
    const commit = uuid(t.r);
    const parent = currentSnapshot(t)?.["snapshot-id"] ?? null;
    t.time += 60000;
    const seq = ++t.lastSeq;

    const df = await writeDataFile(t, rowsById, commit, t.dataFiles.length);
    Object.assign(df, { snapshotId: sid, seq, time: t.time });
    t.dataFiles.push(df);
    addFile(t, { kind: "data", format: "Parquet", path: df.path, name: fileName(df.path), bytes: df.built.bytes, built: df.built, snapshotId: sid, rows: rowsById.length, schemaId: df.schemaId });

    const mPath = `${t.location}/metadata/${commit}-m0.avro`;
    let list;
    if (t.props.merge && t.manifests.length + 1 >= t.props.minCountToMerge) {
      const entries = t.dataFiles.map((d) => d === df
        ? { status: 1, snapshot_id: sid, sequence_number: null, file_sequence_number: null, data_file: d.record }
        : { status: 0, snapshot_id: d.snapshotId, sequence_number: d.seq, file_sequence_number: d.seq, data_file: d.record });
      const path = `${t.location}/metadata/${commit}-m1.avro`;
      const built = await writeManifest(t, entries, path);
      addFile(t, { kind: "manifest", format: "Avro", path, name: fileName(path), bytes: built.bytes, built, snapshotId: sid, entries: entries.length });
      const existingRows = t.dataFiles.reduce((s, d) => s + (d === df ? 0 : d.rows.length), 0);
      list = [manifestFileRecord(path, built.bytes.length, { seq, minSeq: t.dataFiles[0].seq, sid, added: 1, existing: t.dataFiles.length - 1, addedRows: rowsById.length, existingRows })];
    } else {
      const built = await writeManifest(t, [{ status: 1, snapshot_id: sid, sequence_number: null, file_sequence_number: null, data_file: df.record }], mPath);
      addFile(t, { kind: "manifest", format: "Avro", path: mPath, name: fileName(mPath), bytes: built.bytes, built, snapshotId: sid, entries: 1 });
      list = [manifestFileRecord(mPath, built.bytes.length, { seq, minSeq: seq, sid, added: 1, addedRows: rowsById.length }), ...t.manifests];
    }
    t.manifests = list;

    const lPath = `${t.location}/metadata/snap-${sid}-1-${commit}.avro`;
    const lBuilt = await writeManifestList(t, list, lPath, { sid, parent, seq });
    addFile(t, { kind: "list", format: "Avro", path: lPath, name: fileName(lPath), bytes: lBuilt.bytes, built: lBuilt, snapshotId: sid, manifests: list.length });

    const totalRows = t.dataFiles.reduce((s, d) => s + d.rows.length, 0);
    const totalSize = t.dataFiles.reduce((s, d) => s + d.built.bytes.length, 0);
    const snap = snapshotObject({
      seq, sid, parent, time: t.time, manifestList: lPath, schemaId: t.currentSchemaId,
      summary: summaryFor("append", { addedFiles: 1, addedRows: rowsById.length, addedSize: df.built.bytes.length, totalRows, totalSize, totalFiles: t.dataFiles.length }),
    });
    t.snapshots.push(snap);
    t.snapshotLog.push({ "timestamp-ms": t.time, "snapshot-id": sid });
    writeMetadata(t);
    return snap;
  }

  /**
   * Schema change as a metadata-only commit. op: { kind: "add", name, type }
   * | { kind: "rename", id, name } | { kind: "drop", id } | { kind: "retype", id, type }.
   * Returns { ok, reason }.
   */
  function evolve(t, op) {
    const cur = schemaOf(t);
    const fields = cur.fields.map((f) => ({ ...f }));
    const f = op.id !== undefined ? fields.find((x) => x.id === op.id) : null;
    if (op.id !== undefined && !f) return { ok: false, reason: `no field with id ${op.id}` };
    if ((op.kind === "add" || op.kind === "rename") && fields.some((x) => x.name === op.name)) return { ok: false, reason: `a field named '${op.name}' already exists` };
    if (op.kind === "add") {
      fields.push({ id: ++t.lastColumnId, name: op.name, required: false, type: op.type });
    } else if (op.kind === "rename") {
      f.name = op.name;
    } else if (op.kind === "drop") {
      fields.splice(fields.indexOf(f), 1);
    } else if (op.kind === "retype") {
      if (!(PROMOTIONS[f.type] || []).includes(op.type)) return { ok: false, reason: `cannot change ${f.name} from ${f.type} to ${op.type}: Iceberg only allows widening (int to long, float to double, decimal precision)` };
      f.type = op.type;
    } else {
      return { ok: false, reason: `unknown operation ${op.kind}` };
    }
    const schemaId = Math.max(...t.schemas.map((s) => s.schemaId)) + 1;
    t.schemas.push({ schemaId, fields });
    t.currentSchemaId = schemaId;
    t.time += 1000;
    writeMetadata(t);
    return { ok: true };
  }

  /**
   * Read every live data file with the current schema. byId is what an
   * Iceberg reader does; byName is what a reader of plain Parquet files does.
   */
  function scan(t, { byName = false } = {}) {
    const schema = schemaOf(t);
    const rows = [];
    t.dataFiles.forEach((d, fileIdx) => {
      const fileSchema = schemaOf(t, d.schemaId);
      for (const r of d.rows) {
        const values = schema.fields.map((f) => {
          const src = byName ? fileSchema.fields.find((x) => x.name === f.name) : fileSchema.fields.find((x) => x.id === f.id);
          return src && src.id in r ? r[src.id] : null;
        });
        rows.push({ file: fileIdx, values });
      }
    });
    return { fields: schema.fields, rows };
  }

  function totals(t) {
    const by = (k) => t.files.filter((f) => f.kind === k);
    const sum = (fs) => fs.reduce((s, f) => s + f.bytes.length, 0);
    return Object.fromEntries(["data", "manifest", "list", "metadata"].map((k) => [k, { files: by(k).length, bytes: sum(by(k)) }]));
  }

  function interp(points, x) {
    if (x <= points[0][0]) return points[0][1];
    for (let i = 1; i < points.length; i++) {
      const [x1, y1] = points[i];
      if (x <= x1 || i === points.length - 1) {
        const [x0, y0] = points[i - 1];
        return y0 + ((y1 - y0) * (x - x0)) / (x1 - x0);
      }
    }
    return points[points.length - 1][1];
  }

  /**
   * Measure the writer at a few sizes so growth() can extrapolate:
   * data file size, manifest size by entry count, manifest list size by
   * manifest count, metadata.json size by snapshot count.
   */
  async function calibrate({ rowsPerCommit, maxCommits, seed = 11 }) {
    const t = create({ seed });
    for (let i = 0; i < 3; i++) await append(t, readings(t, rowsPerCommit));
    const data = t.dataFiles.reduce((s, d) => s + d.built.bytes.length, 0) / t.dataFiles.length;
    const proto = t.dataFiles[t.dataFiles.length - 1];
    const sizes = [1, 10, 100, 1000, 10000].filter((n) => n <= Math.max(1000, maxCommits));
    if (sizes[sizes.length - 1] < maxCommits) sizes.push(maxCommits);

    const manifest = [];
    for (const k of sizes.filter((n) => n <= 1000)) {
      const entries = Array.from({ length: k }, (_, i) => ({ status: i ? 0 : 1, snapshot_id: snapshotId(t.r), sequence_number: i ? i : null, file_sequence_number: i ? i : null, data_file: { ...proto.record, file_path: proto.path.replace(/-\d+-[0-9a-f-]{36}-/, `-${i}-${uuid(t.r)}-`) } }));
      manifest.push([k, (await writeManifest(t, entries, "m.avro")).bytes.length]);
    }
    const list = [];
    for (const m of sizes.filter((n) => n <= 1000)) {
      const recs = Array.from({ length: m }, (_, i) => manifestFileRecord(`${t.location}/metadata/${uuid(t.r)}-m0.avro`, manifest[0][1] + (i % 7), { seq: m - i, minSeq: m - i, sid: snapshotId(t.r), added: 1, addedRows: rowsPerCommit }));
      list.push([m, (await writeManifestList(t, recs, "l.avro", { sid: 1, parent: 1, seq: m })).bytes.length]);
    }
    const meta = [[0, DF.utf8(JSON.stringify(metadataObject(create({ seed })))).length]];
    for (const n of sizes) {
      const fake = { ...t, snapshots: [], snapshotLog: [], metadataLog: [] };
      for (let i = 0; i < n; i++) {
        const sid = snapshotId(t.r);
        fake.snapshots.push(snapshotObject({ seq: i + 1, sid, parent: i ? fake.snapshots[i - 1]["snapshot-id"] : null, time: START + i * 60000, schemaId: 0,
          manifestList: `${t.location}/metadata/snap-${sid}-1-${uuid(t.r)}.avro`,
          summary: summaryFor("append", { addedFiles: 1, addedRows: rowsPerCommit, addedSize: Math.round(data), totalRows: rowsPerCommit * (i + 1), totalSize: Math.round(data) * (i + 1), totalFiles: i + 1 }) }));
        fake.snapshotLog.push({ "timestamp-ms": START + i * 60000, "snapshot-id": sid });
      }
      for (let i = Math.max(0, n - 100); i < n; i++) fake.metadataLog.push({ "timestamp-ms": START + i * 60000, "metadata-file": `${t.location}/metadata/${String(i).padStart(5, "0")}-${uuid(t.r)}.metadata.json` });
      fake.lastSeq = n;
      meta.push([n, DF.utf8(JSON.stringify(metadataObject(fake))).length]);
    }
    return { rowsPerCommit, data, manifest, list, meta };
  }

  /**
   * Bytes on storage after each of N commits, one data file per commit,
   * nothing expired, compacted or cleaned up. Returns per-commit cumulative
   * series for data files, manifests, manifest lists and metadata.json.
   */
  function growth(cal, { commits, merge = false, minCountToMerge = 100, deleteAfterCommit = false, previousVersionsMax = 100, points = 400 }) {
    const M = (k) => interp(cal.manifest, k);
    const L = (m) => interp(cal.list, m);
    const J = (n) => interp(cal.meta, n);
    const acc = { data: 0, manifest: 0, list: 0, metadata: J(0) };
    const files = { data: 0, manifest: 0, list: 0, metadata: 1 };
    const metaSizes = [J(0)];
    let live = 0;
    const step = Math.max(1, Math.ceil(commits / points));
    const series = [{ n: 0, ...acc, files: { ...files } }];
    for (let n = 1; n <= commits; n++) {
      acc.data += cal.data;
      files.data++;
      if (merge && live + 1 >= minCountToMerge) {
        acc.manifest += M(n);
        live = 1;
      } else {
        acc.manifest += M(1);
        live++;
      }
      files.manifest++;
      acc.list += L(live);
      files.list++;
      const j = J(n);
      metaSizes.push(j);
      acc.metadata += j;
      files.metadata++;
      if (deleteAfterCommit && metaSizes.length > previousVersionsMax + 1) {
        acc.metadata -= metaSizes[metaSizes.length - previousVersionsMax - 2];
        files.metadata = previousVersionsMax + 1;
      }
      if (n % step === 0 || n === commits) series.push({ n, ...acc, files: { ...files } });
    }
    return series;
  }

  /** One compacted data file of totalRows rows: written up to 20,000 rows, scaled linearly beyond. */
  async function compactedSize(totalRows, seed = 13) {
    const t = create({ seed });
    const n = Math.max(1, Math.min(totalRows, 20000));
    const df = await writeDataFile(t, readings(t, n), uuid(t.r), 0);
    return (df.built.bytes.length * totalRows) / n;
  }

  /** What is left after rewrite_data_files, expire_snapshots (keep 1) and removing old metadata files. */
  async function maintained(cal, totalRows) {
    return {
      data: await compactedSize(totalRows),
      metadata: interp(cal.meta, 1) + interp(cal.list, 1) + interp(cal.manifest, 1),
    };
  }

  Object.assign(IB, { create, append, evolve, scan, readings, schemaOf, currentSnapshot, totals, calibrate, growth, maintained });
})(globalThis);
