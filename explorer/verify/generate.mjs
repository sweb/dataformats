/**
 * Write sample files with the explorer codecs so check.py can read them back
 * with the reference libraries. Usage: node explorer/verify/generate.mjs
 */
import fs from "fs";
import path from "path";
import vm from "vm";
import { fileURLToPath } from "url";

const here = path.dirname(fileURLToPath(import.meta.url));
const out = path.join(here, "out");
fs.mkdirSync(out, { recursive: true });
for (const f of ["common.js", "protobuf-codec.js", "avro-codec.js", "parquet-codec.js", "arrow-codec.js", "iceberg-model.js"]) {
  vm.runInThisContext(fs.readFileSync(path.join(here, "..", f), "utf8"), { filename: f });
}
const { PB, AV, PQ, AR, IB } = globalThis.DF;
const write = (name, bytes) => fs.writeFileSync(path.join(out, name), bytes);

const proto = PB.parseProto(`syntax = "proto3";
message Reading {
  uint64 device_id = 1;
  Location location = 2;
  repeated float temps = 3;
  repeated string tags = 4;
  Status status = 5;
  sint32 delta = 6;
  int32 neg = 7;
  double d = 8;
  bool on = 9;
  int32 zero = 10;
}
message Location { string site = 1; int32 floor = 2; }
enum Status { OK = 0; WARN = 1; FAULT = 2; }`);
write("reading.pb", PB.encode(proto, "Reading", {
  device_id: 4711, location: { site: "hall-b", floor: 2 }, temps: [23.5, 19.25], tags: ["a", "bb"],
  status: "WARN", delta: -3, neg: -1, d: 1.5, on: true, zero: 0,
}).bytes);

const flat = PB.parseProto(`syntax = "proto3";
message Reading {
  uint64 device_id = 1;
  string location = 2;
  float temp_c = 3;
}`);
const many = [
  { device_id: 4711, location: "hall-b", temp_c: 23.5 },
  { device_id: 4712, location: "hall-a", temp_c: 19.25 },
  { device_id: 4713, location: "yard", temp_c: 0 },
];
fs.writeFileSync(path.join(out, "pb_many.json"), JSON.stringify(many));
for (const framing of ["delimited", "wrapper"]) write(`readings.${framing}.pb`, PB.encodeMany(flat, "Reading", many, framing).bytes);

const r42 = DF.rng(42);
const page500 = Array.from({ length: 500 }, () => ({
  device_id: r42.int(1000, 9999),
  location: r42.pick(["hall-a", "hall-b", "yard", "cold-storage"]),
  temp_c: Math.round((15 + r42.next() * 30) * 4) / 4,
}));
const rawCases = {
  reading: PB.encode(proto, "Reading", { device_id: 4711, location: { site: "hall-b", floor: 2 }, temps: [23.5, 19.25], tags: ["a", "bb"], status: "WARN", delta: -3, neg: -1, d: 1.5, on: true, zero: 0 }).bytes,
  first_of_500: PB.encodeMany(flat, "Reading", page500, "delimited").payloads[0],
  delimited_500: PB.encodeMany(flat, "Reading", page500, "delimited").bytes,
  wrapper_500: PB.encodeMany(flat, "Reading", page500, "wrapper").bytes,
  edge_cases: Uint8Array.from([0x22, 0x00, 0x32, 0x09, 0x27, 0x0d, 0x09, 0x7f, 0x7e, 0x22, 0x5c, 0xc3, 0xa4, 0x0b, 0x08, 0x01, 0x0c, 0x12, 0x02, 0x0b, 0x0c, 0x12, 0x03, 0x0b, 0x08, 0x01, 0x39, 1, 2, 3, 4, 5, 6, 7, 8, 0xf8, 0xff, 0xff, 0xff, 0x0f, 0x01, 0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01]),
  bad_wire_type: Uint8Array.from([0x0e, 0x01]),
};
const rawExpected = {};
for (const [name, bytes] of Object.entries(rawCases)) {
  write(`raw_${name}.bin`, bytes);
  try {
    rawExpected[name] = PB.formatRaw(PB.decodeRaw(bytes));
  } catch {
    rawExpected[name] = null;
  }
}
fs.writeFileSync(path.join(out, "decode_raw.json"), JSON.stringify(rawExpected));

const schema = { type: "record", name: "Reading", namespace: "x.y", fields: [
  { name: "device_id", type: "long" }, { name: "location", type: "string" }, { name: "temp_c", type: "float" },
  { name: "status", type: { type: "enum", name: "Status", symbols: ["OK", "WARN"] } },
  { name: "tags", type: { type: "array", items: "string" } },
  { name: "attrs", type: { type: "map", values: "int" } },
  { name: "note", type: ["null", "string"], default: null },
  { name: "loc", type: { type: "record", name: "Loc", fields: [{ name: "x", type: "double" }] } },
  { name: "id", type: { type: "fixed", name: "Id", size: 4 } },
] };
const records = [
  { device_id: 4711, location: "hall-b", temp_c: 23.5, status: "WARN", tags: ["a", "b"], attrs: { k: -5 }, note: "hi", loc: { x: 1.5 }, id: "abcd" },
  { device_id: -4712, location: "hall-a", temp_c: 19.25, status: "OK", tags: [], attrs: {}, note: null, loc: { x: -2 }, id: "wxyz" },
];
fs.writeFileSync(path.join(out, "avro_records.json"), JSON.stringify(records));
for (const codec of ["null", "deflate"]) write(`readings.${codec}.avro`, (await AV.build(schema, records, { mode: "ocf", codec, blockSize: 1 })).bytes);
const single = await AV.build(schema, records, { mode: "single" });
fs.writeFileSync(path.join(out, "fingerprint.json"), JSON.stringify({ canonical: single.info.canonical, fingerprint: single.info.fingerprint.toString() }));

const rows = PQ.makeRows(2000);
fs.writeFileSync(path.join(out, "parquet_rows.json"), JSON.stringify(rows));
const variants = { plain: [null, "none", true], dict_by_location: ["location", "none", false], gzip_by_temp: ["temp_c", "gzip", false] };
for (const [name, [sortBy, codec, plain]] of Object.entries(variants)) {
  const cols = PQ.COLUMNS.map((c) => ({ ...c, encoding: plain ? "PLAIN" : c.encoding }));
  const sorted = PQ.sortRows(rows, sortBy);
  const file = await PQ.build(sorted, cols, { rowGroupSize: 250, codec, sortBy });
  write(`${name}.parquet`, file.bytes);
  const plan = PQ.plan(file, sorted, { select: ["temp_c"], where: { col: "temp_c", op: ">", value: 40 } });
  fs.writeFileSync(path.join(out, `${name}.plan.json`), JSON.stringify({ matched: plan.matched, read: plan.decisions.map((d) => d.read) }));
}
const nullRows = rows.slice(0, 300).map((r, i) => ({
  device_id: r.device_id,
  location: i % 7 === 0 ? null : r.location,
  temp_c: i % 3 === 0 ? null : Math.round(r.temp_c * 4) / 4,
}));
const nullCols = [
  { name: "device_id", ptype: "INT32", encoding: "PLAIN" },
  { name: "location", ptype: "BYTE_ARRAY", converted: "UTF8", encoding: "DICT", nullable: true },
  { name: "temp_c", ptype: "FLOAT", encoding: "PLAIN", nullable: true },
];
fs.writeFileSync(path.join(out, "nullable_rows.json"), JSON.stringify(nullRows));
write("nullable.parquet", (await PQ.build(nullRows, nullCols, { rowGroupSize: 128 })).bytes);

const arrowCols = AR.parseSchema("device_id: int32 not null\nts: int64 not null\nlocation: utf8\ntemp_c: float32\nx: float64\nok: bool");
const arrowRows = rows.slice(0, 50).map((r, i) => ({
  device_id: r.device_id, ts: r.ts, location: i % 5 === 0 ? null : r.location,
  temp_c: i % 3 === 0 ? null : Math.round(r.temp_c * 4) / 4, x: i % 7 === 0 ? null : r.temp_c, ok: i % 11 === 0 ? null : i % 2 === 0,
}));
fs.writeFileSync(path.join(out, "arrow_rows.json"), JSON.stringify(arrowRows));
write("readings.arrow", AR.build(arrowCols, arrowRows, { batchRows: 16 }).bytes);

const ibRoot = path.join(out, "iceberg");
fs.rmSync(ibRoot, { recursive: true, force: true });
const table = IB.create({ location: `file://${ibRoot}/telemetry`, seed: 5, manifestCodec: "null" });
for (let i = 0; i < 3; i++) await IB.append(table, IB.readings(table, 40));
const firstSnapshot = IB.currentSnapshot(table)["snapshot-id"];
const ops = [{ kind: "rename", id: 2, name: "site" }, { kind: "add", name: "firmware", type: "string" }, { kind: "retype", id: 1, type: "long" }];
for (const op of ops) if (!IB.evolve(table, op).ok) throw new Error(JSON.stringify(op));
await IB.append(table, IB.readings(table, 7));
if (!IB.evolve(table, { kind: "drop", id: 3 }).ok) throw new Error("drop");
await IB.append(table, IB.readings(table, 5));
const rejected = IB.evolve(table, { kind: "retype", id: 2, type: "int" });
for (const f of table.files) {
  const p = f.path.replace("file://", "");
  fs.mkdirSync(path.dirname(p), { recursive: true });
  fs.writeFileSync(p, f.bytes);
}
const scanned = IB.scan(table);
const bounds = table.dataFiles.map((d) => ({
  path: d.path,
  fields: IB.schemaOf(table, d.schemaId).fields.map((f) => {
    const vals = d.rows.map((r) => r[f.id]).filter((v) => v !== null && v !== undefined).sort((a, b) => (a < b ? -1 : a > b ? 1 : 0));
    return { id: f.id, type: f.type, min: vals[0], max: vals[vals.length - 1] };
  }),
}));
const lastManifest = table.files.filter((f) => f.kind === "manifest").pop();
const lastBounds = bounds[bounds.length - 1];
const shownBounds = [...globalThis.DF.walk(lastManifest.built.root)].filter((n) => /^field id \d+$/.test(n.label)).map((n) => n.meta.value);
const wantBounds = ["min", "max"].flatMap((side) => lastBounds.fields.map((f) => (f.type === "string" ? JSON.stringify(f[side]) : String(f[side]))));
if (JSON.stringify(shownBounds) !== JSON.stringify(wantBounds)) throw new Error(`manifest bound labels ${JSON.stringify(shownBounds)} != ${JSON.stringify(wantBounds)}`);
fs.writeFileSync(path.join(out, "iceberg_expected.json"), JSON.stringify({
  metadata: table.files.filter((f) => f.kind === "metadata").pop().path.replace("file://", ""),
  columns: scanned.fields.map((f) => f.name),
  types: scanned.fields.map((f) => f.type),
  rows: scanned.rows.map((r) => r.values),
  snapshots: table.snapshots.length,
  firstSnapshot, firstRows: 120,
  rejected: !rejected.ok,
  bounds,
}));

const modelRuns = [];
for (const merge of [false, true]) {
  const real = IB.create({ seed: 3, merge });
  const commits = 150;
  for (let i = 0; i < commits; i++) await IB.append(real, IB.readings(real, 100));
  const cal = await IB.calibrate({ rowsPerCommit: 100, maxCommits: commits });
  const model = IB.growth(cal, { commits, merge }).pop();
  const tot = IB.totals(real);
  modelRuns.push({ merge, commits, kinds: Object.fromEntries(Object.entries(tot).map(([k, v]) => [k, { real: v.bytes, model: model[k], realFiles: v.files, modelFiles: model.files[k] }])) });
}
fs.writeFileSync(path.join(out, "iceberg_growth.json"), JSON.stringify(modelRuns));

console.log(`wrote ${fs.readdirSync(out).length} files to ${out}`);
