"use strict";

/**
 * Avro: schema parsing, annotated binary encoding, the four ways a datum is
 * framed (raw, object container file, single-object encoding, Confluent wire
 * format), Parsing Canonical Form with CRC-64-AVRO, and schema resolution.
 */
(function (g) {
  const DF = g.DF;
  const AV = (DF.AV = {});

  const PRIMITIVES = new Set(["null", "boolean", "int", "long", "float", "double", "bytes", "string"]);
  const MAGIC = [0x4f, 0x62, 0x6a, 0x01];

  function fullName(name, ns) {
    if (name.includes(".")) return name;
    return ns ? `${ns}.${name}` : name;
  }

  function parseSchema(json) {
    const names = {};
    const root = norm(json, "", names);
    return { root, names };
  }

  function norm(s, ns, names) {
    if (typeof s === "string") {
      if (PRIMITIVES.has(s)) return { type: s };
      const hit = names[fullName(s, ns)] || names[s];
      if (!hit) throw new Error(`unknown type '${s}'`);
      return hit;
    }
    if (Array.isArray(s)) {
      if (!s.length) throw new Error("empty union");
      return { type: "union", branches: s.map((b) => norm(b, ns, names)) };
    }
    if (!s || typeof s !== "object") throw new Error(`invalid schema: ${JSON.stringify(s)}`);
    if (PRIMITIVES.has(s.type)) return { type: s.type, logicalType: s.logicalType };
    switch (s.type) {
      case "record":
      case "error": {
        if (!s.name) throw new Error("record without name");
        const space = s.namespace ?? (s.name.includes(".") ? s.name.slice(0, s.name.lastIndexOf(".")) : ns);
        const full = fullName(s.name, space);
        const rec = { type: "record", name: s.name.split(".").pop(), fullname: full, namespace: space, aliases: s.aliases || [], fields: [] };
        names[full] = rec;
        if (!Array.isArray(s.fields)) throw new Error(`record ${s.name} needs a fields array`);
        const seen = new Set();
        for (const f of s.fields) {
          if (!f.name) throw new Error(`field without name in ${s.name}`);
          if (seen.has(f.name)) throw new Error(`duplicate field '${f.name}' in ${s.name}`);
          seen.add(f.name);
          rec.fields.push({ name: f.name, type: norm(f.type, space, names), hasDefault: "default" in f, default: f.default, aliases: f.aliases || [], doc: f.doc });
        }
        return rec;
      }
      case "enum": {
        const space = s.namespace ?? ns;
        const full = fullName(s.name, space);
        const e = { type: "enum", name: s.name.split(".").pop(), fullname: full, symbols: s.symbols || [], default: s.default, aliases: s.aliases || [] };
        names[full] = e;
        return e;
      }
      case "fixed": {
        const space = s.namespace ?? ns;
        const full = fullName(s.name, space);
        const f = { type: "fixed", name: s.name.split(".").pop(), fullname: full, size: s.size, aliases: s.aliases || [] };
        names[full] = f;
        return f;
      }
      case "array":
        return { type: "array", items: norm(s.items, ns, names) };
      case "map":
        return { type: "map", values: norm(s.values, ns, names) };
      default:
        if (typeof s.type === "string" || typeof s.type === "object") return norm(s.type, ns, names);
    }
    throw new Error(`unsupported schema type '${s.type}'`);
  }

  function typeName(s) {
    switch (s.type) {
      case "record":
      case "enum":
      case "fixed":
        return s.name;
      case "array":
        return `array<${typeName(s.items)}>`;
      case "map":
        return `map<${typeName(s.values)}>`;
      case "union":
        return `[${s.branches.map(typeName).join(", ")}]`;
      default:
        return s.logicalType ? `${s.type} (${s.logicalType})` : s.type;
    }
  }

  function branchKey(s) {
    return s.fullname || s.type;
  }

  function matches(s, v) {
    switch (s.type) {
      case "null": return v === null;
      case "boolean": return typeof v === "boolean";
      case "int": return Number.isInteger(v) && v >= -2147483648 && v <= 2147483647;
      case "long": return Number.isInteger(v) || typeof v === "bigint";
      case "float":
      case "double": return typeof v === "number";
      case "string": return typeof v === "string";
      case "bytes": return typeof v === "string" || v instanceof Uint8Array;
      case "enum": return typeof v === "string" && s.symbols.includes(v);
      case "fixed": return typeof v === "string" && DF.utf8(v).length === s.size;
      case "array": return Array.isArray(v);
      case "map":
      case "record": return v !== null && typeof v === "object" && !Array.isArray(v) && !(v instanceof Uint8Array);
    }
    return false;
  }

  function pickBranch(u, v) {
    if (v && typeof v === "object" && !Array.isArray(v) && !(v instanceof Uint8Array)) {
      const keys = Object.keys(v);
      if (keys.length === 1) {
        const idx = u.branches.findIndex((b) => branchKey(b) === keys[0] || b.name === keys[0]);
        if (idx >= 0) return [idx, v[keys[0]]];
      }
    }
    const idx = u.branches.findIndex((b) => matches(b, v));
    if (idx < 0) throw new Error(`value ${DF.shorten(JSON.stringify(v), 30)} matches no branch of ${typeName(u)}`);
    return [idx, v];
  }

  function longLeaf(w, label, n, meta) {
    const big = BigInt(n);
    const z = DF.zigzag(big);
    return w.leaf(label, DF.varint(z), {
      varint: true, zigzag: true, value: big.toString(),
      rule: `zigzag(${big}) = ${z}\nvarint(${z})`,
      ...meta,
    });
  }

  /** Encode one datum. Note what is missing: no tags, no names, no field boundaries. */
  function enc(w, s, v, label) {
    switch (s.type) {
      case "null":
        if (v !== null) throw new Error(`${label}: expected null`);
        return w.leaf(label, [], { kind: "value", type: "null", value: "null", rule: "0 bytes", desc: "null is written as nothing at all." });
      case "boolean":
        if (typeof v !== "boolean") throw new Error(`${label}: expected boolean`);
        return w.leaf(label, [v ? 1 : 0], { kind: "value", type: "boolean", value: String(v), rule: "1 byte, 0 or 1" });
      case "int":
      case "long": {
        if (!(Number.isInteger(v) || typeof v === "bigint" || /^-?\d+$/.test(v))) throw new Error(`${label}: expected an integer`);
        if (s.type === "int" && (Number(v) > 2147483647 || Number(v) < -2147483648)) throw new Error(`${label}: ${v} does not fit an int`);
        return longLeaf(w, label, v, {
          kind: "value", type: `${s.type}${s.logicalType ? ` (${s.logicalType})` : ""} (zigzag varint)`,
          desc: "Avro writes every int and long as a zigzag varint: small magnitudes, positive or negative, take few bytes.",
        });
      }
      case "float":
        if (typeof v !== "number") throw new Error(`${label}: expected a number`);
        return w.leaf(label, DF.f32(v), { kind: "value", type: "float", value: String(new DataView(DF.f32(v).buffer).getFloat32(0, true)), rule: "IEEE 754 binary32, little endian, 4 bytes" });
      case "double":
        if (typeof v !== "number") throw new Error(`${label}: expected a number`);
        return w.leaf(label, DF.f64(v), { kind: "value", type: "double", value: String(v), rule: "IEEE 754 binary64, little endian, 8 bytes" });
      case "string":
      case "bytes": {
        const binary = s.type === "bytes" && v instanceof Uint8Array;
        if (typeof v !== "string" && !binary) throw new Error(`${label}: expected a string`);
        const payload = binary ? v : DF.utf8(v);
        const shown = binary ? DF.hex(v, 16) : JSON.stringify(DF.shorten(v, 40));
        return w.group(label, { kind: "field", type: s.type, value: shown }, () => {
          longLeaf(w, "length", payload.length, { kind: "length", type: "long (zigzag varint)", desc: `Length prefix: zigzag(${payload.length}) = ${payload.length * 2}. The only framing inside an Avro record.` });
          w.leaf("bytes", payload, { kind: "value", type: s.type === "string" ? "UTF-8 bytes" : "raw bytes", value: binary ? DF.hex(v, 24) : JSON.stringify(DF.shorten(v, 60)), rule: `${payload.length} bytes` });
        });
      }
      case "fixed": {
        const payload = DF.utf8(String(v));
        if (payload.length !== s.size) throw new Error(`${label}: fixed ${s.name} needs exactly ${s.size} bytes`);
        return w.leaf(label, payload, { kind: "value", type: `fixed ${s.name} (${s.size})`, value: JSON.stringify(v), rule: `exactly ${s.size} bytes, no length prefix`, desc: "The size lives in the schema, so not even a length is written." });
      }
      case "enum": {
        const idx = s.symbols.indexOf(v);
        if (idx < 0) throw new Error(`${label}: '${v}' is not a symbol of ${s.name}`);
        return longLeaf(w, label, idx, { kind: "value", type: `enum ${s.name} (zigzag varint)`, value: `${v} (#${idx})`, desc: `Enums are written as the symbol's position in the schema: ${s.symbols.map((x, i) => `${i}=${x}`).join(", ")}. Reordering symbols changes meaning.` });
      }
      case "record": {
        if (!v || typeof v !== "object" || Array.isArray(v)) throw new Error(`${label}: expected an object for record ${s.name}`);
        for (const k of Object.keys(v)) if (!s.fields.some((f) => f.name === k)) throw new Error(`${label}: '${k}' is not a field of ${s.name}`);
        return w.group(label, { kind: "record", type: `record ${s.name}`, value: `${s.fields.length} fields`, desc: "A record is its fields' values back to back, in schema order. No field names, no tags, no lengths, no separators." }, () => {
          for (const f of s.fields) {
            let fv = v[f.name];
            if (fv === undefined) {
              if (!f.hasDefault) throw new Error(`${label}.${f.name}: missing and no default`);
              fv = f.default;
            }
            enc(w, f.type, fv, f.name);
          }
        });
      }
      case "array": {
        if (!Array.isArray(v)) throw new Error(`${label}: expected an array`);
        return w.group(label, { kind: "field", type: typeName(s), value: `${v.length} items`, desc: "Arrays are written in blocks: an item count, the items, and a 0 count to end." }, () => {
          if (v.length) {
            longLeaf(w, "block count", v.length, { kind: "count", type: "long (zigzag varint)" });
            v.forEach((x, i) => enc(w, s.items, x, `[${i}]`));
          }
          longLeaf(w, "end of array", 0, { kind: "count", type: "long (zigzag varint)", desc: "A block count of 0 terminates the array." });
        });
      }
      case "map": {
        if (!v || typeof v !== "object" || Array.isArray(v)) throw new Error(`${label}: expected an object for map`);
        const entries = Object.entries(v);
        return w.group(label, { kind: "field", type: typeName(s), value: `${entries.length} entries`, desc: "Maps are blocks of (string key, value) pairs, ended by a 0 count." }, () => {
          if (entries.length) {
            longLeaf(w, "block count", entries.length, { kind: "count", type: "long (zigzag varint)" });
            for (const [k, x] of entries) {
              enc(w, { type: "string" }, k, `key "${k}"`);
              enc(w, s.values, x, `value "${k}"`);
            }
          }
          longLeaf(w, "end of map", 0, { kind: "count", type: "long (zigzag varint)" });
        });
      }
      case "union": {
        const [idx, inner] = pickBranch(s, v);
        return w.group(label, { kind: "field", type: typeName(s), value: `branch ${idx}: ${typeName(s.branches[idx])}`, desc: "A union is the branch index followed by the value in that branch's encoding. Optional fields are just [\"null\", T] unions." }, () => {
          longLeaf(w, "branch index", idx, { kind: "count", type: "long (zigzag varint)", desc: `Selects branch ${idx} (${typeName(s.branches[idx])}) of ${typeName(s)}.` });
          enc(w, s.branches[idx], inner, "value");
        });
      }
    }
    throw new Error(`unsupported type ${s.type}`);
  }

  function encodeDatum(schema, value, label = "record") {
    const w = new DF.ByteWriter();
    enc(w, schema, value, label);
    return w.finish();
  }

  class Reader {
    constructor(bytes, pos = 0) {
      this.b = bytes;
      this.pos = pos;
    }

    long() {
      const r = DF.readVarint(this.b, this.pos);
      this.pos = r.pos;
      return DF.unzigzag(r.value);
    }

    int() {
      const v = this.long();
      if (v > 2147483647n || v < -2147483648n) throw new Error(`int out of range at ${this.pos}`);
      return Number(v);
    }

    take(n) {
      if (n < 0 || this.pos + n > this.b.length) throw new Error(`need ${n} bytes at offset ${this.pos}, only ${this.b.length - this.pos} left`);
      const out = this.b.subarray(this.pos, this.pos + n);
      this.pos += n;
      return out;
    }
  }

  function longValue(v) {
    return v >= -9007199254740991n && v <= 9007199254740991n ? Number(v) : v;
  }

  function dec(r, s) {
    switch (s.type) {
      case "null": return null;
      case "boolean": {
        const b = r.take(1)[0];
        if (b > 1) throw new Error(`invalid boolean byte ${b}`);
        return b === 1;
      }
      case "int": return r.int();
      case "long": return longValue(r.long());
      case "float": return DF.view(r.take(4), 0).getFloat32(0, true);
      case "double": return DF.view(r.take(8), 0).getFloat64(0, true);
      case "string": {
        const n = Number(r.long());
        const s2 = DF.tryUtf8(r.take(n));
        if (s2 === null) throw new Error("invalid UTF-8 in string");
        return s2;
      }
      case "bytes": return DF.decodeUtf8(r.take(Number(r.long())));
      case "fixed": return DF.decodeUtf8(r.take(s.size));
      case "enum": {
        const i = r.int();
        if (i < 0 || i >= s.symbols.length) throw new Error(`enum index ${i} out of range`);
        return s.symbols[i];
      }
      case "record": {
        const out = {};
        for (const f of s.fields) out[f.name] = dec(r, f.type);
        return out;
      }
      case "array": {
        const out = [];
        for (let n = Number(r.long()); n !== 0; n = Number(r.long())) {
          if (n < 0) {
            n = -n;
            r.long();
          }
          if (n > 1e6) throw new Error(`implausible block count ${n}`);
          for (let i = 0; i < n; i++) out.push(dec(r, s.items));
        }
        return out;
      }
      case "map": {
        const out = {};
        for (let n = Number(r.long()); n !== 0; n = Number(r.long())) {
          if (n < 0) {
            n = -n;
            r.long();
          }
          if (n > 1e6) throw new Error(`implausible block count ${n}`);
          for (let i = 0; i < n; i++) out[dec(r, { type: "string" })] = dec(r, s.values);
        }
        return out;
      }
      case "union": {
        const i = r.int();
        if (i < 0 || i >= s.branches.length) throw new Error(`union branch ${i} out of range`);
        return dec(r, s.branches[i]);
      }
    }
    throw new Error(`cannot decode ${s.type}`);
  }

  const PROMOTIONS = {
    int: ["long", "float", "double"],
    long: ["float", "double"],
    float: ["double"],
    string: ["bytes"],
    bytes: ["string"],
  };

  function sameName(w, r) {
    return w.name === r.name || r.aliases.some((a) => a.split(".").pop() === w.name);
  }

  function canResolve(ws, rs) {
    try {
      resolver(ws, rs, "", null);
      return true;
    } catch {
      return false;
    }
  }

  /**
   * Build a reader function that decodes writer bytes into the reader's shape,
   * following the spec's resolution rules. Throws when the schemas cannot be
   * reconciled, which is what a real consumer does before reading a single byte.
   */
  function resolver(ws, rs, path, report) {
    const where = path || "(root)";
    if (ws.type === "union") {
      const branches = ws.branches.map((b) => {
        try {
          return resolver(b, rs, path, null);
        } catch (e) {
          return () => {
            throw e;
          };
        }
      });
      return (r) => {
        const i = r.int();
        if (i < 0 || i >= branches.length) throw new Error(`union branch ${i} out of range`);
        return branches[i](r);
      };
    }
    if (rs.type === "union") {
      const idx = rs.branches.findIndex((b) => b.type === ws.type && (!b.name || b.name === ws.name));
      const target = idx >= 0 ? idx : rs.branches.findIndex((b) => canResolve(ws, b));
      if (target < 0) throw new Error(`${where}: writer type ${typeName(ws)} matches no branch of reader union ${typeName(rs)}`);
      return resolver(ws, rs.branches[target], path, report);
    }
    if (PRIMITIVES.has(ws.type) && ws.type === rs.type) return (r) => dec(r, ws);
    if (PRIMITIVES.has(ws.type) && PROMOTIONS[ws.type]?.includes(rs.type)) {
      return (r) => {
        const v = dec(r, ws);
        if (rs.type === "long") return typeof v === "number" ? v : longValue(BigInt(v));
        return v;
      };
    }
    if (ws.type !== rs.type) throw new Error(`${where}: writer type ${typeName(ws)} cannot be read as ${typeName(rs)}`);
    switch (ws.type) {
      case "record": {
        if (!sameName(ws, rs)) throw new Error(`${where}: writer record ${ws.name} does not match reader record ${rs.name}`);
        const steps = [];
        const used = new Set();
        for (const wf of ws.fields) {
          const rf = rs.fields.find((f) => f.name === wf.name) || rs.fields.find((f) => f.aliases.includes(wf.name));
          const fpath = path ? `${path}.${wf.name}` : wf.name;
          if (rf) {
            used.add(rf.name);
            let fn;
            try {
              fn = resolver(wf.type, rf.type, fpath, report);
            } catch (e) {
              report?.push({ path: path ? `${path}.${rf.name}` : rf.name, status: "error", note: e.message.replace(/^[^:]*: /, "") });
              throw e;
            }
            steps.push({ fn, name: rf.name });
            if (report) {
              const notes = [];
              let status = "ok";
              if (rf.name !== wf.name) {
                status = "alias";
                notes.push(`writer field '${wf.name}' matched via alias`);
              }
              if (typeName(wf.type) !== typeName(rf.type)) {
                if (status === "ok") status = "promoted";
                notes.push(`${typeName(wf.type)} promoted to ${typeName(rf.type)}`);
              }
              report.push({ path: path ? `${path}.${rf.name}` : rf.name, status, note: notes.join("; ") || `from writer field '${wf.name}'` });
            }
          } else {
            steps.push({ fn: (r) => dec(r, wf.type), name: null });
            report?.push({ path: fpath, status: "skipped", note: `only in writer schema, decoded and thrown away (${typeName(wf.type)})` });
          }
        }
        const defaults = [];
        for (const rf of rs.fields) {
          if (used.has(rf.name)) continue;
          const fpath = path ? `${path}.${rf.name}` : rf.name;
          if (!rf.hasDefault) {
            report?.push({ path: fpath, status: "error", note: "reader needs it, writer never wrote it, and there is no default" });
            throw new Error(`${fpath}: reader field '${rf.name}' is missing in the writer schema and has no default`);
          }
          defaults.push([rf.name, rf.default]);
          report?.push({ path: fpath, status: "default", note: `not in writer schema, filled from default ${JSON.stringify(rf.default)}` });
        }
        const order = rs.fields.map((f) => f.name);
        return (r) => {
          const tmp = {};
          for (const st of steps) {
            const v = st.fn(r);
            if (st.name) tmp[st.name] = v;
          }
          for (const [k, d] of defaults) tmp[k] = d;
          const out = {};
          for (const k of order) out[k] = tmp[k];
          return out;
        };
      }
      case "enum": {
        if (!sameName(ws, rs)) throw new Error(`${where}: enum ${ws.name} does not match ${rs.name}`);
        return (r) => {
          const i = r.int();
          const sym = ws.symbols[i];
          if (rs.symbols.includes(sym)) return sym;
          if (rs.default !== undefined) return rs.default;
          throw new Error(`${where}: symbol '${sym}' unknown to reader and enum has no default`);
        };
      }
      case "fixed":
        if (!sameName(ws, rs) || ws.size !== rs.size) throw new Error(`${where}: fixed types differ`);
        return (r) => dec(r, ws);
      case "array": {
        const item = resolver(ws.items, rs.items, `${path}[]`, report);
        return (r) => {
          const out = [];
          for (let n = Number(r.long()); n !== 0; n = Number(r.long())) {
            if (n < 0) {
              n = -n;
              r.long();
            }
            for (let i = 0; i < n; i++) out.push(item(r));
          }
          return out;
        };
      }
      case "map": {
        const val = resolver(ws.values, rs.values, `${path}{}`, report);
        return (r) => {
          const out = {};
          for (let n = Number(r.long()); n !== 0; n = Number(r.long())) {
            if (n < 0) {
              n = -n;
              r.long();
            }
            for (let i = 0; i < n; i++) out[dec(r, { type: "string" })] = val(r);
          }
          return out;
        };
      }
    }
    throw new Error(`${where}: cannot resolve ${typeName(ws)} to ${typeName(rs)}`);
  }

  /** Resolve writer against reader and decode each datum. Returns either values or the error. */
  function resolveAndRead(writer, reader, datums) {
    const report = [];
    let fn;
    try {
      fn = resolver(writer, reader, "", report);
    } catch (e) {
      return { report, error: e.message };
    }
    try {
      return { report, values: datums.map((d) => fn(new Reader(d))) };
    } catch (e) {
      return { report, error: e.message };
    }
  }

  /** Decode bytes with only the reader schema, as if it were the writer schema. */
  function readPositionally(reader, datums) {
    const values = [];
    for (const d of datums) {
      const r = new Reader(d);
      try {
        const v = dec(r, reader);
        values.push({ value: v, leftover: d.length - r.pos });
      } catch (e) {
        values.push({ error: e.message });
      }
    }
    return values;
  }

  function canonical(s, seen = new Set()) {
    switch (s.type) {
      case "record": {
        if (seen.has(s.fullname)) return JSON.stringify(s.fullname);
        seen.add(s.fullname);
        const fields = s.fields.map((f) => `{"name":${JSON.stringify(f.name)},"type":${canonical(f.type, seen)}}`);
        return `{"name":${JSON.stringify(s.fullname)},"type":"record","fields":[${fields.join(",")}]}`;
      }
      case "enum":
        if (seen.has(s.fullname)) return JSON.stringify(s.fullname);
        seen.add(s.fullname);
        return `{"name":${JSON.stringify(s.fullname)},"type":"enum","symbols":[${s.symbols.map((x) => JSON.stringify(x)).join(",")}]}`;
      case "fixed":
        if (seen.has(s.fullname)) return JSON.stringify(s.fullname);
        seen.add(s.fullname);
        return `{"name":${JSON.stringify(s.fullname)},"type":"fixed","size":${s.size}}`;
      case "array":
        return `{"type":"array","items":${canonical(s.items, seen)}}`;
      case "map":
        return `{"type":"map","values":${canonical(s.values, seen)}}`;
      case "union":
        return `[${s.branches.map((b) => canonical(b, seen)).join(",")}]`;
      default:
        return JSON.stringify(s.type);
    }
  }

  const CRC_EMPTY = 0xc15d213aa4d7a795n;
  let crcTable = null;

  function crc64(bytes) {
    if (!crcTable) {
      crcTable = [];
      for (let i = 0; i < 256; i++) {
        let fp = BigInt(i);
        for (let j = 0; j < 8; j++) fp = (fp >> 1n) ^ (CRC_EMPTY & -(fp & 1n));
        crcTable.push(fp);
      }
    }
    let fp = CRC_EMPTY;
    for (const b of bytes) fp = (fp >> 8n) ^ crcTable[Number((fp ^ BigInt(b)) & 0xffn)];
    return fp;
  }

  function randomSync() {
    const b = new Uint8Array(16);
    (g.crypto || require("crypto").webcrypto).getRandomValues(b);
    return b;
  }

  /**
   * Frame encoded records. mode: "ocf" | "raw" | "single" | "confluent".
   * Returns { bytes, root, datums } where datums are the per-record encodings.
   */
  async function build(schemaJson, records, opts = {}) {
    const { mode = "ocf", codec = "null", blockSize = 100, blockBytes = 0, sync = randomSync(), schemaId = 1, meta: extraMeta = [] } = opts;
    const parsed = parseSchema(schemaJson);
    const schema = parsed.root;
    const datums = records.map((rec, i) => encodeDatum(schema, rec, `record ${i}`));
    const w = new DF.ByteWriter();
    const info = { datumBytes: datums.reduce((s, d) => s + d.bytes.length, 0) };

    const recordSpans = [];
    if (mode === "raw") {
      for (const d of datums) {
        recordSpans.push({ start: w.pos, end: w.pos + d.bytes.length });
        w.embed(d);
      }
      const out = w.finish("datums");
      out.root.meta = { type: "concatenated datums", value: `${records.length} record(s)`, desc: "Raw Avro binary: the records back to back. There is no marker where one record ends and the next begins. Only a reader holding the exact writer schema can find the boundaries." };
      return { ...out, datums: datums.map((d) => d.bytes), schema, info, recordSpans };
    }

    if (mode === "single" || mode === "confluent") {
      const pcf = canonical(schema);
      const fp = crc64(DF.utf8(pcf));
      info.canonical = pcf;
      info.fingerprint = fp;
      datums.forEach((d, i) => {
        w.group(`message ${i}`, { kind: "record", type: mode === "single" ? "single-object encoding" : "Confluent wire format", value: `${d.bytes.length + (mode === "single" ? 10 : 5)} bytes` }, () => {
          if (mode === "single") {
            w.leaf("marker", [0xc3, 0x01], { kind: "magic", type: "2 bytes", value: "C3 01", rule: "0xC3 0x01", desc: "Marks a single-object encoded Avro datum (Avro spec, 'Single object encoding')." });
            const fpBytes = DF.le64(fp);
            w.leaf("schema fingerprint", fpBytes, {
              kind: "meta", type: "CRC-64-AVRO, 8 bytes little endian", value: `0x${fp.toString(16).padStart(16, "0")}`,
              rule: `crc64(parsing canonical form)\n${DF.shorten(pcf, 120)}`,
              desc: "A 64-bit fingerprint of the schema's Parsing Canonical Form. The reader looks the full writer schema up by this fingerprint.",
            });
          } else {
            w.leaf("magic byte", [0x00], { kind: "magic", type: "1 byte", value: "0x00", rule: "always 0", desc: "Confluent Schema Registry framing, used by most Kafka Avro serializers." });
            w.leaf("schema id", DF.be32(schemaId), { kind: "meta", type: "int32, big endian", value: String(schemaId), rule: `schema id ${schemaId} as 4 bytes big endian`, desc: "The id under which the writer schema is registered in the Schema Registry. The consumer fetches the schema by this id, once, and caches it." });
          }
          w.embed(d);
        });
      });
      const out = w.finish(mode === "single" ? "single-object messages" : "Kafka message values");
      out.root.meta = { type: mode === "single" ? "single-object encoding" : "Confluent wire format", value: `${records.length} message(s)` };
      return { ...out, datums: datums.map((d) => d.bytes), schema, info };
    }

    w.group("File header", { kind: "header", type: "header", desc: "Magic bytes, a metadata map holding the full writer schema as JSON, and a random 16-byte sync marker." }, () => {
      w.leaf("Magic", MAGIC, { kind: "magic", type: "4 bytes ASCII", value: "'Obj\\x01'", rule: "0x4F 0x62 0x6A 0x01 (\"Obj\\x01\")", desc: "The four bytes 'O' 'b' 'j' 0x01 at the very start of the file identify it as an Avro Object Container File, version 1." });
      const meta = [["avro.schema", JSON.stringify(schemaJson)], ["avro.codec", codec], ...extraMeta];
      w.group("Metadata", { kind: "meta", type: "map<bytes>", value: meta.map(([k]) => k.replace(/^avro\./, "")).join(", "), desc: "File metadata is an Avro map<bytes>. avro.schema holds the writer schema in plain JSON, which is why you can read it with xxd or less." }, () => {
        longLeaf(w, "entry count", meta.length, { kind: "count", type: "long (zigzag varint)", desc: `Map block with ${meta.length} entries.` });
        for (const [k, v] of meta) {
          w.group(k, { kind: k === "avro.schema" ? "schema" : "meta", type: "map entry", value: k === "avro.schema" ? "writer schema (JSON)" : JSON.stringify(v) }, () => {
            const kb = DF.utf8(k);
            const vb = DF.utf8(v);
            longLeaf(w, "key length", kb.length, { kind: "length", type: "long (zigzag varint)" });
            w.leaf("key", kb, { kind: "meta", type: "UTF-8", value: JSON.stringify(k) });
            longLeaf(w, "value length", vb.length, { kind: "length", type: "long (zigzag varint)" });
            w.leaf("value", vb, {
              kind: k === "avro.schema" ? "schema" : "meta", type: k === "avro.schema" ? "JSON text" : "UTF-8", value: DF.shorten(v, 60),
              desc: k === "avro.schema" ? `The full writer schema travels with the data: ${vb.length} bytes, paid once per file and amortized over every record in it.` : k === "avro.codec" ? `Block compression codec. "null" means uncompressed; "deflate", "snappy" and "zstandard" compress each data block independently.` : "Application metadata. Avro carries it verbatim and attaches no meaning to it.",
            });
          });
        }
        longLeaf(w, "end of map", 0, { kind: "count", type: "long (zigzag varint)", desc: "A block count of 0 terminates the map." });
      });
      w.leaf("Sync marker", sync, { kind: "sync", type: "16 bytes", value: "random", rule: DF.hex(sync), desc: "16 random bytes chosen per file. The same marker follows every data block, so a reader can jump into the middle of a file, scan for the marker and start decoding there. That is what makes Avro files splittable." });
    });

    let blockNo = 0;
    const chunks = [];
    if (blockBytes > 0) {
      let cur = [];
      let size = 0;
      for (const d of datums) {
        cur.push(d);
        size += d.bytes.length;
        if (size >= blockBytes) {
          chunks.push(cur);
          cur = [];
          size = 0;
        }
      }
      if (cur.length) chunks.push(cur);
    } else {
      for (let start = 0; start < datums.length; start += blockSize) chunks.push(datums.slice(start, start + blockSize));
    }
    const blockFirst = [];
    for (const chunk of chunks) {
      if (!chunk.length) break;
      blockFirst.push(blockFirst.length ? blockFirst[blockFirst.length - 1] + chunks[blockFirst.length - 1].length : 0);
      const body = new DF.ByteWriter();
      w.open(`Data block ${blockNo}`, { kind: "block", type: "data block", value: `${chunk.length} record(s)`, desc: "Record count, byte size, the records, and the file's sync marker again." });
      chunk.forEach((d) => body.embed(d));
      const plain = body.finish();
      let payload = plain.bytes;
      if (codec === "deflate") payload = await DF.compress(plain.bytes, "deflate-raw");
      longLeaf(w, "Obj count", chunk.length, { kind: "count", type: "long (zigzag varint)", desc: "Number of records in this block." });
      longLeaf(w, "Block size", payload.length, { kind: "length", type: "long (zigzag varint)", desc: `Byte size of the (possibly compressed) records that follow. ${codec === "deflate" ? `Uncompressed they are ${plain.bytes.length} bytes.` : ""} A reader that does not care about this block can skip it in one jump.` });
      if (codec === "deflate") {
        w.leaf("Objects (deflate)", payload, { kind: "compressed", type: "raw deflate (RFC 1951)", value: `${plain.bytes.length} B to ${payload.length} B`, desc: "The whole block is compressed as one unit. Compression works across records, which is why many small records compress far better in a container than one by one." });
      } else {
        let at = w.pos;
        for (const d of chunk) {
          recordSpans.push({ start: at, end: at + d.bytes.length });
          at += d.bytes.length;
        }
        w.group("Objects", { kind: "objects", type: "binary, serialized", value: `${chunk.length} record(s), ${plain.bytes.length} B`, desc: "The records, back to back, with no separators. The sync marker below is the only boundary." }, () => w.embed(plain));
      }
      w.leaf("Sync", sync, { kind: "sync", type: "16 bytes", value: "matches header", rule: DF.hex(sync), desc: "Identical to the header's sync marker. A reader resynchronising after a seek looks for exactly these 16 bytes." });
      w.close();
      blockNo++;
    }
    const out = w.finish("Avro Container File");
    out.root.meta = { type: "Object Container File", value: `${records.length} record(s), ${blockNo} block(s)` };
    return { ...out, datums: datums.map((d) => d.bytes), schema, info, recordSpans: codec === "null" ? recordSpans : null, sync, codec, blockFirst };
  }

  function sameValue(a, b) {
    return DF.toJSON(a, 0) === DF.toJSON(b, 0);
  }

  /** Decode records back to back from an arbitrary offset, with no framing to find boundaries. */
  function readRawFrom(bytes, offset, schema, spans) {
    const truth = new Map(spans.map((r, i) => [r.start, { i, end: r.end }]));
    const attempts = [];
    let pos = offset;
    while (pos < bytes.length) {
      const r = new Reader(bytes, pos);
      try {
        const value = dec(r, schema);
        const t = truth.get(pos);
        const aligned = !!t && t.end === r.pos;
        attempts.push({ start: pos, end: r.pos, kind: aligned ? "ok" : "silent", value, record: aligned ? t.i : null });
        if (r.pos === pos) break;
        pos = r.pos;
      } catch (e) {
        attempts.push({ start: pos, end: Math.min(bytes.length, pos + 1), kind: "error", note: e.message });
        break;
      }
    }
    return attempts;
  }

  function indexOf(bytes, pattern, from) {
    outer: for (let i = Math.max(0, from); i <= bytes.length - pattern.length; i++) {
      for (let j = 0; j < pattern.length; j++) if (bytes[i + j] !== pattern[j]) continue outer;
      return i;
    }
    return -1;
  }

  /**
   * What a split reader does with an Object Container File: it already read the
   * header (schema and sync marker), seeks to offset, scans forward for the
   * next sync marker and decodes whole blocks from there.
   */
  async function readOcfFrom(built, offset) {
    const { bytes, sync, schema, codec, datums, blockFirst } = built;
    const headerEnd = built.root.children[0].end;
    const from = Math.max(offset, headerEnd - 16);
    const syncAt = indexOf(bytes, sync, from);
    const result = { offset, scanStart: offset, syncAt, blocks: [] };
    if (syncAt < 0 || syncAt + 16 >= bytes.length) {
      result.syncAt = syncAt;
      result.scanEnd = bytes.length;
      return result;
    }
    result.scanEnd = syncAt;
    const blockStarts = built.root.children.slice(1).map((n) => n.start);
    let pos = syncAt + 16;
    while (pos < bytes.length) {
      const r = new Reader(bytes, pos);
      const count = Number(r.long());
      const size = Number(r.long());
      const dataStart = r.pos;
      const raw = bytes.subarray(dataStart, dataStart + size);
      const data = codec === "deflate" ? await DF.decompress(raw, "deflate-raw") : raw;
      const blockNo = blockStarts.indexOf(pos);
      const first = blockNo >= 0 ? blockFirst[blockNo] : 0;
      const dr = new Reader(data);
      const recs = [];
      for (let k = 0; k < count; k++) {
        const s0 = dr.pos;
        const value = dec(dr, schema);
        const expected = dec(new Reader(datums[first + k]), schema);
        recs.push({
          start: codec === "deflate" ? null : dataStart + s0,
          end: codec === "deflate" ? null : dataStart + dr.pos,
          value, record: first + k, ok: blockNo >= 0 && sameValue(value, expected),
        });
      }
      const syncOk = indexOf(bytes, sync, dataStart + size) === dataStart + size;
      result.blocks.push({ start: pos, dataStart, end: dataStart + size + 16, count, size, records: recs, syncOk, blockNo });
      pos = dataStart + size + 16;
    }
    return result;
  }

  Object.assign(AV, { parseSchema, build, resolveAndRead, readPositionally, canonical, Reader, dec, readRawFrom, readOcfFrom });
})(globalThis);
