"use strict";

/**
 * Protobuf (proto3 subset): .proto parser, annotated encoder, schemaless wire
 * walker and a reader-schema decoder that reports what happened to each field.
 */
(function (g) {
  const DF = g.DF;
  const PB = (DF.PB = {});

  const WT = { VARINT: 0, I64: 1, LEN: 2, SGROUP: 3, EGROUP: 4, I32: 5 };
  const WT_NAME = { 0: "VARINT", 1: "I64", 2: "LEN", 3: "SGROUP", 4: "EGROUP", 5: "I32" };

  const SCALARS = {
    int32: WT.VARINT, int64: WT.VARINT, uint32: WT.VARINT, uint64: WT.VARINT,
    sint32: WT.VARINT, sint64: WT.VARINT, bool: WT.VARINT,
    fixed32: WT.I32, sfixed32: WT.I32, float: WT.I32,
    fixed64: WT.I64, sfixed64: WT.I64, double: WT.I64,
    string: WT.LEN, bytes: WT.LEN,
  };

  function tokenize(src) {
    const clean = src.replace(/\/\*[\s\S]*?\*\//g, (m) => m.replace(/[^\n]/g, " ")).replace(/\/\/[^\n]*/g, "");
    const re = /\s*(?:([A-Za-z_][\w.]*)|(-?\d+(?:\.\d+)?)|("(?:[^"\\]|\\.)*"|'(?:[^'\\]|\\.)*')|([{}=;\[\]<>,()]))/y;
    const tokens = [];
    let line = 1;
    let pos = 0;
    while (pos < clean.length) {
      re.lastIndex = pos;
      const m = re.exec(clean);
      if (!m || m[0].trim() === "") {
        if (/^\s*$/.test(clean.slice(pos))) break;
        const bad = clean.slice(pos).trimStart()[0];
        throw new Error(`line ${line}: unexpected character '${bad}'`);
      }
      line += (m[0].match(/\n/g) || []).length;
      tokens.push({ v: m[1] || m[2] || m[3] || m[4], kind: m[1] ? "id" : m[2] ? "num" : m[3] ? "str" : "sym", line });
      pos = re.lastIndex;
    }
    return tokens;
  }

  /** Parse a proto3 subset: messages (nested allowed), enums, repeated/optional, field options. */
  function parseProto(src) {
    const t = tokenize(src);
    let i = 0;
    const peek = () => t[i];
    const next = () => {
      if (i >= t.length) throw new Error("unexpected end of schema");
      return t[i++];
    };
    const expect = (v) => {
      const tok = next();
      if (tok.v !== v) throw new Error(`line ${tok.line}: expected '${v}' but found '${tok.v}'`);
      return tok;
    };
    const schema = { messages: {}, enums: {}, order: [] };

    function skipStatement() {
      while (i < t.length && next().v !== ";");
    }

    function parseEnum() {
      const name = next().v;
      expect("{");
      const values = [];
      while (peek() && peek().v !== "}") {
        const tok = next();
        if (tok.v === "option" || tok.v === "reserved") {
          skipStatement();
          continue;
        }
        expect("=");
        const num = Number(next().v);
        if (peek().v === "[") while (next().v !== "]");
        expect(";");
        values.push({ name: tok.v, number: num });
      }
      expect("}");
      if (!values.length) throw new Error(`enum ${name} has no values`);
      schema.enums[name] = { name, values };
    }

    function parseMessage() {
      const nameTok = next();
      const msg = { name: nameTok.v, fields: [] };
      expect("{");
      while (peek() && peek().v !== "}") {
        const tok = peek();
        if (tok.v === "message") {
          next();
          parseMessage();
          continue;
        }
        if (tok.v === "enum") {
          next();
          parseEnum();
          continue;
        }
        if (tok.v === "reserved" || tok.v === "option" || tok.v === "extensions") {
          skipStatement();
          continue;
        }
        if (tok.v === ";") {
          next();
          continue;
        }
        let label = "";
        if (tok.v === "repeated" || tok.v === "optional" || tok.v === "required") label = next().v;
        const typeTok = next();
        if (typeTok.kind !== "id") throw new Error(`line ${typeTok.line}: expected a type but found '${typeTok.v}'`);
        if (typeTok.v === "map" || typeTok.v === "oneof") throw new Error(`line ${typeTok.line}: '${typeTok.v}' is not supported by this explorer`);
        const fname = next().v;
        expect("=");
        const numTok = next();
        const number = Number(numTok.v);
        if (!Number.isInteger(number) || number < 1 || number > 536870911) throw new Error(`line ${numTok.line}: invalid field number '${numTok.v}'`);
        let packed;
        if (peek() && peek().v === "[") {
          next();
          while (peek() && peek().v !== "]") {
            const o = next();
            if (o.v === "packed") {
              expect("=");
              packed = next().v === "true";
            }
          }
          expect("]");
        }
        expect(";");
        if (msg.fields.some((f) => f.number === number)) throw new Error(`line ${numTok.line}: field number ${number} is used twice in ${msg.name}`);
        msg.fields.push({ name: fname, type: typeTok.v.split(".").pop(), number, label, packed });
      }
      expect("}");
      schema.messages[msg.name] = msg;
      schema.order.push(msg.name);
    }

    while (i < t.length) {
      const tok = next();
      if (tok.v === "syntax" || tok.v === "package" || tok.v === "import" || tok.v === "option") skipStatement();
      else if (tok.v === "message") parseMessage();
      else if (tok.v === "enum") parseEnum();
      else if (tok.v === ";") continue;
      else throw new Error(`line ${tok.line}: unexpected '${tok.v}'`);
    }
    if (!schema.order.length) throw new Error("schema contains no message");
    for (const m of Object.values(schema.messages)) {
      for (const f of m.fields) {
        if (!(f.type in SCALARS) && !schema.messages[f.type] && !schema.enums[f.type]) {
          throw new Error(`unknown type '${f.type}' for field ${m.name}.${f.name}`);
        }
      }
    }
    return schema;
  }

  function kindOf(schema, type) {
    if (type in SCALARS) return "scalar";
    if (schema.enums[type]) return "enum";
    return "message";
  }

  function wireTypeOf(schema, type) {
    if (type in SCALARS) return SCALARS[type];
    if (schema.enums[type]) return WT.VARINT;
    return WT.LEN;
  }

  function isPackable(schema, f) {
    return f.label === "repeated" && wireTypeOf(schema, f.type) !== WT.LEN && f.packed !== false;
  }

  function defaultFor(schema, f) {
    if (f.label === "repeated") return [];
    const k = kindOf(schema, f.type);
    if (k === "message") return null;
    if (k === "enum") return schema.enums[f.type].values[0].name;
    if (f.type === "string") return "";
    if (f.type === "bytes") return "";
    if (f.type === "bool") return false;
    return 0;
  }

  function isDefault(schema, f, v) {
    const k = kindOf(schema, f.type);
    if (k === "message") return false;
    if (k === "enum") return enumNumber(schema, f.type, v) === 0;
    if (f.type === "string" || f.type === "bytes") return v === "" || (Array.isArray(v) && v.length === 0);
    if (f.type === "bool") return v === false;
    return Number(v) === 0;
  }

  function enumNumber(schema, type, v) {
    const e = schema.enums[type];
    if (typeof v === "number") return v;
    const hit = e.values.find((x) => x.name === v);
    if (!hit) throw new Error(`'${v}' is not a value of enum ${type}`);
    return hit.number;
  }

  function serializeProto(schema) {
    const lines = ['syntax = "proto3";', ""];
    for (const name of schema.order) {
      const m = schema.messages[name];
      lines.push(`message ${m.name} {`);
      for (const f of m.fields) {
        const label = f.label ? f.label + " " : "";
        const opt = f.packed === false ? " [packed = false]" : "";
        lines.push(`  ${label}${f.type} ${f.name} = ${f.number}${opt};`);
      }
      lines.push("}", "");
    }
    for (const e of Object.values(schema.enums)) {
      lines.push(`enum ${e.name} {`);
      for (const v of e.values) lines.push(`  ${v.name} = ${v.number};`);
      lines.push("}", "");
    }
    return lines.join("\n").trimEnd() + "\n";
  }

  function cloneSchema(s) {
    return JSON.parse(JSON.stringify(s));
  }

  function tagMeta(f, wt) {
    const tag = (f.number << 3) | wt;
    return {
      kind: "tag",
      type: "varint tag",
      rule: `(field_number << 3) | wire_type\n(${f.number} << 3) | ${wt} = ${tag} = ${DF.hexAddr(tag)}`,
      value: `field ${f.number}, ${WT_NAME[wt]}`,
      varint: true,
      tag: true,
      desc: `The tag is the only metadata on the wire. It says "field ${f.number}" and "wire type ${wt} (${WT_NAME[wt]})". The name "${f.name}" never leaves the .proto file.`,
    };
  }

  const I32 = [-(2n ** 31n), 2n ** 31n - 1n];
  const U32 = [0n, 2n ** 32n - 1n];
  const I64 = [-(2n ** 63n), 2n ** 63n - 1n];
  const U64 = [0n, 2n ** 64n - 1n];
  const RANGES = { int32: I32, sint32: I32, sfixed32: I32, uint32: U32, fixed32: U32, int64: I64, sint64: I64, sfixed64: I64, uint64: U64, fixed64: U64 };

  function checkRange(f, t, v) {
    const n = BigInt(v);
    const [lo, hi] = RANGES[t];
    if (n < lo || n > hi) throw new Error(`${f.name}: ${v} does not fit ${t} (${lo} to ${hi})`);
    return n;
  }

  function encodeScalar(w, schema, f, v, label) {
    const t = f.type;
    if (schema.enums[t]) {
      const n = enumNumber(schema, t, v);
      const name = schema.enums[t].values.find((x) => x.number === n)?.name ?? n;
      return w.leaf(label, DF.varint(BigInt.asUintN(64, BigInt(n))), {
        kind: "value", type: `enum ${t} (varint)`, rule: `${name} = ${n}\nvarint(${n})`, value: String(name), varint: true,
        desc: "Enums travel as their number. The symbol name is only in the .proto file.",
      });
    }
    switch (t) {
      case "int32":
      case "int64": {
        const n = checkRange(f, t, v);
        const bytes = DF.varint(BigInt.asUintN(64, n));
        return w.leaf(label, bytes, {
          kind: "value", type: `${t} (varint)`, varint: true, value: n.toString(),
          rule: n < 0n ? `negative ${t}: sign-extended to 64 bit two's complement\nvarint(${BigInt.asUintN(64, n)}) = ${bytes.length} bytes` : `varint(${n})`,
          desc: n < 0n
            ? `Negative ${t} values are sign-extended to 64 bits, so they always take 10 bytes. Use sint32/sint64 (zigzag) if negatives are common.`
            : "Plain varint: 7 payload bits per byte, the high bit says whether another byte follows. Small numbers take few bytes.",
        });
      }
      case "uint32":
      case "uint64": {
        const n = checkRange(f, t, v);
        return w.leaf(label, DF.varint(n), {
          kind: "value", type: `${t} (varint)`, varint: true, value: n.toString(), rule: `varint(${n})`,
          desc: "Plain varint: 7 payload bits per byte, the high bit says whether another byte follows.",
        });
      }
      case "sint32":
      case "sint64": {
        const n = checkRange(f, t, v);
        const z = DF.zigzag(n);
        return w.leaf(label, DF.varint(z), {
          kind: "value", type: `${t} (zigzag varint)`, varint: true, zigzag: true, value: n.toString(),
          rule: `zigzag(${n}) = (n << 1) ^ (n >> 63) = ${z}\nvarint(${z})`,
          desc: "ZigZag maps signed to unsigned so small negatives stay small: 0, -1, 1, -2, 2 become 0, 1, 2, 3, 4.",
        });
      }
      case "bool":
        return w.leaf(label, [v ? 1 : 0], { kind: "value", type: "bool (varint)", varint: true, value: String(!!v), rule: `varint(${v ? 1 : 0})`, desc: "A bool is a varint 0 or 1." });
      case "float":
        return w.leaf(label, DF.f32(v), { kind: "value", type: "float (I32)", value: String(new DataView(DF.f32(v).buffer).getFloat32(0, true)), rule: "IEEE 754 binary32, little endian, always 4 bytes", desc: "Fixed width. Nothing on the wire says this is a float rather than a fixed32 or sfixed32." });
      case "double":
        return w.leaf(label, DF.f64(v), { kind: "value", type: "double (I64)", value: String(v), rule: "IEEE 754 binary64, little endian, always 8 bytes", desc: "Fixed width. Nothing on the wire says this is a double rather than a fixed64 or sfixed64." });
      case "fixed32":
      case "sfixed32":
        checkRange(f, t, v);
        return w.leaf(label, DF.le32(v), { kind: "value", type: `${t} (I32)`, value: String(v), rule: "4 bytes, little endian", desc: "Fixed width integers are cheaper than varints for large values (hashes, ids)." });
      case "fixed64":
      case "sfixed64":
        checkRange(f, t, v);
        return w.leaf(label, DF.le64(v), { kind: "value", type: `${t} (I64)`, value: String(v), rule: "8 bytes, little endian", desc: "Fixed width integers are cheaper than varints for large values (hashes, ids)." });
      case "string":
      case "bytes": {
        const payload = Array.isArray(v) ? Uint8Array.from(v) : DF.utf8(String(v));
        w.leaf(`${label} length`, DF.varint(payload.length), {
          kind: "length", type: "varint length", varint: true, value: String(payload.length), rule: `varint(${payload.length})`,
          desc: "LEN fields carry their byte length, so any reader can skip them without knowing what they contain.",
        });
        return w.leaf(label, payload, {
          kind: "value", type: t === "string" ? "UTF-8 bytes" : "raw bytes", value: t === "string" ? JSON.stringify(String(v)) : DF.hex(payload, 16),
          rule: `${payload.length} bytes, no terminator`,
          desc: t === "string" ? "Strings are length-prefixed UTF-8." : "Raw bytes, length-prefixed.",
        });
      }
    }
    throw new Error(`unsupported type ${t}`);
  }

  function encodeMessage(schema, msgName, value, omitted, path = "") {
    const msg = schema.messages[msgName];
    const w = new DF.ByteWriter();
    if (value === null || typeof value !== "object" || Array.isArray(value)) throw new Error(`${path || msgName}: expected a JSON object`);
    const known = new Set(msg.fields.map((f) => f.name));
    for (const key of Object.keys(value)) if (!known.has(key)) throw new Error(`${path || msgName}: '${key}' is not a field of ${msgName}`);
    const fields = [...msg.fields].sort((a, b) => a.number - b.number);
    for (const f of fields) {
      const v = value[f.name];
      const fpath = path ? `${path}.${f.name}` : f.name;
      if (v === undefined || v === null) {
        omitted.push({ path: fpath, reason: "not set" });
        continue;
      }
      if (f.label === "repeated") {
        if (!Array.isArray(v)) throw new Error(`${fpath}: repeated field needs a JSON array`);
        if (!v.length) {
          omitted.push({ path: fpath, reason: "empty list" });
          continue;
        }
        if (isPackable(schema, f)) {
          const inner = new DF.ByteWriter();
          v.forEach((x, idx) => encodeScalar(inner, schema, f, x, `${f.name}[${idx}]`));
          const sub = inner.finish();
          w.group(`${f.name}`, { kind: "field", type: `repeated ${f.type}, packed`, value: `${v.length} elements`, desc: "Packed repeated scalars: one tag, one length, then the values back to back. No per-element tag." }, () => {
            w.leaf(`tag`, DF.varint((f.number << 3) | WT.LEN), tagMeta(f, WT.LEN));
            w.leaf(`length`, DF.varint(sub.bytes.length), { kind: "length", type: "varint length", varint: true, value: String(sub.bytes.length), rule: `varint(${sub.bytes.length})`, desc: "Byte length of all packed elements together." });
            w.embed(sub);
          });
        } else {
          v.forEach((x, idx) => encodeField(w, schema, f, x, `${f.name}[${idx}]`, omitted, `${fpath}[${idx}]`, true));
        }
        continue;
      }
      if (f.label !== "optional" && isDefault(schema, f, v)) {
        omitted.push({ path: fpath, reason: `default value ${JSON.stringify(v)}` });
        continue;
      }
      encodeField(w, schema, f, v, f.name, omitted, fpath, false);
    }
    return w.finish(msgName);
  }

  function encodeField(w, schema, f, v, label, omitted, fpath, repeated) {
    const wt = wireTypeOf(schema, f.type);
    const k = kindOf(schema, f.type);
    w.group(label, { kind: "field", type: `${repeated ? "repeated " : ""}${f.type} = ${f.number}`, value: k === "message" ? f.type : DF.shorten(DF.show(v), 30), desc: repeated ? "Non-packed repeated fields repeat the tag for every element." : undefined }, () => {
      w.leaf("tag", DF.varint((f.number << 3) | wt), tagMeta(f, wt));
      if (k === "message") {
        const sub = encodeMessage(schema, f.type, v, omitted, fpath);
        w.leaf("length", DF.varint(sub.bytes.length), { kind: "length", type: "varint length", varint: true, value: String(sub.bytes.length), rule: `varint(${sub.bytes.length})`, desc: "An embedded message is just a LEN field. Its bytes are another protobuf message." });
        w.group(f.type, { kind: "nested", type: `message ${f.type}`, value: `${sub.bytes.length} bytes`, desc: "Nested message: the same tag/value structure again, one level down." }, () => w.embed(sub));
      } else {
        encodeScalar(w, schema, f, v, "value");
      }
    });
  }

  function encode(schema, root, value) {
    const omitted = [];
    const result = encodeMessage(schema, root, value, omitted);
    result.root.label = root;
    result.root.meta = { type: `message ${root}`, value: `${result.bytes.length} bytes`, desc: "A protobuf message is nothing but a sequence of (tag, value) pairs. No header, no field names, no length of the whole message." };
    result.omitted = omitted;
    return result;
  }

  /** Read one (tag, value) record starting at pos. Throws on anything malformed. */
  function readField(bytes, pos, end = bytes.length) {
    const tagStart = pos;
    const t = DF.readVarint(bytes, pos);
    pos = t.pos;
    const number = Number(t.value >> 3n);
    const wt = Number(t.value & 7n);
    if (number < 1) throw new Error(`invalid field number 0 at ${DF.hexAddr(tagStart)}`);
    const rec = { number, wt, tagStart, valueStart: pos };
    if (wt === WT.VARINT) {
      const v = DF.readVarint(bytes, pos);
      if (v.pos > end) throw new Error("VARINT runs past the end");
      rec.raw = v.value;
      pos = v.pos;
    } else if (wt === WT.I32) {
      if (pos + 4 > end) throw new Error("I32 runs past the end");
      rec.raw = bytes.subarray(pos, pos + 4);
      pos += 4;
    } else if (wt === WT.I64) {
      if (pos + 8 > end) throw new Error("I64 runs past the end");
      rec.raw = bytes.subarray(pos, pos + 8);
      pos += 8;
    } else if (wt === WT.LEN) {
      const l = DF.readVarint(bytes, pos);
      rec.lenStart = pos;
      pos = l.pos;
      const n = Number(l.value);
      if (pos + n > end) throw new Error(`LEN field ${number} claims ${n} bytes, only ${end - pos} left`);
      rec.payloadStart = pos;
      rec.raw = bytes.subarray(pos, pos + n);
      pos += n;
    } else {
      throw new Error(`wire type ${wt} at ${DF.hexAddr(tagStart)} is not supported (groups are deprecated)`);
    }
    rec.end = pos;
    return rec;
  }

  /** Split bytes into (tag, value) records without any schema. */
  function walkWire(bytes, start = 0, end = bytes.length) {
    const out = [];
    let pos = start;
    while (pos < end) {
      const rec = readField(bytes, pos, end);
      out.push(rec);
      pos = rec.end;
    }
    return out;
  }

  const MAX_FIELD_NUMBER = 536870911;

  /**
   * Parse without any schema, the way protoc --decode_raw does: every
   * non-empty LEN payload that parses as a message is shown as one.
   */
  function decodeRaw(bytes) {
    return parseRaw(bytes, 0, 0).fields;
  }

  function parseRaw(bytes, pos, group) {
    const fields = [];
    while (pos < bytes.length) {
      const tagStart = pos;
      const t = DF.readVarint(bytes, pos);
      if (t.value > 0xffffffffn) throw new Error(`tag at ${DF.hexAddr(tagStart)} is wider than 32 bits`);
      const number = Number(t.value >> 3n);
      const wt = Number(t.value & 7n);
      if (number < 1 || number > MAX_FIELD_NUMBER) throw new Error(`invalid field number ${number} at ${DF.hexAddr(tagStart)}`);
      pos = t.pos;
      if (wt === WT.EGROUP) {
        if (number !== group) throw new Error(`end of group ${number} at ${DF.hexAddr(tagStart)} without a matching start`);
        return { fields, pos };
      }
      const f = { number, wt, valueStart: pos };
      if (wt === WT.VARINT) {
        const v = DF.readVarint(bytes, pos);
        f.raw = v.value;
        pos = v.pos;
      } else if (wt === WT.I32 || wt === WT.I64) {
        const n = wt === WT.I32 ? 4 : 8;
        if (pos + n > bytes.length) throw new Error(`${WT_NAME[wt]} at ${DF.hexAddr(tagStart)} runs past the end`);
        f.raw = bytes.subarray(pos, pos + n);
        pos += n;
      } else if (wt === WT.LEN) {
        const l = DF.readVarint(bytes, pos);
        const n = Number(l.value);
        if (l.pos + n > bytes.length) throw new Error(`LEN field ${number} at ${DF.hexAddr(tagStart)} claims ${n} bytes, only ${bytes.length - l.pos} left`);
        f.raw = bytes.subarray(l.pos, l.pos + n);
        pos = l.pos + n;
        if (n > 0) {
          try {
            f.children = parseRaw(f.raw, 0, 0).fields;
          } catch {
            f.children = null;
          }
        }
      } else if (wt === WT.SGROUP) {
        const sub = parseRaw(bytes, pos, number);
        f.children = sub.fields;
        pos = sub.pos;
      } else {
        throw new Error(`invalid wire type ${wt} at ${DF.hexAddr(tagStart)}`);
      }
      f.bytes = bytes.subarray(f.valueStart, pos);
      fields.push(f);
    }
    if (group) throw new Error(`group ${group} is never closed`);
    return { fields, pos };
  }

  function cEscape(bytes) {
    const named = { 0x0a: "\\n", 0x0d: "\\r", 0x09: "\\t", 0x22: '\\"', 0x27: "\\'", 0x5c: "\\\\" };
    let s = "";
    for (const b of bytes) {
      if (named[b]) s += named[b];
      else if (b < 0x20 || b >= 0x7f) s += "\\" + b.toString(8).padStart(3, "0");
      else s += String.fromCharCode(b);
    }
    return s;
  }

  /** Text exactly as protoc --decode_raw prints it. */
  function formatRaw(fields, indent = "") {
    return fields.map((f) => {
      if (f.children) return `${indent}${f.number} {\n${formatRaw(f.children, indent + "  ")}${indent}}\n`;
      let v;
      if (f.wt === WT.VARINT) v = String(f.raw);
      else if (f.wt === WT.I32) v = "0x" + DF.view(f.raw, 0).getUint32(0, true).toString(16).padStart(8, "0");
      else if (f.wt === WT.I64) v = "0x" + DF.view(f.raw, 0).getBigUint64(0, true).toString(16).padStart(16, "0");
      else v = `"${cEscape(f.raw)}"`;
      return `${indent}${f.number}: ${v}\n`;
    }).join("");
  }

  function shortFloat(x) {
    return Number.isFinite(x) ? String(Number(x.toPrecision(7))) : String(x);
  }

  function readVarints(bytes) {
    const out = [];
    let pos = 0;
    while (pos < bytes.length) {
      const v = DF.readVarint(bytes, pos);
      out.push(v.value);
      pos = v.pos;
    }
    return out;
  }

  function listPreview(values, max = 4) {
    return values.slice(0, max).join(", ") + (values.length > max ? `, ... (${values.length})` : "");
  }

  /**
   * Every type a schema-less reader could assign to one raw field, grouped
   * by the value it would produce: [{ types: [...], value }].
   */
  function interpretations(f) {
    const out = [];
    const add = (type, value) => {
      const hit = out.find((o) => o.value === value);
      if (hit) hit.types.push(type);
      else out.push({ types: [type], value });
    };
    if (f.wt === WT.VARINT) {
      const v = f.raw;
      const signed = BigInt.asIntN(64, v);
      add("uint64", String(v));
      add("int64", String(signed));
      if (v < 1n << 32n) add("uint32", String(v));
      if (signed >= -(1n << 31n) && signed < 1n << 31n) {
        add("int32", String(signed));
        add("enum", String(signed));
      }
      add("sint64", String(DF.unzigzag(v)));
      if (v < 1n << 32n) add("sint32", String(DF.unzigzag(v)));
      if (v <= 1n) add("bool", v === 1n ? "true" : "false");
    } else if (f.wt === WT.I32) {
      const dv = DF.view(f.raw, 0);
      add("float", shortFloat(dv.getFloat32(0, true)));
      add("fixed32", String(dv.getUint32(0, true)));
      add("sfixed32", String(dv.getInt32(0, true)));
    } else if (f.wt === WT.I64) {
      const dv = DF.view(f.raw, 0);
      add("double", shortFloat(dv.getFloat64(0, true)));
      add("fixed64", String(dv.getBigUint64(0, true)));
      add("sfixed64", String(dv.getBigInt64(0, true)));
    } else if (f.wt === WT.SGROUP) {
      add("group (deprecated)", `${f.children.length} field(s)`);
    } else if (f.raw.length === 0) {
      for (const t of ["string", "bytes", "message", "packed repeated"]) add(t, "empty");
    } else {
      if (f.children) add("message", `fields ${listPreview(f.children.map((c) => "#" + c.number))}`);
      try {
        const s = new TextDecoder("utf-8", { fatal: true }).decode(f.raw);
        add("string", JSON.stringify(s.length > 40 ? s.slice(0, 40) + "..." : s));
      } catch {}
      add("bytes", DF.hex(f.raw, 12));
      try {
        add("packed varints", listPreview(readVarints(f.raw).map(String)));
      } catch {}
      if (f.raw.length % 4 === 0) {
        const dv = DF.view(f.raw, 0);
        const floats = Array.from({ length: f.raw.length / 4 }, (_, i) => shortFloat(dv.getFloat32(i * 4, true)));
        add("packed floats", listPreview(floats));
      }
    }
    return out;
  }

  function decodeScalarAs(schema, type, rec) {
    if (schema.enums[type]) {
      const n = Number(BigInt.asIntN(32, rec.raw));
      return schema.enums[type].values.find((v) => v.number === n)?.name ?? n;
    }
    const dv = rec.raw instanceof Uint8Array ? DF.view(rec.raw, 0) : null;
    switch (type) {
      case "int32": return Number(BigInt.asIntN(32, rec.raw));
      case "int64": return BigInt.asIntN(64, rec.raw);
      case "uint32": return Number(BigInt.asUintN(32, rec.raw));
      case "uint64": return rec.raw;
      case "sint32": return Number(DF.unzigzag(rec.raw));
      case "sint64": return DF.unzigzag(rec.raw);
      case "bool": return rec.raw !== 0n;
      case "float": return dv.getFloat32(0, true);
      case "double": return dv.getFloat64(0, true);
      case "fixed32": return dv.getUint32(0, true);
      case "sfixed32": return dv.getInt32(0, true);
      case "fixed64": return dv.getBigUint64(0, true);
      case "sfixed64": return dv.getBigInt64(0, true);
      case "string": return DF.decodeUtf8(rec.raw);
      case "bytes": return DF.hex(rec.raw);
    }
    throw new Error(`cannot decode ${type}`);
  }

  function readPacked(schema, type, payload) {
    const wt = wireTypeOf(schema, type);
    const out = [];
    let pos = 0;
    while (pos < payload.length) {
      if (wt === WT.VARINT) {
        const v = DF.readVarint(payload, pos);
        out.push(decodeScalarAs(schema, type, { raw: v.value }));
        pos = v.pos;
      } else {
        const n = wt === WT.I32 ? 4 : 8;
        out.push(decodeScalarAs(schema, type, { raw: payload.subarray(pos, pos + n) }));
        pos += n;
      }
    }
    return out;
  }

  /**
   * Decode with the reader's schema, the way generated code does it: match by
   * field number, skip unknown numbers, fill absent fields with defaults.
   * The writer schema is only consulted to explain what happened.
   */
  function decodeWithReader(bytes, reader, readerRoot, writer, writerRoot) {
    const report = [];
    const value = decodeMsg(bytes, reader, readerRoot, writer, writerRoot, "", report);
    return { value, report };
  }

  function decodeMsg(bytes, reader, rName, writer, wName, path, report) {
    const rmsg = reader.messages[rName];
    const wmsg = writer && wName ? writer.messages[wName] : null;
    const recs = walkWire(bytes);
    const out = {};
    const seen = new Set();
    const byNum = new Map(rmsg.fields.map((f) => [f.number, f]));
    for (const rec of recs) {
      const rf = byNum.get(rec.number);
      const wf = wmsg?.fields.find((f) => f.number === rec.number);
      const wdesc = wf ? `writer: ${wf.type} ${wf.name} = ${wf.number}` : `writer: field ${rec.number}`;
      if (!rf) {
        if (!report.some((r) => r.path === `${path}#${rec.number}`)) {
          report.push({ path: `${path}#${rec.number}`, field: wf ? `(${wf.name})` : `#${rec.number}`, number: rec.number, status: "skipped", note: `unknown field number ${rec.number}, ${rec.end - rec.tagStart} bytes skipped using the wire type. ${wf ? `The writer called it '${wf.name}'.` : ""}` });
        }
        continue;
      }
      const fpath = path ? `${path}.${rf.name}` : rf.name;
      const expected = wireTypeOf(reader, rf.type);
      const packedOk = rf.label === "repeated" && rec.wt === WT.LEN && expected !== WT.LEN;
      if (rec.wt !== expected && !packedOk) {
        report.push({ path: fpath, field: rf.name, number: rf.number, status: "mismatch", note: `wire type ${WT_NAME[rec.wt]} on the wire, reader expects ${WT_NAME[expected]}. Kept as unknown field, '${rf.name}' falls back to its default. ${wdesc}` });
        continue;
      }
      seen.add(rf.number);
      let v;
      const k = kindOf(reader, rf.type);
      if (k === "message") {
        const wsub = wf && writer.messages[wf.type] ? wf.type : null;
        try {
          v = decodeMsg(rec.raw, reader, rf.type, wsub ? writer : null, wsub, fpath, report);
        } catch (e) {
          report.push({ path: fpath, field: rf.name, number: rf.number, status: "error", note: `payload does not parse as ${rf.type}: ${e.message}` });
          continue;
        }
      } else if (packedOk) {
        v = readPacked(reader, rf.type, rec.raw);
      } else {
        try {
          v = decodeScalarAs(reader, rf.type, rec);
        } catch (e) {
          report.push({ path: fpath, field: rf.name, number: rf.number, status: "error", note: e.message });
          continue;
        }
      }
      if (rf.label === "repeated") out[rf.name] = (out[rf.name] || []).concat(Array.isArray(v) ? v : [v]);
      else out[rf.name] = v;
      if (!report.some((r) => r.path === fpath)) {
        let status = "ok";
        const notes = [];
        if (wf && wf.name !== rf.name) {
          status = "renamed";
          notes.push(`writer calls it '${wf.name}', matched by number ${rf.number} anyway`);
        }
        if (wf && wf.type !== rf.type) {
          status = "silent";
          notes.push(`writer wrote ${wf.type}, reader decodes the same bytes as ${rf.type}`);
        }
        if (!wf && writer) {
          notes.push("number not in writer schema");
        }
        report.push({ path: fpath, field: rf.name, number: rf.number, status, note: notes.join("; ") || `${rf.type} = ${rf.number}, same as writer` });
      }
    }
    for (const rf of rmsg.fields) {
      if (seen.has(rf.number)) continue;
      const fpath = path ? `${path}.${rf.name}` : rf.name;
      out[rf.name] = defaultFor(reader, rf);
      if (report.some((r) => r.path === fpath)) continue;
      const sameName = wmsg?.fields.find((f) => f.name === rf.name);
      const sameNum = wmsg?.fields.find((f) => f.number === rf.number);
      let status = "default";
      let note = "not on the wire, reader uses the proto3 default";
      if (sameName && sameName.number !== rf.number) {
        status = "silent";
        note = `writer sends '${rf.name}' as field ${sameName.number}, reader looks for ${rf.number}. No error, just a default value`;
      } else if (sameNum) {
        note = `writer omitted it (proto3 does not encode default values)`;
      } else if (wmsg) {
        note = "writer schema has no such field, reader uses the default";
      }
      report.push({ path: fpath, field: rf.name, number: rf.number, status, note });
    }
    return out;
  }

  /**
   * Encode many messages into one byte stream. framing "wrapper" is
   * message Xs { repeated X xs = 1; }, framing "delimited" is the
   * writeDelimitedTo convention: varint length, message, repeated.
   */
  function encodeMany(schema, root, values, framing) {
    const w = new DF.ByteWriter();
    const omitted = [];
    const records = [];
    const payloads = [];
    const field = { number: 1, name: `${root.toLowerCase()}s` };
    values.forEach((v, i) => {
      const sub = encodeMessage(schema, root, v, omitted, `[${i}]`);
      payloads.push(sub.bytes);
      const start = w.pos;
      w.group(`${root} ${i}`, { kind: "field", type: framing === "wrapper" ? `repeated ${root} ${field.name} = 1` : "length-delimited message", value: `${sub.bytes.length} B payload` }, () => {
        if (framing === "wrapper") w.leaf("tag", DF.varint((1 << 3) | WT.LEN), tagMeta(field, WT.LEN));
        w.leaf("length", DF.varint(sub.bytes.length), {
          kind: "length", type: "varint length", varint: true, value: String(sub.bytes.length), rule: `varint(${sub.bytes.length})`,
          desc: framing === "wrapper"
            ? "Every element of a repeated message field is an embedded message: tag, length, bytes."
            : "The delimited convention (writeDelimitedTo / parseDelimitedFrom): a varint length before each message. A library convention, not part of the wire format, and nothing in the bytes says it is being used.",
        });
        w.group(root, { kind: "nested", type: `message ${root}`, value: `${sub.bytes.length} bytes` }, () => w.embed(sub));
      });
      records.push({ start, end: w.pos });
    });
    const result = w.finish(framing === "wrapper" ? `${root}s message` : "delimited stream");
    result.root.meta = framing === "wrapper"
      ? { type: `message ${root}s { repeated ${root} ${field.name} = 1; }`, value: `${values.length} elements`, desc: "One regular message holding a list. Generated code parses it as a whole: the full object graph in memory, one thread, before you see the first element." }
      : { type: "length-delimited stream", value: `${values.length} messages`, desc: "No header, no schema, no markers. Just length, message, length, message." };
    const seen = new Set();
    result.omitted = omitted.map((o) => ({ ...o, path: o.path.replace(/^\[\d+\]\./, "") })).filter((o) => !seen.has(o.path) && seen.add(o.path));
    result.records = records;
    result.payloads = payloads;
    result.framing = framing;
    return result;
  }

  Object.assign(PB, { WT_NAME, parseProto, serializeProto, cloneSchema, encode, encodeMany, decodeRaw, formatRaw, interpretations, decodeWithReader });
})(globalThis);
