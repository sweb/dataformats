"use strict";

/**
 * Apache Arrow: columnar buffers (validity, offsets, values) and the IPC file
 * format that stores exactly those buffers, described by FlatBuffers metadata.
 * The FlatBuffers here are written front to back by a tiny builder, so every
 * vtable, table, vector and offset can be annotated.
 */
(function (g) {
  const DF = g.DF;
  const AR = (DF.AR = {});

  const TYPES = {
    int32: { width: 4, fb: "Int", bits: 32, signed: true, rule: "int32, little endian" },
    int64: { width: 8, fb: "Int", bits: 64, signed: true, rule: "int64, little endian" },
    float32: { width: 4, fb: "FloatingPoint", precision: 1, rule: "IEEE 754 binary32, little endian" },
    float64: { width: 8, fb: "FloatingPoint", precision: 2, rule: "IEEE 754 binary64, little endian" },
    utf8: { width: null, fb: "Utf8" },
    bool: { width: null, fb: "Bool" },
  };
  const TYPE_UNION = { Int: 2, FloatingPoint: 3, Utf8: 5, Bool: 6 };
  const HEADER_UNION = { Schema: 1, RecordBatch: 3 };
  const SIZE = { u8: 1, bool: 1, i16: 2, i32: 4, i64: 8, offset: 4 };

  function parseSchema(text) {
    const cols = [];
    for (const raw of text.split("\n")) {
      const line = raw.replace(/#.*/, "").trim();
      if (!line) continue;
      const m = line.match(/^([A-Za-z_]\w*)\s*:\s*(\w+)(\s+not\s+null)?$/i);
      if (!m) throw new Error(`cannot parse '${line}', expected 'name: type' or 'name: type not null'`);
      if (!TYPES[m[2]]) throw new Error(`unknown type '${m[2]}', supported: ${Object.keys(TYPES).join(", ")}`);
      cols.push({ name: m[1], type: m[2], nullable: !m[3] });
    }
    if (!cols.length) throw new Error("schema has no columns");
    return cols;
  }

  function scalarBytes(type, v) {
    switch (type) {
      case "int32": return DF.le32(v);
      case "int64": return DF.le64(v);
      case "float32": return DF.f32(v);
      case "float64": return DF.f64(v);
    }
    throw new Error(type);
  }

  function checkValue(col, v, i) {
    if (v === null || v === undefined) {
      if (!col.nullable) throw new Error(`row ${i}: ${col.name} is 'not null' but has no value`);
      return null;
    }
    const t = col.type;
    if ((t === "int32" || t === "int64") && !Number.isInteger(v)) throw new Error(`row ${i}: ${col.name} needs an integer`);
    if ((t === "float32" || t === "float64") && typeof v !== "number") throw new Error(`row ${i}: ${col.name} needs a number`);
    if (t === "utf8" && typeof v !== "string") throw new Error(`row ${i}: ${col.name} needs a string`);
    if (t === "bool" && typeof v !== "boolean") throw new Error(`row ${i}: ${col.name} needs true or false`);
    return v;
  }

  /**
   * The in-memory layout of one column: the list of buffers with the byte
   * range of every element, exactly as they will sit in the IPC body.
   */
  function columnBuffers(col, values) {
    const n = values.length;
    const nulls = values.filter((v) => v === null).length;
    const bufs = [];
    const bitmapBytes = Math.ceil(n / 8);
    if (nulls > 0) {
      const b = new Uint8Array(bitmapBytes);
      values.forEach((v, i) => {
        if (v !== null) b[i >> 3] |= 1 << (i & 7);
      });
      bufs.push({ role: "validity", bytes: b, elems: values.map((v, i) => ({ i, byte: i >> 3, bit: i & 7, set: v !== null })) });
    } else {
      bufs.push({ role: "validity", bytes: new Uint8Array(0), elems: [], omitted: true });
    }
    const t = TYPES[col.type];
    if (t.width) {
      const b = new Uint8Array(n * t.width);
      values.forEach((v, i) => {
        if (v !== null) b.set(scalarBytes(col.type, v), i * t.width);
      });
      bufs.push({ role: "values", bytes: b, width: t.width, elems: values.map((v, i) => ({ i, start: i * t.width, end: (i + 1) * t.width, value: v })) });
    } else if (col.type === "bool") {
      const b = new Uint8Array(bitmapBytes);
      values.forEach((v, i) => {
        if (v) b[i >> 3] |= 1 << (i & 7);
      });
      bufs.push({ role: "values", bytes: b, bitmap: true, elems: values.map((v, i) => ({ i, byte: i >> 3, bit: i & 7, value: v })) });
    } else {
      const offs = new Uint8Array((n + 1) * 4);
      const parts = [];
      let o = 0;
      const ov = [0];
      const dataElems = [];
      values.forEach((v, i) => {
        const s = v === null ? new Uint8Array(0) : DF.utf8(v);
        dataElems.push({ i, start: o, end: o + s.length, value: v });
        parts.push(s);
        o += s.length;
        ov.push(o);
      });
      ov.forEach((x, i) => offs.set(DF.le32(x), i * 4));
      bufs.push({ role: "offsets", bytes: offs, elems: ov.map((x, i) => ({ i, start: i * 4, end: i * 4 + 4, value: x })) });
      bufs.push({ role: "data", bytes: DF.concat(parts), elems: dataElems });
    }
    return { col, n, nulls, bufs };
  }

  function pad8(n) {
    return (8 - (n % 8)) % 8;
  }

  const fb = {
    table: (name, fields, desc) => ({ kind: "table", name, fields, desc }),
    vtables: (name, items, desc) => ({ kind: "vtables", name, items, desc }),
    vstructs: (name, size, align, items, desc) => ({ kind: "vstructs", name, size, align, items, desc }),
    string: (value) => ({ kind: "string", value }),
  };

  /**
   * Write a FlatBuffer forward: parents first, children after, so every
   * uoffset points forward as the format requires. Returns nothing, spans go
   * into w.
   */
  function writeFlatBuffer(w, root, label, desc) {
    const patches = [];
    const queue = [];
    const align = (n) => {
      const p = (n - (w.pos % n)) % n;
      if (p) w.leaf("padding", new Array(p).fill(0), { kind: "padding", type: `${p} byte(s)`, desc: "Alignment padding: FlatBuffers aligns every scalar to its own size, so it can be read in place without copying." });
    };
    const ref = (leafNode, target, what) => {
      patches.push({ at: leafNode.start, node: leafNode, target, what });
      if (!queue.includes(target)) queue.push(target);
    };
    const scalar = (type, v) => {
      switch (type) {
        case "u8":
        case "bool": return [v ? Number(v) & 0xff : 0];
        case "i16": return [v & 0xff, (v >> 8) & 0xff];
        case "i32": return Array.from(DF.le32(v));
        case "i64": return Array.from(DF.le64(v));
        case "offset": return [0, 0, 0, 0];
      }
      throw new Error(type);
    };

    function writeTable(t) {
      const slots = t.fields.length ? Math.max(...t.fields.map((f) => f.id)) + 1 : 0;
      const ordered = [...t.fields].sort((a, b) => SIZE[b.type] - SIZE[a.type]);
      let rel = 4;
      const placed = [];
      for (const f of ordered) {
        const s = SIZE[f.type];
        rel += (s - (rel % s)) % s;
        placed.push({ f, rel });
        rel += s;
      }
      const tableSize = rel;
      const needs8 = t.fields.some((f) => SIZE[f.type] === 8);
      return w.group(t.name, { kind: "fb", type: "FlatBuffers table", value: `${t.fields.length} field(s)`, desc: t.desc }, () => {
        align(2);
        const vtSize = 4 + 2 * slots;
        let vt;
        let tableStart = w.pos + vtSize;
        const tAlign = needs8 ? 8 : 4;
        tableStart += (tAlign - (tableStart % tAlign)) % tAlign;
        const vtPos = w.pos;
        w.group("vtable", { kind: "fb", type: "vtable", value: `${slots} slot(s)`, desc: "The vtable says where each field of the table sits. Absent fields have offset 0 and cost nothing in the table: that is how FlatBuffers evolves, by field slot, much like Protobuf field numbers." }, () => {
          w.leaf("vtable size", [vtSize & 0xff, vtSize >> 8], { kind: "fb", type: "uint16", value: `${vtSize} bytes`, rule: `4 + 2 x ${slots} slots` });
          w.leaf("table size", [tableSize & 0xff, tableSize >> 8], { kind: "fb", type: "uint16", value: `${tableSize} bytes` });
          for (let id = 0; id < slots; id++) {
            const p = placed.find((x) => x.f.id === id);
            const off = p ? p.rel : 0;
            w.leaf(`slot ${id}: ${p ? p.f.name : "(absent)"}`, [off & 0xff, off >> 8], { kind: "fb", type: "uint16 field offset", value: p ? `table + ${off}` : "0 = not present, default value", rule: p ? `${p.f.name} sits ${off} bytes into the table` : "offset 0 means the field is absent" });
          }
          vt = w.pos;
        });
        while (w.pos < tableStart) w.leaf("padding", [0], { kind: "padding", type: "1 byte" });
        w.group("table", { kind: "fb", type: "table data", value: `${tableSize} bytes` }, () => {
          const so = w.pos - vtPos;
          w.leaf("vtable offset", Array.from(DF.le32(so)), { kind: "fb", type: "int32 soffset", value: `vtable at table - ${so}`, rule: `table position - vtable position = ${so}`, desc: "Every table starts with a signed offset back to its vtable. Tables with the same shape may share one vtable." });
          const base = w.pos - 4;
          for (const { f, rel: r } of placed) {
            while (w.pos < base + r) w.leaf("padding", [0], { kind: "padding", type: "1 byte" });
            const leafNode = w.leaf(f.name, scalar(f.type, f.value), { kind: "fb", type: f.type === "offset" ? "uint32 offset" : f.type, value: f.show ?? String(f.value), desc: f.desc });
            if (f.type === "offset") ref(leafNode, f.value, f.name);
          }
          while (w.pos < base + tableSize) w.leaf("padding", [0], { kind: "padding", type: "1 byte" });
        });
      });
    }

    function writeStructs(v) {
      const a = v.align;
      while ((w.pos + 4) % a) w.leaf("padding", [0], { kind: "padding", type: "1 byte" });
      return w.group(v.name, { kind: "fb", type: `vector of ${v.size}-byte structs`, value: `${v.items.length} item(s)`, desc: v.desc }, () => {
        w.leaf("length", Array.from(DF.le32(v.items.length)), { kind: "fb", type: "uint32", value: String(v.items.length) });
        v.items.forEach((it) => {
          w.group(it.label, { kind: "fb", type: `struct (${v.size} bytes, inline)`, value: it.show, desc: it.desc, ref: it.ref }, () => {
            const start = w.pos;
            for (const f of it.fields) {
              const s = SIZE[f.type];
              while ((w.pos - start) % s) w.leaf("padding", [0], { kind: "padding", type: "1 byte" });
              w.leaf(f.name, scalar(f.type, f.value), { kind: "fb", type: f.type, value: String(f.value), desc: f.desc, ref: it.ref });
            }
            while (w.pos - start < v.size) w.leaf("padding", [0], { kind: "padding", type: "1 byte" });
          });
        });
      });
    }

    function writeTables(v) {
      align(4);
      return w.group(v.name, { kind: "fb", type: "vector of tables", value: `${v.items.length} item(s)`, desc: v.desc }, () => {
        w.leaf("length", Array.from(DF.le32(v.items.length)), { kind: "fb", type: "uint32", value: String(v.items.length) });
        v.items.forEach((t, i) => {
          const leafNode = w.leaf(`[${i}] -> ${t.name}`, [0, 0, 0, 0], { kind: "fb", type: "uint32 offset", value: t.name });
          ref(leafNode, t, `[${i}]`);
        });
      });
    }

    function writeString(s) {
      align(4);
      const b = DF.utf8(s.value);
      return w.group(`"${s.value}"`, { kind: "fb", type: "string", value: JSON.stringify(s.value) }, () => {
        w.leaf("length", Array.from(DF.le32(b.length)), { kind: "fb", type: "uint32", value: String(b.length) });
        w.leaf("bytes", b, { kind: "fb", type: "UTF-8", value: JSON.stringify(s.value) });
        w.leaf("terminator", [0], { kind: "fb", type: "NUL", value: "0x00", desc: "FlatBuffers strings are length-prefixed and NUL-terminated, so C code can use them in place." });
      });
    }

    w.group(label, { kind: "fb", type: "FlatBuffer", desc }, () => {
      const base = w.pos;
      const rootLeaf = w.leaf("root offset", [0, 0, 0, 0], { kind: "fb", type: "uint32 offset", value: root.name, desc: "A FlatBuffer starts with the offset of its root table. Readers follow offsets instead of parsing: nothing is decoded until it is touched." });
      ref(rootLeaf, root, "root");
      while (queue.length) {
        const obj = queue.shift();
        const node = obj.kind === "table" ? writeTable(obj) : obj.kind === "vstructs" ? writeStructs(obj) : obj.kind === "vtables" ? writeTables(obj) : writeString(obj);
        obj.node = node;
        obj.pos = obj.kind === "table" ? node.children.find((c) => c.label === "table").start : node.children[0].start;
      }
      for (const p of patches) {
        const delta = p.target.pos - p.at;
        if (delta <= 0) throw new Error("flatbuffer offset must point forward");
        w.buf.set(DF.le32(delta), p.at);
        p.node.meta.rule = `${p.at - base} + ${delta} = ${p.target.pos - base} (relative to buffer start)`;
        p.node.meta.value = `-> ${p.target.name || "string"}`;
      }
      const tail = (8 - ((w.pos - base) % 8)) % 8;
      if (tail) w.leaf("padding", new Array(tail).fill(0), { kind: "padding", type: `${tail} byte(s)`, desc: "Message metadata is padded to a multiple of 8 bytes, so the body that follows starts aligned." });
    });
  }

  function typeTable(col) {
    const t = TYPES[col.type];
    if (t.fb === "Int") return fb.table("Int", [{ id: 0, name: "bitWidth", type: "i32", value: t.bits }, { id: 1, name: "is_signed", type: "bool", value: t.signed ? 1 : 0, show: String(t.signed) }]);
    if (t.fb === "FloatingPoint") return fb.table("FloatingPoint", [{ id: 0, name: "precision", type: "i16", value: t.precision, show: t.precision === 1 ? "SINGLE (1)" : "DOUBLE (2)" }]);
    return fb.table(t.fb, [], `${t.fb} has no parameters: an empty table.`);
  }

  function schemaTable(cols) {
    return fb.table("Schema", [
      { id: 0, name: "endianness", type: "i16", value: 0, show: "Little (0)" },
      { id: 1, name: "fields", type: "offset", value: fb.vtables("fields", cols.map((c) => fb.table(`Field ${c.name}`, [
        { id: 0, name: "name", type: "offset", value: fb.string(c.name) },
        { id: 1, name: "nullable", type: "bool", value: c.nullable ? 1 : 0, show: String(c.nullable) },
        { id: 2, name: "type_type", type: "u8", value: TYPE_UNION[TYPES[c.type].fb], show: `${TYPES[c.type].fb} (${TYPE_UNION[TYPES[c.type].fb]})` },
        { id: 3, name: "type", type: "offset", value: typeTable(c) },
        { id: 5, name: "children", type: "offset", value: fb.vtables("children", []) },
      ], `Column '${c.name}': name, nullability and type. The name is the identity here, there is no field id.`)), "One Field table per column.") },
    ], "The schema: one Field per column.");
  }

  function message(headerName, header, bodyLength) {
    return fb.table("Message", [
      { id: 0, name: "version", type: "i16", value: 4, show: "V5 (4)" },
      { id: 1, name: "header_type", type: "u8", value: HEADER_UNION[headerName], show: `${headerName} (${HEADER_UNION[headerName]})` },
      { id: 2, name: "header", type: "offset", value: header },
      { id: 3, name: "bodyLength", type: "i64", value: bodyLength, show: `${bodyLength} bytes` },
    ], `Every IPC message is a FlatBuffer Message: a version, the header (${headerName}) and how many body bytes follow.`);
  }

  function encapsulate(w, label, fbRoot, fbDesc, writeBody) {
    const start = w.pos;
    let metaLen = 0;
    const node = w.group(label, { kind: "fb", type: "encapsulated IPC message", desc: "0xFFFFFFFF, a 4-byte metadata length, the FlatBuffer metadata padded to 8 bytes, then the body." }, () => {
      w.leaf("continuation", [0xff, 0xff, 0xff, 0xff], { kind: "magic", type: "4 bytes", value: "0xFFFFFFFF", desc: "Continuation marker, starts every message." });
      const lenLeaf = w.leaf("metadata length", [0, 0, 0, 0], { kind: "length", type: "int32, little endian" });
      const fbStart = w.pos;
      writeFlatBuffer(w, fbRoot, "metadata (FlatBuffer)", fbDesc);
      metaLen = w.pos - fbStart;
      w.buf.set(DF.le32(metaLen), lenLeaf.start);
      lenLeaf.meta.value = `${metaLen} bytes`;
      lenLeaf.meta.rule = `FlatBuffer + padding = ${metaLen} (multiple of 8)`;
      if (writeBody) writeBody();
    });
    return { start, node, metaDataLength: metaLen + 8, bodyLength: node.end - start - 8 - metaLen };
  }

  /** Write an Arrow IPC file. cols from parseSchema, rows as objects. */
  function build(cols, rows, opts = {}) {
    const batchRows = Math.max(1, opts.batchRows || rows.length || 1);
    rows.forEach((r, i) => cols.forEach((c) => checkValue(c, r[c.name], i)));
    const w = new DF.ByteWriter();
    w.leaf("Magic", DF.utf8("ARROW1"), { kind: "magic", type: "6 bytes ASCII", value: "ARROW1", rule: "41 52 52 4F 57 31", desc: "Arrow IPC file magic. The same 6 bytes end the file." });
    w.leaf("padding", [0, 0], { kind: "padding", type: "2 bytes", desc: "Pads the magic to 8 bytes so everything after it is 8-byte aligned." });
    encapsulate(w, "Schema message", message("Schema", schemaTable(cols), 0), "The schema, as a FlatBuffer. No body.");
    const blocks = [];
    const batches = [];
    for (let s = 0, bi = 0; s < Math.max(rows.length, 1); s += batchRows, bi++) {
      const slice = rows.slice(s, s + batchRows);
      const columns = cols.map((c) => columnBuffers(c, slice.map((r) => (r[c.name] === undefined ? null : r[c.name]))));
      const bufferList = [];
      let off = 0;
      for (const cb of columns) {
        for (const b of cb.bufs) {
          bufferList.push({ cb, b, offset: off, length: b.bytes.length });
          off += b.bytes.length + pad8(b.bytes.length);
        }
      }
      const bodyLength = off;
      const rb = fb.table("RecordBatch", [
        { id: 0, name: "length", type: "i64", value: slice.length, show: `${slice.length} rows` },
        { id: 1, name: "nodes", type: "offset", value: fb.vstructs("nodes", 16, 8, columns.map((cb) => ({
          label: `FieldNode ${cb.col.name}`, show: `length ${cb.n}, nulls ${cb.nulls}`,
          fields: [{ name: "length", type: "i64", value: cb.n }, { name: "null_count", type: "i64", value: cb.nulls }],
        })), "One FieldNode per column: row count and null count.") },
        { id: 2, name: "buffers", type: "offset", value: fb.vstructs("buffers", 16, 8, bufferList.map((x) => ({
          label: `Buffer ${x.cb.col.name}.${x.b.role}`, show: `offset ${x.offset}, length ${x.length}`,
          desc: x.length === 0 ? "Length 0: no nulls in this column, so no validity bitmap is stored at all." : `Where ${x.cb.col.name}.${x.b.role} sits in the body. A reader turns this into a pointer, no parsing, no copy.`,
          ref: { buffer: `${x.cb.col.name}.${x.b.role}`, batch: bi },
          fields: [{ name: "offset", type: "i64", value: x.offset }, { name: "length", type: "i64", value: x.length }],
        })), "Offset and length of every buffer in the body, in column order.") },
      ], `${slice.length} rows. The metadata only says where the buffers are, the body holds them verbatim.`);
      const batchInfo = { index: bi, rows: slice.length, firstRow: s, columns, buffers: [] };
      const msg = encapsulate(w, `Record batch ${bi}`, message("RecordBatch", rb, bodyLength), "RecordBatch metadata: row count, per-column null counts, and the location of every buffer in the body.", () => {
        w.group("body", { kind: "value", type: "buffers, each padded to 8 bytes", value: `${bodyLength} bytes`, desc: "The body is the in-memory layout, byte for byte. Memory-map the file and the arrays are ready: this is what zero-copy means for files." }, () => {
          for (const x of bufferList) writeBuffer(w, x, batchInfo);
        });
      });
      blocks.push({ offset: msg.start, metaDataLength: msg.metaDataLength, bodyLength });
      batches.push(batchInfo);
      if (!rows.length) break;
    }
    w.group("End of stream", { kind: "magic", type: "EOS marker", desc: "0xFFFFFFFF followed by a 0 length ends the stream part of the file." }, () => {
      w.leaf("continuation", [0xff, 0xff, 0xff, 0xff], { kind: "magic", type: "4 bytes", value: "0xFFFFFFFF" });
      w.leaf("length 0", [0, 0, 0, 0], { kind: "magic", type: "int32", value: "0 = end of stream" });
    });
    const footerStart = w.pos;
    const footer = fb.table("Footer", [
      { id: 0, name: "version", type: "i16", value: 4, show: "V5 (4)" },
      { id: 1, name: "schema", type: "offset", value: schemaTable(cols) },
      { id: 3, name: "recordBatches", type: "offset", value: fb.vstructs("recordBatches", 24, 8, blocks.map((b, i) => ({
        label: `Block ${i}`, show: `at ${b.offset}, meta ${b.metaDataLength}, body ${b.bodyLength}`,
        desc: "Where record batch i starts in the file. This is what makes the file format randomly accessible by batch, unlike the stream format.",
        fields: [{ name: "offset", type: "i64", value: b.offset }, { name: "metaDataLength", type: "i32", value: b.metaDataLength }, { name: "bodyLength", type: "i64", value: b.bodyLength }],
      })), "Offset of every record batch message.") },
    ], "The footer repeats the schema and lists where each record batch starts.");
    writeFlatBuffer(w, footer, "Footer (FlatBuffer)", "Read first: a reader seeks to the end, reads the footer size, then the footer, and can jump straight to any record batch.");
    const footerLen = w.pos - footerStart;
    w.leaf("Footer length", DF.le32(footerLen), { kind: "length", type: "int32, little endian", value: `${footerLen} bytes`, rule: `uint32 LE ${footerLen}` });
    w.leaf("Magic", DF.utf8("ARROW1"), { kind: "magic", type: "6 bytes ASCII", value: "ARROW1" });
    const out = w.finish("Arrow IPC file");
    out.root.meta = { type: "Arrow IPC file (Feather v2)", value: `${rows.length} rows, ${batches.length} batch(es)` };
    out.batches = batches;
    return out;
  }

  function writeBuffer(w, x, batchInfo) {
    const { cb, b } = x;
    const name = `${cb.col.name}.${b.role}`;
    const start = w.pos;
    const info = { name, role: b.role, col: cb.col.name, start, length: b.bytes.length, elems: [] };
    batchInfo.buffers.push(info);
    if (!b.bytes.length && !pad8(b.bytes.length)) return;
    w.group(name, { kind: b.role === "validity" ? "validity" : b.role === "offsets" ? "offsets" : "value", type: `${b.role} buffer`, value: `${b.bytes.length} bytes`, desc: bufferDesc(cb, b) }, () => {
      if (b.role === "validity" || b.bitmap) {
        for (let k = 0; k < b.bytes.length; k++) {
          const first = k * 8;
          const last = Math.min(cb.n, first + 8) - 1;
          const bits = [];
          for (let i = first; i <= last; i++) bits.push((b.bytes[k] >> (i - first)) & 1);
          const leafNode = w.leaf(`rows ${first} to ${last}`, [b.bytes[k]], {
            kind: b.role === "validity" ? "validity" : "value", type: "8 bits, LSB = first row", value: bits.join(""),
            bitpacked: { bw: 1, values: bits },
            rule: b.role === "validity" ? `row i is valid if byte[i / 8] >> (i % 8) & 1` : `value of row i = byte[i / 8] >> (i % 8) & 1`,
          });
          info.elems.push({ rows: [first, last], start: leafNode.start, end: leafNode.end });
        }
      } else if (b.role === "offsets") {
        b.elems.forEach((e) => {
          const leafNode = w.leaf(`offsets[${e.i}] = ${e.value}`, b.bytes.subarray(e.start, e.end), { kind: "offsets", type: "int32", value: String(e.value), rule: e.i < cb.n ? `row ${e.i} = data[offsets[${e.i}]..offsets[${e.i + 1}]]` : "end of the last string" });
          info.elems.push({ i: e.i, start: leafNode.start, end: leafNode.end });
        });
      } else if (b.role === "data") {
        b.elems.forEach((e) => {
          if (e.end === e.start) {
            info.elems.push({ i: e.i, start: start + e.start, end: start + e.start });
            return;
          }
          const leafNode = w.leaf(`[${e.i}] ${JSON.stringify(e.value)}`, b.bytes.subarray(e.start, e.end), { kind: "value", type: "UTF-8, no length, no separator", value: JSON.stringify(e.value), rule: `bytes ${e.start} to ${e.end - 1} of the data buffer` });
          info.elems.push({ i: e.i, start: leafNode.start, end: leafNode.end });
        });
      } else {
        b.elems.forEach((e) => {
          const leafNode = w.leaf(e.value === null ? `[${e.i}] null slot` : `[${e.i}] ${e.value}`, b.bytes.subarray(e.start, e.end), {
            kind: e.value === null ? "padding" : "value", type: `${cb.col.type}, ${b.width} bytes`,
            value: e.value === null ? "null (slot still occupied)" : String(e.value),
            rule: `address = values + ${b.width} x ${e.i} = values + ${b.width * e.i}`,
            desc: e.value === null ? "A null still occupies its full slot, so every value stays at a fixed address. The validity bitmap says to ignore it." : "Fixed width: row i is at a computed address, one load, no decoding.",
          });
          info.elems.push({ i: e.i, start: leafNode.start, end: leafNode.end });
        });
      }
      const p = pad8(b.bytes.length);
      if (p) w.leaf("padding", new Array(p).fill(0), { kind: "padding", type: `${p} byte(s)`, desc: "Buffers are padded to a multiple of 8 bytes." });
    });
  }

  function bufferDesc(cb, b) {
    if (b.role === "validity") return `One bit per row, 1 = valid. ${cb.nulls} of ${cb.n} rows are null.`;
    if (b.role === "offsets") return `${cb.n + 1} int32 offsets: row i spans data[offsets[i]..offsets[i+1]]. Two loads find any string, no walking.`;
    if (b.role === "data") return "All strings concatenated, no lengths, no separators. The offsets buffer is the index.";
    if (b.bitmap) return "Booleans are bit-packed: one bit per row.";
    return `${cb.n} slots of ${b.width} bytes. Row i is at offset ${b.width} x i, nulls included.`;
  }

  Object.assign(AR, { parseSchema, build });
})(globalThis);
