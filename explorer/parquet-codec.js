"use strict";

/**
 * Parquet: a small but real writer (data page v1, PLAIN and RLE_DICTIONARY,
 * optional GZIP, Thrift compact footer with min/max statistics) that records
 * every byte range it writes, plus a query planner that uses only the footer.
 */
(function (g) {
  const DF = g.DF;
  const PQ = (DF.PQ = {});

  const CT = { BOOL_TRUE: 1, BOOL_FALSE: 2, BYTE: 3, I16: 4, I32: 5, I64: 6, DOUBLE: 7, BINARY: 8, LIST: 9, SET: 10, MAP: 11, STRUCT: 12 };
  const CT_NAME = Object.fromEntries(Object.entries(CT).map(([k, v]) => [v, k]));
  const TYPE = { BOOLEAN: 0, INT32: 1, INT64: 2, INT96: 3, FLOAT: 4, DOUBLE: 5, BYTE_ARRAY: 6, FIXED_LEN_BYTE_ARRAY: 7 };
  const ENC = { PLAIN: 0, PLAIN_DICTIONARY: 2, RLE: 3, BIT_PACKED: 4, RLE_DICTIONARY: 8 };
  const ENC_NAME = Object.fromEntries(Object.entries(ENC).map(([k, v]) => [v, k]));
  const CONVERTED = { UTF8: 0, TIMESTAMP_MILLIS: 9 };
  const CODEC = { none: 0, gzip: 2 };
  const PAGE = { DATA_PAGE: 0, DICTIONARY_PAGE: 2 };
  const MAGIC = DF.utf8("PAR1");

  const T = {
    i16: (v, show, note) => ({ t: "i16", v, show, note }),
    i32: (v, show, note) => ({ t: "i32", v, show, note }),
    i64: (v, show, note) => ({ t: "i64", v, show, note }),
    bin: (v, show, note) => ({ t: "bin", v: typeof v === "string" ? DF.utf8(v) : v, show, note }),
    bool: (v, note) => ({ t: "bool", v, note }),
    struct: (fields, show, note) => ({ t: "struct", fields, show, note }),
    list: (elem, items, show, note) => ({ t: "list", elem, items, show, note }),
  };

  function ctype(spec) {
    switch (spec.t) {
      case "i16": return CT.I16;
      case "i32": return CT.I32;
      case "i64": return CT.I64;
      case "bin": return CT.BINARY;
      case "bool": return spec.v ? CT.BOOL_TRUE : CT.BOOL_FALSE;
      case "struct": return CT.STRUCT;
      case "list": return CT.LIST;
    }
    throw new Error(`unknown thrift type ${spec.t}`);
  }

  function display(spec) {
    if (spec.show !== undefined) return spec.show;
    switch (spec.t) {
      case "i16":
      case "i32":
      case "i64": return String(spec.v);
      case "bool": return String(spec.v);
      case "bin": {
        const s = DF.tryUtf8(spec.v);
        return s !== null && /^[\x20-\x7e]*$/.test(s) ? JSON.stringify(DF.shorten(s, 40)) : DF.hex(spec.v, 8);
      }
      case "list": return `${spec.items.length} item(s)`;
      case "struct": return "";
    }
    return "";
  }

  function tname(spec) {
    if (spec.t === "list") return `list<${spec.elem}>`;
    return { i16: "i16", i32: "i32", i64: "i64", bin: "binary", bool: "bool", struct: "struct" }[spec.t];
  }

  function writeFields(w, fields, kind) {
    let last = 0;
    for (const [id, name, spec] of fields) {
      if (spec === undefined || spec === null) continue;
      const ct = ctype(spec);
      const delta = id - last;
      w.group(name, { kind, type: `thrift field ${id}: ${tname(spec)}`, value: display(spec), desc: spec.note }, () => {
        const short = delta > 0 && delta <= 15;
        const hdr = short ? [(delta << 4) | ct] : [ct, ...DF.varint(DF.zigzag(id))];
        w.leaf("field header", hdr, {
          kind, type: "thrift compact field header", value: `field ${id}, ${CT_NAME[ct]}`,
          rule: short ? `(id delta ${delta} << 4) | type ${ct} (${CT_NAME[ct]}) = ${DF.hexAddr(hdr[0])}` : `type ${ct}, then zigzag varint field id ${id}`,
          desc: spec.t === "bool"
            ? "Thrift compact protocol packs booleans into the field header: type 1 means true, 2 means false. No value bytes follow."
            : "Thrift compact protocol: high nibble is the field id minus the previous field id, low nibble is the value type. Unset optional fields cost nothing.",
        });
        if (spec.t !== "bool") writeValue(w, spec, "value", kind);
      });
      last = id;
    }
    w.leaf("stop", [0], { kind, type: "struct end", value: "STOP", rule: "0x00", desc: "Field type 0 ends the struct." });
  }

  function writeValue(w, spec, label, kind) {
    switch (spec.t) {
      case "i16":
      case "i32":
      case "i64":
        return w.leaf(label, DF.varint(DF.zigzag(spec.v)), { kind, type: `${spec.t} (zigzag varint)`, value: display(spec), varint: true, zigzag: true, rule: `zigzag(${spec.v}) = ${DF.zigzag(spec.v)}`, desc: spec.note });
      case "bin":
        w.leaf(`${label} length`, DF.varint(spec.v.length), { kind, type: "varint length", value: String(spec.v.length), varint: true });
        return w.leaf(label, spec.v, { kind, type: "binary", value: display(spec), desc: spec.note });
      case "struct":
        return w.group(label, { kind, type: "struct", value: spec.show || "", desc: spec.note }, () => writeFields(w, spec.fields, kind));
      case "list": {
        const n = spec.items.length;
        const et = spec.items.length ? ctype(spec.items[0]) : CT.STRUCT;
        const hdr = n < 15 ? [(n << 4) | et] : [0xf0 | et, ...DF.varint(n)];
        w.leaf("list header", hdr, { kind, type: "thrift list header", value: `${n} x ${CT_NAME[et]}`, rule: n < 15 ? `(size ${n} << 4) | elem type ${et}` : `0xF0 | elem type ${et}, then varint size ${n}` });
        spec.items.forEach((it, i) => writeValue(w, it, it.show && it.t === "struct" ? `[${i}] ${it.show}` : `[${i}]`, kind));
        return;
      }
    }
  }

  function plainBytes(col, v) {
    switch (col.ptype) {
      case "INT32": return DF.le32(v);
      case "INT64": return DF.le64(v);
      case "DOUBLE": return DF.f64(v);
      case "FLOAT": return DF.f32(v);
      case "BYTE_ARRAY": {
        const b = DF.utf8(v);
        return DF.concat([DF.le32(b.length), b]);
      }
    }
    throw new Error(col.ptype);
  }

  function statBytes(col, v) {
    return col.ptype === "BYTE_ARRAY" ? DF.utf8(v) : plainBytes(col, v);
  }

  function showValue(col, v) {
    if (v === null || v === undefined) return "null";
    if (col.converted === "TIMESTAMP_MILLIS") return new Date(v).toISOString().replace(".000Z", "Z");
    if (col.ptype === "BYTE_ARRAY") return JSON.stringify(v);
    return String(v);
  }

  function plainRule(col) {
    switch (col.ptype) {
      case "INT32": return "4 bytes, little endian";
      case "INT64": return "8 bytes, little endian";
      case "DOUBLE": return "IEEE 754 binary64, little endian";
      case "FLOAT": return "IEEE 754 binary32, little endian";
      case "BYTE_ARRAY": return "4-byte little endian length, then the UTF-8 bytes";
    }
    return "";
  }

  function compare(a, b) {
    return a < b ? -1 : a > b ? 1 : 0;
  }

  function bitWidth(n) {
    let w = 0;
    while ((1 << w) < n) w++;
    return Math.max(1, w);
  }

  /** RLE / bit-packing hybrid: runs of 8+ equal values become RLE runs, everything else is bit-packed in groups of 8. */
  function hybridRuns(values) {
    const runs = [];
    let pending = [];
    const flush = () => {
      if (pending.length) runs.push({ kind: "bp", values: pending });
      pending = [];
    };
    let i = 0;
    while (i < values.length) {
      let j = i;
      while (j < values.length && values[j] === values[i]) j++;
      let run = j - i;
      if (run >= 8) {
        while (pending.length % 8 !== 0 && run > 0) {
          pending.push(values[i]);
          i++;
          run--;
        }
        flush();
        if (run >= 8) {
          runs.push({ kind: "rle", value: values[i], count: run });
          i += run;
        } else {
          for (let k = 0; k < run; k++) pending.push(values[i + k]);
          i += run;
        }
      } else {
        for (let k = 0; k < run; k++) pending.push(values[i + k]);
        i += run;
      }
    }
    flush();
    return runs;
  }

  function bitPack(values, bw) {
    const padded = values.length % 8 ? values.concat(new Array(8 - (values.length % 8)).fill(0)) : values;
    const out = new Uint8Array((padded.length * bw) / 8);
    let bit = 0;
    for (const v of padded) {
      for (let b = 0; b < bw; b++) {
        if ((v >> b) & 1) out[bit >> 3] |= 1 << (bit & 7);
        bit++;
      }
    }
    return { bytes: out, groups: padded.length / 8 };
  }

  function encodeHybrid(w, indices, dict, col) {
    const bw = bitWidth(dict.length);
    const valueBytes = Math.ceil(bw / 8);
    w.leaf("bit width", [bw], {
      kind: "rle", type: "1 byte", value: `${bw} bit(s)`,
      rule: `ceil(log2(${dict.length} dictionary entries)) = ${bw}`,
      desc: `Every row stores only an index into the dictionary, ${bw} bit(s) wide. The strings themselves are in the dictionary page.`,
    });
    const runs = hybridRuns(indices);
    let row = 0;
    runs.forEach((run, ri) => {
      if (run.kind === "rle") {
        const header = DF.varint(run.count << 1);
        const label = `RLE run: ${run.count} x ${showValue(col, dict[run.value])}`;
        w.group(label, { kind: "rle", type: "RLE run", value: `${run.count} x index ${run.value}`, desc: `Rows ${row} to ${row + run.count - 1} all hold ${showValue(col, dict[run.value])}. ${run.count} rows cost ${header.length + valueBytes} bytes.` }, () => {
          w.leaf("run header", header, { kind: "rle", type: "varint", varint: true, value: `${run.count} values, RLE`, rule: `(count ${run.count} << 1) | 0 = ${run.count << 1}`, desc: "Lowest bit 0 marks an RLE run. The remaining bits are the repeat count." });
          const vb = new Uint8Array(valueBytes);
          for (let b = 0; b < valueBytes; b++) vb[b] = (run.value >> (8 * b)) & 0xff;
          w.leaf("repeated value", vb, { kind: "rle", type: `dictionary index, ${valueBytes} byte(s)`, value: `${run.value} = ${showValue(col, dict[run.value])}`, rule: `index ${run.value} padded to whole bytes` });
        });
        row += run.count;
      } else {
        const packed = bitPack(run.values, bw);
        const header = DF.varint((packed.groups << 1) | 1);
        const preview = run.values.slice(0, 16).map((x) => x).join(" ");
        w.group(`bit-packed: ${run.values.length} values`, { kind: "bitpacked", type: "bit-packed run", value: `${run.values.length} values x ${bw} bit`, desc: `${run.values.length} rows that change too often for RLE. Each costs ${bw} bit(s): ${packed.bytes.length} bytes for ${run.values.length} rows.` }, () => {
          w.leaf("run header", header, { kind: "bitpacked", type: "varint", varint: true, value: `${packed.groups} group(s) of 8`, rule: `(groups ${packed.groups} << 1) | 1 = ${(packed.groups << 1) | 1}`, desc: "Lowest bit 1 marks a bit-packed run, the rest is the number of 8-value groups." });
          w.leaf("packed indices", packed.bytes, {
            kind: "bitpacked", type: `${bw}-bit values, LSB first`, value: preview + (run.values.length > 16 ? " ..." : ""),
            rule: `${packed.groups * 8} values x ${bw} bits = ${packed.bytes.length} bytes`,
            desc: `Dictionary indices: ${run.values.slice(0, 8).map((x) => `${x}=${showValue(col, dict[x])}`).join(", ")}${run.values.length > 8 ? ", ..." : ""}`,
            bitpacked: { bw, values: run.values.slice(0, 16) },
          });
        });
        row += run.values.length;
      }
    });
    return runs;
  }

/** Definition levels for an OPTIONAL column: 1 = value present, 0 = null, as a 4-byte length plus RLE/bit-packed hybrid. */
  function encodeLevels(w, values) {
    const levels = values.map((v) => (v === null || v === undefined ? 0 : 1));
    const nulls = levels.filter((l) => l === 0).length;
    const sub = new DF.ByteWriter();
    let row = 0;
    for (const run of hybridRuns(levels)) {
      if (run.kind === "rle") {
        const header = DF.varint(run.count << 1);
        const what = run.value ? "present" : "null";
        sub.group(`RLE run: ${run.count} x ${what}`, { kind: "levels", type: "RLE run of levels", value: `${run.count} x ${run.value}`, desc: `Rows ${row} to ${row + run.count - 1} are all ${what}, ${header.length + 1} bytes for ${run.count} rows.` }, () => {
          sub.leaf("run header", header, { kind: "levels", type: "varint", varint: true, value: `${run.count} levels, RLE`, rule: `(count ${run.count} << 1) | 0 = ${run.count << 1}` });
          sub.leaf("level", [run.value], { kind: "levels", type: "1 byte", value: `${run.value} = ${what}` });
        });
        row += run.count;
      } else {
        const packed = bitPack(run.values, 1);
        const header = DF.varint((packed.groups << 1) | 1);
        sub.group(`bit-packed: ${run.values.length} levels`, { kind: "levels", type: "bit-packed levels", value: run.values.slice(0, 16).join(""), desc: `One bit per row, 1 = present, 0 = null.` }, () => {
          sub.leaf("run header", header, { kind: "levels", type: "varint", varint: true, value: `${packed.groups} group(s) of 8`, rule: `(groups ${packed.groups} << 1) | 1 = ${(packed.groups << 1) | 1}` });
          sub.leaf("packed levels", packed.bytes, { kind: "levels", type: "1-bit values, LSB first", value: run.values.slice(0, 16).join(""), bitpacked: { bw: 1, values: run.values.slice(0, 16) } });
        });
        row += run.values.length;
      }
    }
    const body = sub.finish();
    w.group("definition levels", { kind: "levels", type: "RLE/bit-packed hybrid, bit width 1", value: `${nulls} null(s) in ${values.length} rows`, desc: "One level per row: 1 means a value follows in the values section, 0 means null. Nulls cost these bits and nothing else." }, () => {
      w.leaf("levels length", DF.le32(body.bytes.length), { kind: "levels", type: "4 bytes, little endian", value: `${body.bytes.length} bytes`, rule: `uint32 LE ${body.bytes.length}` });
      w.embed(body);
    });
    return nulls;
  }

  async function writePage(w, label, kind, body, headerFields, codec, desc) {
    const uncompressed = body.bytes;
    const compressed = codec === "gzip" ? await DF.compress(uncompressed, "gzip") : uncompressed;
    const start = w.pos;
    let headerLen = 0;
    w.group(label, { kind: "page", type: kind === PAGE.DATA_PAGE ? "DATA_PAGE (v1)" : "DICTIONARY_PAGE", value: `${compressed.length} B body`, desc }, () => {
      const h = w.group("Page header", { kind: "pagehdr", type: "thrift PageHeader", value: `${PAGE_NAME[kind]}`, desc: "Each page starts with a Thrift-encoded header: page type, sizes and encoding. A reader can skip a page by jumping over compressed_page_size bytes." }, () => {
        writeFields(w, [
          [1, "type", T.i32(kind, `${PAGE_NAME[kind]} (${kind})`)],
          [2, "uncompressed_page_size", T.i32(uncompressed.length)],
          [3, "compressed_page_size", T.i32(compressed.length)],
          ...headerFields,
        ], "pagehdr");
      });
      headerLen = h.end - h.start;
      if (codec === "gzip") {
        w.leaf("compressed body", compressed, { kind: "compressed", type: "GZIP", value: `${uncompressed.length} B to ${compressed.length} B`, desc: "The page body is compressed as a unit. The encodings inside (dictionary indices, RLE runs) are applied first, compression second." });
      } else {
        w.group(kind === PAGE.DATA_PAGE ? "Values" : "Dictionary values", { kind: kind === PAGE.DATA_PAGE ? "value" : "dict", type: "page body", value: `${uncompressed.length} B` }, () => w.embed(body));
      }
    });
    return { offset: start, headerLen, uncompressed: uncompressed.length, compressed: compressed.length };
  }

  const PAGE_NAME = { 0: "DATA_PAGE", 2: "DICTIONARY_PAGE" };

  async function writeChunk(w, col, ci, values, codec) {
    const start = w.pos;
    const all = values;
    values = all.filter((v) => v !== null && v !== undefined);
    const nulls = all.length - values.length;
    if (nulls && !col.nullable) throw new Error(`column ${col.name} is REQUIRED but has nulls`);
    let min = values[0];
    let max = values[0];
    for (const v of values) {
      if (compare(v, min) < 0) min = v;
      if (compare(v, max) > 0) max = v;
    }
    const info = { name: col.name, index: ci, start, encoding: col.encoding, min, max, numValues: all.length, nulls, pages: [] };
    const levels = (vb) => {
      if (col.nullable) encodeLevels(vb, all);
    };
    w.open(col.name, { kind: "chunk", col: ci, type: `column chunk (${col.ptype})`, value: `${all.length} values`, desc: `All ${all.length} values of '${col.name}' in this row group, stored contiguously. A query that does not touch '${col.name}' never reads these bytes.` });
    if (col.encoding === "DICT") {
      const index = new Map();
      const dict = [];
      const indices = values.map((v) => {
        if (!index.has(v)) {
          index.set(v, dict.length);
          dict.push(v);
        }
        return index.get(v);
      });
      const db = new DF.ByteWriter();
      dict.forEach((v, i) => db.leaf(`[${i}] ${showValue(col, v)}`, plainBytes(col, v), { kind: "dict", type: `${col.ptype}, PLAIN`, value: showValue(col, v), rule: plainRule(col), desc: `Dictionary entry ${i}. Stored once per column chunk, referenced by index from every row.` }));
      const dp = await writePage(w, "Dictionary page", PAGE.DICTIONARY_PAGE, db.finish(), [
        [7, "dictionary_page_header", T.struct([[1, "num_values", T.i32(dict.length)], [2, "encoding", T.i32(ENC.PLAIN, "PLAIN (0)")]])],
      ], codec, `${dict.length} distinct value(s), each stored once.`);
      info.dictOffset = dp.offset;
      info.dictSize = dict.length;
      info.pages.push(dp);
      const vb = new DF.ByteWriter();
      levels(vb);
      const runs = encodeHybrid(vb, indices, dict, col);
      info.runs = { rle: runs.filter((r) => r.kind === "rle").length, bp: runs.filter((r) => r.kind === "bp").length };
      const pp = await writePage(w, "Data page", PAGE.DATA_PAGE, vb.finish(), [
        [5, "data_page_header", T.struct([
          [1, "num_values", T.i32(all.length)],
          [2, "encoding", T.i32(ENC.RLE_DICTIONARY, "RLE_DICTIONARY (8)")],
          [3, "definition_level_encoding", T.i32(ENC.RLE, "RLE (3)")],
          [4, "repetition_level_encoding", T.i32(ENC.RLE, "RLE (3)")],
        ])],
      ], codec, `${values.length} dictionary indices in ${runs.length} run(s).`);
      info.dataOffset = pp.offset;
      info.pages.push(pp);
      info.encodings = col.nullable ? [ENC.PLAIN, ENC.RLE, ENC.RLE_DICTIONARY] : [ENC.PLAIN, ENC.RLE_DICTIONARY];
    } else {
      const vb = new DF.ByteWriter();
      levels(vb);
      values.forEach((v, i) => vb.leaf(`[${i}] ${showValue(col, v)}`, plainBytes(col, v), { kind: "value", type: `${col.ptype}, PLAIN`, value: showValue(col, v), rule: plainRule(col) }));
      const pp = await writePage(w, "Data page", PAGE.DATA_PAGE, vb.finish(), [
        [5, "data_page_header", T.struct([
          [1, "num_values", T.i32(all.length)],
          [2, "encoding", T.i32(ENC.PLAIN, "PLAIN (0)")],
          [3, "definition_level_encoding", T.i32(ENC.RLE, "RLE (3)")],
          [4, "repetition_level_encoding", T.i32(ENC.RLE, "RLE (3)")],
        ])],
      ], codec, `${values.length} values, PLAIN: every value written in full.`);
      info.dataOffset = pp.offset;
      info.pages.push(pp);
      info.encodings = col.nullable ? [ENC.PLAIN, ENC.RLE] : [ENC.PLAIN];
    }
    const node = w.close();
    node.meta.value = `${all.length} values${nulls ? ` (${nulls} null)` : ""}, ${DF.fmtBytes(node.end - node.start)}`;
    info.end = w.pos;
    info.uncompressed = info.pages.reduce((s, p) => s + p.headerLen + p.uncompressed, 0);
    info.compressed = info.pages.reduce((s, p) => s + p.headerLen + p.compressed, 0);
    return info;
  }

  /** Write a Parquet file. columns: [{name, ptype, converted, encoding: "PLAIN"|"DICT"}]. */
  async function build(rows, columns, opts = {}) {
    const { rowGroupSize = rows.length, codec = "none", sortBy = null } = opts;
    const w = new DF.ByteWriter();
    w.leaf("Magic", MAGIC, { kind: "magic", type: "4 bytes ASCII", value: "PAR1", rule: "0x50 0x41 0x52 0x31", desc: "Parquet files start and end with 'PAR1'. Readers check the end first, because that is where the footer is." });
    const groups = [];
    for (let s = 0, gi = 0; s < rows.length; s += rowGroupSize, gi++) {
      const slice = rows.slice(s, s + rowGroupSize);
      const rgStart = w.pos;
      w.open(`Row group ${gi}`, { kind: "rowgroup", type: "row group", value: `rows ${s} to ${s + slice.length - 1}`, desc: `A horizontal slice of ${slice.length} rows. Inside it, every column is stored as its own contiguous chunk.` });
      const chunks = [];
      for (let ci = 0; ci < columns.length; ci++) {
        chunks.push(await writeChunk(w, columns[ci], ci, slice.map((r) => r[columns[ci].name]), codec));
      }
      w.close();
      groups.push({ index: gi, start: rgStart, end: w.pos, numRows: slice.length, firstRow: s, chunks });
    }

    const sortIdx = sortBy ? columns.findIndex((c) => c.name === sortBy) : -1;
    const footerStart = w.pos;
    const fileMeta = [
      [1, "version", T.i32(1)],
      [2, "schema", T.list("SchemaElement", [
        T.struct([[4, "name", T.bin("schema")], [5, "num_children", T.i32(columns.length)]], "root", "The root of the schema tree. Columns are its children."),
        ...columns.map((c) => T.struct([
          [1, "type", T.i32(TYPE[c.ptype], `${c.ptype} (${TYPE[c.ptype]})`)],
          [3, "repetition_type", c.nullable ? T.i32(1, "OPTIONAL (1)") : T.i32(0, "REQUIRED (0)")],
          [4, "name", T.bin(c.name)],
          [6, "converted_type", c.converted ? T.i32(CONVERTED[c.converted], `${c.converted} (${CONVERTED[c.converted]})`) : undefined],
          [9, "field_id", c.fieldId !== undefined ? T.i32(c.fieldId, undefined, "The column's permanent id, assigned by the table format. Readers that match by id survive renames.") : undefined],
        ], c.name)),
      ], undefined, columns.some((c) => c.fieldId !== undefined)
        ? "The schema lives here, at the end of the file. Every column carries a field_id: the name is a label, the id is the identity."
        : "The schema lives here, at the end of the file. Columns are identified by name and position: there are no field ids.")],
      [3, "num_rows", T.i64(rows.length)],
      [4, "row_groups", T.list("RowGroup", groups.map((rg) => T.struct([
        [1, "columns", T.list("ColumnChunk", rg.chunks.map((ch) => T.struct([
          [2, "file_offset", T.i64(ch.start)],
          [3, "meta_data", T.struct([
            [1, "type", T.i32(TYPE[columns[ch.index].ptype], `${columns[ch.index].ptype} (${TYPE[columns[ch.index].ptype]})`)],
            [2, "encodings", T.list("Encoding", ch.encodings.map((e) => T.i32(e, `${ENC_NAME[e]} (${e})`)))],
            [3, "path_in_schema", T.list("string", [T.bin(ch.name)])],
            [4, "codec", T.i32(CODEC[codec], `${codec === "gzip" ? "GZIP" : "UNCOMPRESSED"} (${CODEC[codec]})`)],
            [5, "num_values", T.i64(ch.numValues)],
            [6, "total_uncompressed_size", T.i64(ch.uncompressed)],
            [7, "total_compressed_size", T.i64(ch.compressed)],
            [9, "data_page_offset", T.i64(ch.dataOffset, undefined, "Absolute byte offset of the first data page. This is how a reader jumps straight to one column of one row group.")],
            [11, "dictionary_page_offset", ch.dictOffset !== undefined ? T.i64(ch.dictOffset) : undefined],
            [12, "statistics", T.struct([
              [3, "null_count", T.i64(ch.nulls)],
              [5, "max_value", ch.max !== undefined ? T.bin(statBytes(columns[ch.index], ch.max), showValue(columns[ch.index], ch.max), "Largest value in this column chunk.") : undefined],
              [6, "min_value", ch.min !== undefined ? T.bin(statBytes(columns[ch.index], ch.min), showValue(columns[ch.index], ch.min), "Smallest value in this column chunk.") : undefined],
            ], `min ${showValue(columns[ch.index], ch.min)}, max ${showValue(columns[ch.index], ch.max)}`, "Min/max per column chunk. A query whose predicate cannot match this range skips the whole chunk without reading it.")],
          ], `${ch.name}`)],
        ], ch.name))), undefined],
        [2, "total_byte_size", T.i64(rg.chunks.reduce((s, c) => s + c.uncompressed, 0))],
        [3, "num_rows", T.i64(rg.numRows)],
        [4, "sorting_columns", sortIdx >= 0 ? T.list("SortingColumn", [T.struct([[1, "column_idx", T.i32(sortIdx, `${sortIdx} (${sortBy})`)], [2, "descending", T.bool(false)], [3, "nulls_first", T.bool(false)]], sortBy)], undefined, "The writer declares the sort order. Engines can use it, but only the min/max statistics make the skipping work.") : undefined],
        [5, "file_offset", T.i64(rg.start)],
        [6, "total_compressed_size", T.i64(rg.chunks.reduce((s, c) => s + c.compressed, 0))],
        [7, "ordinal", T.i16(rg.index)],
      ], `rows ${rg.firstRow} to ${rg.firstRow + rg.numRows - 1}`)), undefined, "One entry per row group, each listing its column chunks with byte offsets and statistics.")],
      [6, "created_by", T.bin("dataformats explorer")],
      [7, "column_orders", T.list("ColumnOrder", columns.map((c) => T.struct([[1, "TYPE_ORDER", T.struct([], "TypeDefinedOrder")]], c.name)), undefined, "Declares that min/max follow the natural order of each type, so readers may trust them.")],
    ];
    w.group("Footer (FileMetaData)", { kind: "footer", type: "thrift FileMetaData", value: "schema, row groups, offsets, stats", desc: "Everything a reader needs to plan: the schema, every row group, every column chunk's byte offset and min/max statistics. Written last, because only now are offsets and statistics known." }, () => writeFields(w, fileMeta, "footer"));
    const footerLen = w.pos - footerStart;
    w.leaf("Footer length", DF.le32(footerLen), { kind: "length", type: "4 bytes, little endian", value: `${footerLen} bytes`, rule: `uint32 LE ${footerLen}`, desc: "A reader seeks to the end of the file, reads these 4 bytes and the magic, then reads exactly this many bytes of footer. Two small reads, then it knows where everything is." });
    w.leaf("Magic", MAGIC, { kind: "magic", type: "4 bytes ASCII", value: "PAR1", rule: "0x50 0x41 0x52 0x31" });
    const out = w.finish("Parquet file");
    out.root.meta = { type: "Apache Parquet file", value: `${rows.length} rows, ${groups.length} row group(s)` };
    out.groups = groups;
    out.footer = { start: footerStart, end: footerStart + footerLen, tail: [footerStart + footerLen, out.bytes.length] };
    return out;
  }

  function canMatch(min, max, op, v) {
    switch (op) {
      case "=": return compare(min, v) <= 0 && compare(max, v) >= 0;
      case "!=": return !(compare(min, v) === 0 && compare(max, v) === 0);
      case ">": return compare(max, v) > 0;
      case ">=": return compare(max, v) >= 0;
      case "<": return compare(min, v) < 0;
      case "<=": return compare(min, v) <= 0;
    }
    return true;
  }

  function test(x, op, v) {
    const c = compare(x, v);
    return { "=": c === 0, "!=": c !== 0, ">": c > 0, ">=": c >= 0, "<": c < 0, "<=": c <= 0 }[op];
  }

  /**
   * Plan a query the way a Parquet reader does: read the tail and footer,
   * drop row groups whose statistics rule the predicate out, then fetch only
   * the column chunks the query needs. Returns byte ranges and the result.
   */
  function plan(file, rows, query) {
    const { select, where } = query;
    const ranges = [{ start: file.footer.start, end: file.bytes.length, what: "footer" }];
    const decisions = [];
    let matched = 0;
    const sample = [];
    const needed = new Set(select);
    if (where) needed.add(where.col);
    for (const rg of file.groups) {
      let read = true;
      let reason = "no predicate";
      if (where) {
        const ch = rg.chunks.find((c) => c.name === where.col);
        read = ch.min !== undefined && canMatch(ch.min, ch.max, where.op, where.value);
        reason = `${where.col} in [${fmt(ch.min)}, ${fmt(ch.max)}]`;
      }
      decisions.push({ rg: rg.index, read, reason });
      if (!read) continue;
      for (const ch of rg.chunks) if (needed.has(ch.name)) ranges.push({ start: ch.start, end: ch.end, what: `rg ${rg.index} ${ch.name}` });
      for (let i = rg.firstRow; i < rg.firstRow + rg.numRows; i++) {
        if (!where || (rows[i][where.col] !== null && test(rows[i][where.col], where.op, where.value))) {
          matched++;
          if (sample.length < 5) sample.push(Object.fromEntries(select.map((c) => [c, rows[i][c]])));
        }
      }
    }
    const bytesRead = ranges.reduce((s, r) => s + (r.end - r.start), 0);
    return { ranges, decisions, matched, sample, bytesRead, total: file.bytes.length };
  }

  function fmt(v) {
    return typeof v === "string" ? JSON.stringify(v) : String(v);
  }

  const COLUMNS = [
    { name: "device_id", ptype: "INT32", encoding: "PLAIN" },
    { name: "location", ptype: "BYTE_ARRAY", converted: "UTF8", encoding: "DICT" },
    { name: "status", ptype: "BYTE_ARRAY", converted: "UTF8", encoding: "DICT" },
    { name: "temp_c", ptype: "DOUBLE", encoding: "PLAIN" },
    { name: "ts", ptype: "INT64", converted: "TIMESTAMP_MILLIS", encoding: "PLAIN" },
  ];

  /** Synthetic sensor readings with an arrival timestamp, seeded so every load writes the same file. */
  function makeRows(n, seed = 42) {
    const r = DF.rng(seed);
    const base = Date.UTC(2026, 9, 1, 8, 0, 0);
    let ts = base;
    const rows = [];
    for (let i = 0; i < n; i++) {
      ts += 1000 + r.int(0, 4000);
      rows.push({
        device_id: r.int(1000, 9999),
        location: r.pick(["hall-a", "hall-b", "yard", "cold-storage"]),
        status: r.weighted(["ok", "warn", "fault"], [90, 8, 2]),
        temp_c: Math.round((15 + r.next() * 30) * 100) / 100,
        ts,
      });
    }
    return rows;
  }

  function sortRows(rows, col) {
    if (!col) return rows.slice();
    return rows.slice().sort((a, b) => compare(a[col], b[col]) || a.ts - b.ts);
  }

  Object.assign(PQ, { build, plan, makeRows, sortRows, COLUMNS, showValue });
})(globalThis);
