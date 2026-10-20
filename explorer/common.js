"use strict";

/**
 * Format independent helpers: a byte writer that records a span tree while
 * writing, varint and zigzag codecs, and a few formatting utilities.
 * Pure JavaScript, no DOM access, so the codecs can also run under Node.
 */
(function (g) {
  const DF = g.DF || (g.DF = {});

  const MASK64 = (1n << 64n) - 1n;
  const textEncoder = new TextEncoder();
  const textDecoder = new TextDecoder("utf-8", { fatal: true });

  /** A growable byte buffer that builds a tree of labelled byte ranges. */
  class ByteWriter {
    constructor() {
      this.buf = new Uint8Array(256);
      this.len = 0;
      this.root = { label: "file", start: 0, end: 0, children: [], meta: {}, parent: null };
      this.stack = [this.root];
    }

    get pos() {
      return this.len;
    }

    get top() {
      return this.stack[this.stack.length - 1];
    }

    ensure(extra) {
      if (this.len + extra <= this.buf.length) return;
      let size = this.buf.length * 2;
      while (size < this.len + extra) size *= 2;
      const next = new Uint8Array(size);
      next.set(this.buf.subarray(0, this.len));
      this.buf = next;
    }

    push(bytes) {
      if (typeof bytes === "number") bytes = [bytes];
      this.ensure(bytes.length);
      this.buf.set(bytes, this.len);
      this.len += bytes.length;
      return this;
    }

    open(label, meta = {}) {
      const node = { label, start: this.len, end: this.len, children: [], meta, parent: this.top };
      this.top.children.push(node);
      this.stack.push(node);
      return node;
    }

    close() {
      const node = this.stack.pop();
      node.end = this.len;
      return node;
    }

    leaf(label, bytes, meta = {}) {
      const node = this.open(label, meta);
      this.push(bytes);
      return this.close();
    }

    group(label, meta, fn) {
      const node = this.open(label, meta);
      fn(node);
      this.close();
      return node;
    }

    /** Append the bytes and span tree of another finished writer at the current position. */
    embed(result) {
      const offset = this.len;
      this.push(result.bytes);
      const parent = this.top;
      const rebase = (node, newParent) => {
        const copy = { label: node.label, start: node.start + offset, end: node.end + offset, meta: node.meta, parent: newParent, children: [] };
        copy.children = node.children.map((c) => rebase(c, copy));
        return copy;
      };
      for (const child of result.root.children) parent.children.push(rebase(child, parent));
    }

    finish(label) {
      while (this.stack.length > 1) this.close();
      this.root.end = this.len;
      if (label) this.root.label = label;
      return { bytes: this.buf.slice(0, this.len), root: this.root };
    }
  }

  function toBig(n) {
    return typeof n === "bigint" ? n : BigInt(Math.trunc(Number(n)));
  }

  /** Unsigned LEB128 varint, 7 payload bits per byte, high bit means "more bytes follow". */
  function varint(n) {
    let v = toBig(n) & MASK64;
    const out = [];
    while (v > 0x7fn) {
      out.push(Number(v & 0x7fn) | 0x80);
      v >>= 7n;
    }
    out.push(Number(v));
    return out;
  }

  function readVarint(bytes, pos) {
    let result = 0n;
    let shift = 0n;
    const start = pos;
    for (;;) {
      if (pos >= bytes.length) throw new Error(`varint at 0x${start.toString(16)} runs past the end of the data`);
      const b = bytes[pos++];
      result |= BigInt(b & 0x7f) << shift;
      if (!(b & 0x80)) break;
      shift += 7n;
      if (shift > 63n) throw new Error(`varint at 0x${start.toString(16)} is longer than 10 bytes`);
    }
    return { value: result, pos };
  }

  function zigzag(n) {
    const v = BigInt.asIntN(64, toBig(n));
    return BigInt.asUintN(64, (v << 1n) ^ (v >> 63n));
  }

  function unzigzag(u) {
    const v = BigInt.asUintN(64, u);
    return BigInt.asIntN(64, (v >> 1n) ^ -(v & 1n));
  }

  function le32(n) {
    const b = new Uint8Array(4);
    new DataView(b.buffer).setInt32(0, Number(n), true);
    return b;
  }

  function le64(n) {
    const b = new Uint8Array(8);
    new DataView(b.buffer).setBigInt64(0, BigInt.asIntN(64, toBig(n)), true);
    return b;
  }

  function be32(n) {
    const b = new Uint8Array(4);
    new DataView(b.buffer).setUint32(0, Number(n) >>> 0, false);
    return b;
  }

  function f32(x) {
    const b = new Uint8Array(4);
    new DataView(b.buffer).setFloat32(0, Number(x), true);
    return b;
  }

  function f64(x) {
    const b = new Uint8Array(8);
    new DataView(b.buffer).setFloat64(0, Number(x), true);
    return b;
  }

  function view(bytes, pos) {
    return new DataView(bytes.buffer, bytes.byteOffset + pos);
  }

  function utf8(s) {
    return textEncoder.encode(s);
  }

  function tryUtf8(bytes) {
    try {
      return textDecoder.decode(bytes);
    } catch {
      return null;
    }
  }

  function decodeUtf8(bytes) {
    return new TextDecoder().decode(bytes);
  }

  function hex(bytes, max = Infinity) {
    const parts = [];
    for (let i = 0; i < Math.min(bytes.length, max); i++) parts.push(bytes[i].toString(16).padStart(2, "0").toUpperCase());
    if (bytes.length > max) parts.push("...");
    return parts.join(" ");
  }

  function hexAddr(n, width = 2) {
    return "0x" + n.toString(16).toUpperCase().padStart(width, "0");
  }

  function fmtBytes(n) {
    if (n < 1024) return `${n} B`;
    if (n < 1024 * 1024) return `${(n / 1024).toFixed(1)} KiB`;
    if (n < 1024 * 1024 * 1024) return `${(n / 1024 / 1024).toFixed(2)} MiB`;
    return `${(n / 1024 / 1024 / 1024).toFixed(2)} GiB`;
  }

  function shorten(s, n = 40) {
    s = String(s);
    return s.length > n ? s.slice(0, n - 1) + "…" : s;
  }

  function show(v) {
    if (typeof v === "bigint") return v.toString();
    if (typeof v === "string") return JSON.stringify(v);
    if (v instanceof Uint8Array) return `bytes[${v.length}]`;
    return JSON.stringify(v, jsonReplacer);
  }

  function jsonReplacer(_k, v) {
    if (typeof v === "bigint") return Number.isSafeInteger(Number(v)) ? Number(v) : v.toString();
    if (v instanceof Uint8Array) return hex(v, 32);
    return v;
  }

  function toJSON(v, indent = 2) {
    return JSON.stringify(v, jsonReplacer, indent);
  }

  function concat(parts) {
    const total = parts.reduce((s, p) => s + p.length, 0);
    const out = new Uint8Array(total);
    let o = 0;
    for (const p of parts) {
      out.set(p, o);
      o += p.length;
    }
    return out;
  }

  async function compress(bytes, format) {
    const stream = new Blob([bytes]).stream().pipeThrough(new CompressionStream(format));
    return new Uint8Array(await new Response(stream).arrayBuffer());
  }

  async function decompress(bytes, format) {
    const stream = new Blob([bytes]).stream().pipeThrough(new DecompressionStream(format));
    return new Uint8Array(await new Response(stream).arrayBuffer());
  }

  /** Small deterministic PRNG (mulberry32) so generated data is identical on every load. */
  function rng(seed) {
    let a = seed >>> 0;
    const next = () => {
      a = (a + 0x6d2b79f5) >>> 0;
      let t = a;
      t = Math.imul(t ^ (t >>> 15), t | 1);
      t ^= t + Math.imul(t ^ (t >>> 7), t | 61);
      return ((t ^ (t >>> 14)) >>> 0) / 4294967296;
    };
    return {
      next,
      int: (lo, hi) => lo + Math.floor(next() * (hi - lo)),
      pick: (arr) => arr[Math.floor(next() * arr.length)],
      weighted: (arr, weights) => {
        const total = weights.reduce((a, b) => a + b, 0);
        let r = next() * total;
        for (let i = 0; i < arr.length; i++) {
          r -= weights[i];
          if (r < 0) return arr[i];
        }
        return arr[arr.length - 1];
      },
    };
  }

  function nodePath(node) {
    const out = [];
    while (node && node.parent) {
      out.unshift(node.parent.children.indexOf(node));
      node = node.parent;
    }
    return out;
  }

  function nodeAt(root, path) {
    let node = root;
    for (const i of path) {
      if (!node.children[i]) return node === root ? null : node;
      node = node.children[i];
    }
    return node;
  }

  function* walk(node) {
    yield node;
    for (const c of node.children) yield* walk(c);
  }

  Object.assign(DF, {
    ByteWriter, varint, readVarint, zigzag, unzigzag, le32, le64, be32, f32, f64, view,
    utf8, tryUtf8, decodeUtf8, hex, hexAddr, fmtBytes, shorten, show, toJSON, concat,
    compress, decompress, rng, nodePath, nodeAt, walk,
  });
})(globalThis);
