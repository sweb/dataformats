"use strict";

/**
 * Shared DOM components: structure diagram, file map, virtualized hex view,
 * field inspector, and the Explorer that keeps their selection in sync.
 */
(function (g) {
  const DF = g.DF;
  const UI = (DF.UI = {});

  const HUES = { magic: 215, meta: 265, schema: 290, sync: 25, count: 175, length: 45, tag: 345, value: 135, pagehdr: 195, footer: 250, dict: 305, rle: 10, bitpacked: 55, compressed: 220, levels: 95, validity: 95, offsets: 45, fb: 265, padding: 220 };
  const COL_HUES = [215, 135, 305, 25, 175, 265, 45];

  function esc(s) {
    return String(s ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" })[c]);
  }

  function el(tag, attrs = {}, ...children) {
    const e = document.createElement(tag);
    for (const [k, v] of Object.entries(attrs)) {
      if (v === undefined || v === null || v === false) continue;
      if (k === "class") e.className = v;
      else if (k === "html") e.innerHTML = v;
      else if (k.startsWith("on")) e.addEventListener(k.slice(2), v);
      else e.setAttribute(k, v === true ? "" : v);
    }
    for (const c of children.flat()) if (c !== null && c !== undefined && c !== false) e.append(c instanceof Node ? c : document.createTextNode(String(c)));
    return e;
  }

  function debounce(fn, ms = 250) {
    let t;
    return (...a) => {
      clearTimeout(t);
      t = setTimeout(() => fn(...a), ms);
    };
  }

  function kindOf(node) {
    for (let n = node; n; n = n.parent) if (n.meta && n.meta.kind && HUES[n.meta.kind] !== undefined) return n.meta.kind;
    return "unknown";
  }

  function hueOf(node) {
    for (let n = node; n; n = n.parent) {
      if (n.meta && n.meta.col !== undefined) return COL_HUES[n.meta.col % COL_HUES.length];
    }
    return HUES[kindOf(node)] ?? 220;
  }

  function size(node) {
    return node.end - node.start;
  }

  function rangeLabel(node) {
    const n = size(node);
    if (n === 0) return `${DF.hexAddr(node.start)} (0 B)`;
    return `${DF.hexAddr(node.start)}..${DF.hexAddr(node.end - 1)} (${n} B)`;
  }

  function ancestors(node) {
    const out = [];
    for (let n = node; n && n.parent; n = n.parent) out.unshift(n);
    return out;
  }

  /** Rows of boxes: the top level, then the children of whatever is selected, one row per level. */
  class StructureView {
    constructor(container, opts = {}) {
      this.c = container;
      this.opts = opts;
      this.expanded = new Set();
      this.maxBoxes = 40;
    }

    setRoot(root) {
      this.root = root;
      this.expanded.clear();
    }

    render(sel) {
      this.sel = sel;
      const path = sel ? ancestors(sel) : [];
      const rows = [];
      let parent = this.root;
      let depth = 0;
      while (parent && parent.children.length) {
        rows.push({ parent, onNode: path[depth] });
        parent = path[depth];
        depth++;
        if (!parent) break;
      }
      this.c.innerHTML = "";
      rows.forEach((row, ri) => {
        const title = ri === 0 ? `${row.parent.label}` : `${row.parent.label}${row.parent.meta?.type ? ` · ${row.parent.meta.type}` : ""}`;
        const wrap = el("div", { class: "srow-wrap" }, el("div", { class: "srow-title" }, title));
        const r = el("div", { class: "srow" });
        const key = DF.nodePath(row.parent).join(".");
        const kids = row.parent.children;
        const limit = this.expanded.has(key) ? kids.length : this.maxBoxes;
        let shown = kids.slice(0, limit);
        if (row.onNode && !shown.includes(row.onNode)) shown = shown.concat([row.onNode]);
        for (const n of shown) {
          const cls = ["sbox"];
          if (n === sel) cls.push("sel");
          else if (n === row.onNode) cls.push("on");
          const box = el("div", { class: cls.join(" "), title: `${n.label}\n${n.meta?.type || ""}\n${rangeLabel(n)}` },
            el("div", { class: "t" }, n.label),
            el("div", { class: "s" }, n.meta?.type ? DF.shorten(n.meta.type, 30) : `${size(n)} B`),
            n.meta?.value !== undefined && n.meta?.value !== "" ? el("div", { class: "v" }, DF.shorten(n.meta.value, 28)) : el("div", { class: "s" }, `${size(n)} B`));
          box.style.setProperty("--h", hueOf(n));
          box.addEventListener("click", () => this.opts.onSelect?.(n));
          box.addEventListener("mouseenter", () => this.opts.onHover?.(n));
          box.addEventListener("mouseleave", () => this.opts.onHover?.(null));
          r.append(box);
        }
        if (kids.length > limit) {
          r.append(el("div", { class: "sbox more", onclick: () => { this.expanded.add(key); this.render(this.sel); } }, `+${kids.length - limit} more`));
        }
        wrap.append(r);
        this.c.append(wrap);
      });
    }
  }

  /** Proportional bar of the file: top-level nodes, optionally split into their children. */
  class FileMap {
    constructor(container, opts = {}) {
      this.c = container;
      this.opts = opts;
      this.segs = [];
    }

    setRoot(root) {
      this.root = root;
      this.c.innerHTML = "";
      this.segs = [];
      const total = Math.max(1, size(root));
      const bar = el("div", { class: "fmap" });
      const labels = el("div", { class: "fmap-labels" });
      for (const top of root.children) {
        const grow = Math.max(size(top), total * 0.004);
        const split = top.children.length > 1 && top.children.length <= 64 && !this.opts.flat?.(top);
        if (split) {
          const grp = el("div", { class: "fgroup" });
          grp.style.flex = `${grow} 1 0`;
          for (const ch of top.children) grp.append(this.seg(ch, Math.max(size(ch), total * 0.002)));
          bar.append(grp);
        } else {
          bar.append(this.seg(top, grow));
        }
        const lab = el("div", { title: top.label }, top.label);
        lab.style.flex = `${grow} 1 0`;
        labels.append(lab);
      }
      this.c.append(bar, labels);
    }

    seg(node, grow) {
      const s = el("div", { class: "fseg leafseg", title: `${node.label}\n${rangeLabel(node)}` },
        el("div", { class: "fl" }, node.label),
        el("div", { class: "fs" }, DF.fmtBytes(size(node))));
      s.style.flex = `${grow} 1 0`;
      s.style.setProperty("--h", hueOf(node));
      s.addEventListener("click", () => this.opts.onSelect?.(node));
      this.segs.push({ node, el: s });
      return s;
    }

    render(sel) {
      for (const { node, el: e } of this.segs) {
        const on = sel && sel.start >= node.start && sel.end <= node.end && (sel !== this.root);
        e.classList.toggle("sel", !!on && (sel === node || ancestors(sel).includes(node)));
      }
    }

    overlay(ranges) {
      for (const { node, el: e } of this.segs) {
        e.classList.remove("read", "skip");
        if (!ranges) continue;
        const hit = ranges.some((r) => r.start < node.end && r.end > node.start);
        e.classList.add(hit ? "read" : "skip");
      }
    }
  }

  /** Hex dump with ASCII column. Only visible rows are in the DOM, so large files stay fast. */
  class HexView {
    constructor(container, opts = {}) {
      this.opts = opts;
      this.per = 16;
      this.rowH = 24;
      this.scroll = el("div", { class: "hex-scroll" });
      this.spacer = el("div", { class: "hex-spacer" });
      this.rows = el("div", { class: "hex-rows" });
      this.spacer.append(this.rows);
      this.scroll.append(this.spacer);
      container.append(this.scroll);
      this.scroll.addEventListener("scroll", () => this.paint());
      this.rows.addEventListener("click", (e) => {
        const i = e.target.dataset?.i;
        if (i !== undefined) this.opts.onByte?.(Number(i));
      });
      this.rows.addEventListener("mouseover", (e) => {
        const i = e.target.dataset?.i;
        if (i !== undefined && this.leafAt) this.hover(this.nodes[this.leafAt[Number(i)]]);
      });
      this.rows.addEventListener("mouseleave", () => this.hover(null));
      this.bytes = new Uint8Array(0);
      this.sel = null;
      this.hov = null;
      this.overlayRanges = null;
    }

    setData(bytes, root) {
      this.bytes = bytes;
      this.root = root;
      this.nodes = [];
      this.leafAt = new Int32Array(bytes.length).fill(-1);
      this.kindCls = [];
      for (const n of DF.walk(root)) {
        const idx = this.nodes.length;
        this.nodes.push(n);
        this.kindCls.push("k-" + kindOf(n));
        for (let i = n.start; i < n.end; i++) this.leafAt[i] = idx;
      }
      const contentH = Math.ceil(bytes.length / this.per) * this.rowH + 8;
      this.spacer.style.height = `${contentH}px`;
      this.scroll.style.height = `${Math.min(380, Math.max(110, contentH + 14))}px`;
      this.spacer.style.width = `${(this.per * 3.1 + this.per * 1.15 + 10)}ch`;
      this.paint(true);
    }

    leaf(i) {
      return this.nodes[this.leafAt[i]];
    }

    setSelection(node, scroll = true) {
      this.sel = node;
      this.paint(true);
      if (scroll && node && node !== this.root) this.scrollTo(node.start);
    }

    hover(node) {
      if (node === this.hov) return;
      this.hov = node;
      this.paint(true);
    }

    setOverlay(ranges) {
      this.overlayRanges = ranges;
      this.paint(true);
    }

    scrollTo(offset) {
      const row = Math.floor(offset / this.per);
      const top = row * this.rowH;
      const h = this.scroll.clientHeight;
      if (top < this.scroll.scrollTop || top > this.scroll.scrollTop + h - this.rowH * 2) {
        this.scroll.scrollTop = Math.max(0, top - h / 3);
      }
    }

    inOverlay(i) {
      if (!this.overlayRanges) return true;
      for (const r of this.overlayRanges) if (i >= r.start && i < r.end) return true;
      return false;
    }

    paint(force) {
      const first = Math.max(0, Math.floor(this.scroll.scrollTop / this.rowH) - 4);
      const count = Math.ceil((this.scroll.clientHeight || 360) / this.rowH) + 10;
      if (!force && first === this.lastFirst) return;
      this.lastFirst = first;
      const last = Math.min(Math.ceil(this.bytes.length / this.per), first + count);
      const sel = this.sel && this.sel !== this.root ? this.sel : null;
      const hov = this.hov && this.hov !== this.root ? this.hov : null;
      let html = "";
      for (let r = first; r < last; r++) {
        const off = r * this.per;
        let hx = "";
        let asc = "";
        for (let j = 0; j < this.per; j++) {
          const i = off + j;
          if (i >= this.bytes.length) {
            hx += `<span class="b none">00</span>`;
            continue;
          }
          const b = this.bytes[i];
          let cls = this.kindCls[this.leafAt[i]] || "k-unknown";
          if (sel && i >= sel.start && i < sel.end) cls += sel.children.length ? " selg" : " sel";
          else if (hov && i >= hov.start && i < hov.end) cls += " hov";
          else if (!this.inOverlay(i)) cls += " dim";
          hx += `<span class="b ${cls}" data-i="${i}">${b.toString(16).padStart(2, "0").toUpperCase()}</span>`;
          asc += `<span class="a ${cls}" data-i="${i}">${b >= 32 && b < 127 ? esc(String.fromCharCode(b)) : "·"}</span>`;
        }
        html += `<div class="hr"><span class="off">${off.toString(16).toUpperCase().padStart(4, "0")}:</span><span class="hx">${hx}</span><span class="asc">${asc}</span></div>`;
      }
      this.rows.style.transform = `translateY(${first * this.rowH}px)`;
      this.rows.innerHTML = html;
    }
  }

  function bitsHTML(bytes, node) {
    const m = node.meta;
    const parts = [];
    const groups = [];
    let html = '<div class="bits">';
    bytes.forEach((b, i) => {
      const bin = b.toString(2).padStart(8, "0");
      const alt = i % 2 ? " alt" : "";
      html += `<div class="bitbyte"><div class="bb"><span class="bit c">${bin[0]}</span>${[...bin.slice(1)].map((x) => `<span class="bit p${alt}">${x}</span>`).join("")}</div><div class="bx">${b.toString(16).padStart(2, "0").toUpperCase()}</div></div>`;
      groups.push(bin.slice(1));
    });
    html += "</div>";
    let raw = 0n;
    groups.forEach((g2, i) => (raw |= BigInt(parseInt(g2, 2)) << BigInt(7 * i)));
    parts.push(`<div class="bits-legend"><span class="bit c">1</span> continuation bit: 1 = another byte follows. <span class="bit p">0</span> 7 payload bits each.</div>`);
    const rev = groups.slice().reverse().join(" ");
    parts.push(`<div class="rule" style="margin-top:6px">payload groups, last byte first:\n${rev}\n= ${raw}</div>`);
    if (m.zigzag) {
      const v = DF.unzigzag(raw);
      parts.push(`<div class="rule" style="margin-top:6px">zigzag decode: (${raw} >> 1) ^ -(${raw} & 1) = ${v}</div>`);
    }
    if (m.tag) {
      parts.push(`<div class="rule" style="margin-top:6px">${raw} = 0b${raw.toString(2)}\nfield number = ${raw} >> 3 = ${raw >> 3n}\nwire type    = ${raw} & 7  = ${raw & 7n}</div>`);
    }
    return html + parts.join("");
  }

  function bitpackedHTML(bytes, bp) {
    const { bw, values } = bp;
    const n = Math.min(values.length, 16);
    let html = '<div class="bits">';
    const show = bytes.subarray(0, Math.ceil((n * bw) / 8));
    show.forEach((b, i) => {
      const bin = b.toString(2).padStart(8, "0");
      let cells = "";
      for (let k = 7; k >= 0; k--) {
        const bit = i * 8 + k;
        const vi = Math.floor(bit / bw);
        cells += `<span class="bit ${vi % 2 ? "alt" : "p"}" title="value ${vi}">${bin[7 - k]}</span>`;
      }
      html += `<div class="bitbyte"><div class="bb">${cells}</div><div class="bx">${b.toString(16).padStart(2, "0").toUpperCase()}</div></div>`;
    });
    html += "</div>";
    html += `<div class="bits-legend">Bits are filled from the least significant bit of each byte upward, ${bw} bit(s) per value, alternating colours per value. Decoded: ${values.slice(0, n).join(", ")}</div>`;
    return html;
  }

  class Inspector {
    constructor(container, opts = {}) {
      this.c = container;
      this.opts = opts;
    }

    show(node, bytes) {
      if (!node) {
        this.c.innerHTML = '<p class="muted">Click a byte or a box to inspect it.</p>';
        return;
      }
      const m = node.meta || {};
      const slice = bytes.subarray(node.start, node.end);
      const crumbs = ancestors(node);
      const out = [];
      out.push(`<div class="lbl2">Field name</div><div class="val">${esc(node.label)}</div>`);
      if (crumbs.length > 1) {
        out.push(`<div class="crumbs" style="margin-top:4px">${crumbs.map((n, i) => (i < crumbs.length - 1 ? `<a data-c="${i}">${esc(n.label)}</a>` : esc(n.label))).join(" › ")}</div>`);
      }
      if (m.type) out.push(`<div class="lbl2">Data type &amp; layout</div><div class="val">${esc(m.type)}</div>`);
      if (m.value !== undefined && m.value !== "") out.push(`<div class="lbl2">Value</div><div class="val">${esc(m.value)}</div>`);
      if (m.rule) out.push(`<div class="lbl2">Encoding rule mechanics</div><div class="rule">${esc(m.rule)}</div>`);
      if (m.varint && slice.length && slice.length <= 10 && !node.children.length) out.push(`<div class="lbl2">Varint bits</div>${bitsHTML(slice, node)}`);
      if (m.bitpacked) out.push(`<div class="lbl2">Bit-packed layout</div>${bitpackedHTML(slice, m.bitpacked)}`);
      if (m.desc) out.push(`<p class="desc">${esc(m.desc)}</p>`);
      if (slice.length) out.push(`<div class="lbl2">Bytes (${slice.length})</div><div class="val">${esc(DF.hex(slice, 48))}</div>`);
      this.c.innerHTML = out.join("");
      this.c.querySelectorAll(".crumbs a").forEach((a) => a.addEventListener("click", () => this.opts.onSelect?.(crumbs[Number(a.dataset.c)])));
    }
  }

  /** Wires the components together and keeps one selection across all of them. */
  class Explorer {
    constructor({ structure, hex, inspector, filemap, chip, chipName, mapFlat, onSelect: onSelectHook }) {
      this.onSelectHook = onSelectHook;
      this.chip = chip;
      this.chipName = chipName;
      const onSelect = (n) => this.select(n);
      const onHover = (n) => {
        this.hex?.hover(n);
      };
      this.structure = structure ? new StructureView(structure, { onSelect, onHover }) : null;
      this.filemap = filemap ? new FileMap(filemap, { onSelect, flat: mapFlat }) : null;
      this.hex = hex ? new HexView(hex, {
        onByte: (i) => {
          const leaf = this.hex.leaf(i);
          const s = this.sel;
          if (s && s !== this.root && i >= s.start && i < s.end && s.parent && s.parent !== this.root) {
            this.select(s.parent, false);
          } else {
            this.select(leaf, false);
          }
        },
      }) : null;
      this.inspector = inspector ? new Inspector(inspector, { onSelect }) : null;
    }

    load(result, keepSelection = true) {
      const prev = keepSelection && this.sel ? DF.nodePath(this.sel) : null;
      this.bytes = result.bytes;
      this.root = result.root;
      this.structure?.setRoot(this.root);
      this.filemap?.setRoot(this.root);
      this.hex?.setData(this.bytes, this.root);
      let sel = prev ? DF.nodeAt(this.root, prev) : null;
      if (!sel) {
        sel = this.root.children[0] || this.root;
        while (sel.children && sel.children.length && sel.end - sel.start > 0 && sel.children[0].end > sel.children[0].start) sel = sel.children[0];
      }
      this.select(sel, true);
    }

    select(node, scroll = true) {
      if (!node) return;
      this.sel = node;
      this.structure?.render(node);
      this.filemap?.render(node);
      this.hex?.setSelection(node, scroll);
      this.inspector?.show(node, this.bytes);
      if (this.chip) this.chip.textContent = rangeLabel(node);
      if (this.chipName) this.chipName.textContent = DF.shorten(node.label, 28);
      this.onSelectHook?.(node);
    }

    /** Select the deepest node that starts at offset and covers it. */
    selectOffset(offset) {
      if (!this.hex || offset < 0 || offset >= this.bytes.length) return;
      this.select(this.hex.leaf(offset), true);
    }
  }

  function legend(container, entries) {
    container.innerHTML = "";
    for (const [kind, label] of entries) {
      const sw = el("span", { class: `sw k-${kind}` });
      container.append(el("span", {}, sw, label));
    }
  }

  function themeToggle(button) {
    const key = "df-theme";
    const apply = (t) => {
      if (t) document.documentElement.setAttribute("data-theme", t);
      else document.documentElement.removeAttribute("data-theme");
    };
    try {
      apply(localStorage.getItem(key));
    } catch {}
    button.addEventListener("click", () => {
      const dark = document.documentElement.getAttribute("data-theme") === "dark" || (!document.documentElement.getAttribute("data-theme") && matchMedia("(prefers-color-scheme: dark)").matches);
      const next = dark ? "light" : "dark";
      apply(next);
      try {
        localStorage.setItem(key, next);
      } catch {}
    });
  }

  function sizeBars(container, items) {
    const max = Math.max(...items.map((i) => i.n), 1);
    container.innerHTML = "";
    const wrap = el("div", { class: "sizebars" });
    for (const it of items) {
      const fill = el("div", { class: it.alt ? "alt" : "" });
      fill.style.width = `${(it.n / max) * 100}%`;
      wrap.append(el("div", { class: "sizebar" }, el("span", {}, it.label), el("div", { class: "bar" }, fill), el("span", { class: "n" }, DF.fmtBytes(it.n))));
    }
    container.append(wrap);
  }

  function download(bytes, name) {
    const url = URL.createObjectURL(new Blob([bytes], { type: "application/octet-stream" }));
    const a = el("a", { href: url, download: name });
    document.body.append(a);
    a.click();
    a.remove();
    setTimeout(() => URL.revokeObjectURL(url), 1000);
  }

  function tabs(container, options, onChange, initial) {
    container.classList.add("tabs");
    container.innerHTML = "";
    const buttons = options.map(([value, label]) => {
      const b = el("button", { type: "button" }, label);
      b.addEventListener("click", () => {
        buttons.forEach((x) => x.classList.toggle("on", x === b));
        onChange(value);
      });
      container.append(b);
      return b;
    });
    const idx = Math.max(0, options.findIndex(([v]) => v === initial));
    buttons[idx].classList.add("on");
    return {
      set(value) {
        buttons.forEach((b, i) => b.classList.toggle("on", options[i][0] === value));
      },
    };
  }

  const STATUS_CHIP = {
    ok: ["ok", "ok"], renamed: ["ok", "renamed, ok"], alias: ["ok", "alias, ok"], promoted: ["ok", "promoted"],
    default: ["info", "default"], skipped: ["muted", "skipped"], silent: ["warn", "SILENT"],
    mismatch: ["warn", "wire type mismatch"], error: ["bad", "BREAKS"],
  };

  function statusChip(status) {
    const [cls, label] = STATUS_CHIP[status] || ["muted", status];
    return `<span class="chip ${cls}">${esc(label)}</span>`;
  }

  /**
   * Lanes of byte ranges over a window of the file, with a cursor. Used to put
   * the true record boundaries next to what a reader starting mid-file sees.
   */
  function boundaryStrip(container, { from, to, cursor, lanes, onPick, fmt }) {
    container.innerHTML = "";
    const span = Math.max(1, to - from);
    const pct = (x) => `${((Math.min(Math.max(x, from), to) - from) / span) * 100}%`;
    const wrap = el("div", { class: "bstrip" });
    for (const lane of lanes) {
      const track = el("div", { class: "btrack" });
      for (const s of lane.spans) {
        if (s.end <= from || s.start >= to) continue;
        const e = el("div", { class: `bspan ${s.cls || ""}`, title: s.title || "" }, s.label || "");
        e.style.left = pct(s.start);
        e.style.width = `calc(${pct(s.end)} - ${pct(s.start)})`;
        if (onPick) e.addEventListener("click", () => onPick(s.start));
        track.append(e);
      }
      if (cursor !== undefined) {
        const c = el("div", { class: "bcursor" });
        c.style.left = pct(cursor);
        track.append(c);
      }
      wrap.append(el("div", { class: "blabel" }, lane.label), track);
    }
    const f = fmt || ((x) => DF.hexAddr(x, 4));
    const axis = el("div", { class: "baxis" }, el("span", {}, f(from)), el("span", {}, f(to)));
    wrap.append(el("div", {}), axis);
    container.append(wrap);
  }

  /** Window + slider + number input for picking a start offset. */
  function offsetPicker(container, { max, value, onChange }) {
    container.innerHTML = "";
    const range = el("input", { type: "range", min: 0, max, value, style: "flex:1;min-width:160px" });
    const num = el("input", { type: "number", min: 0, max, value, style: "width:7em" });
    const rand = el("button", { class: "btn small", type: "button" }, "random offset");
    const set = (v) => {
      v = Math.max(0, Math.min(max, Math.round(Number(v) || 0)));
      range.value = v;
      num.value = v;
      onChange(v);
    };
    range.addEventListener("input", () => set(range.value));
    num.addEventListener("input", () => set(num.value));
    rand.addEventListener("click", () => set(Math.floor(Math.random() * max)));
    container.append(el("div", { class: "flex", style: "width:100%" }, el("span", { class: "small muted" }, "start at byte"), num, range, rand));
    return { set };
  }

  function repoLink(path, label = path) {
    return `<a href="${esc(DF.SRC.repoUrl(path))}" target="_blank" rel="noopener"><code>${esc(label)}</code></a>`;
  }

  /**
   * The numbered source list at the bottom of a page. Replaces every
   * <sup data-cite="id"> already in the document with a numbered link and
   * returns cite(id) for markup the page builds later.
   */
  function sources(container, ids, { code = [], verified = "" } = {}) {
    const S = DF.SRC.SOURCES;
    const num = new Map(ids.map((id, i) => [id, i + 1]));
    const cite = (id) => {
      if (!num.has(id)) throw new Error(`source '${id}' is not in this page's list`);
      return `<sup class="cite"><a href="#src-${id}" title="${esc(S[id].title)}">[${num.get(id)}]</a></sup>`;
    };
    document.querySelectorAll("sup[data-cite]").forEach((s) => {
      s.outerHTML = s.dataset.cite.split(" ").map(cite).join("");
    });
    const items = ids.map((id) => `<li id="src-${id}"><a href="${esc(S[id].url)}" target="_blank" rel="noopener">${esc(S[id].title)}</a>, ${esc(S[id].by)}. <span class="muted">Used for: ${esc(S[id].used)}.</span></li>`);
    const V = DF.SRC.VERIFY;
    container.innerHTML = `<div class="card-h"><h3>Sources</h3></div><ol class="refs">${items.join("")}</ol>` +
      `<p class="note">Everything else on this page is computed in your browser by ${code.map((p) => repoLink(`explorer/${p}`, p)).join(", ")}. ` +
      `${verified ? `${verified} Checked by ${repoLink(V.check)} on files written by ${repoLink(V.generate)}. ` : ""}` +
      `Code, benchmarks and verification: <a href="${esc(DF.SRC.REPO)}" target="_blank" rel="noopener">${esc(DF.SRC.REPO.replace(/^https:\/\//, ""))}</a>.</p>`;
    return cite;
  }

  /** Where a set of measured numbers comes from: script, raw results, dataset, method and what was not recorded. */
  function benchSource(container, key, bench) {
    const S = DF.SRC;
    const b = S.BENCH[key];
    const env = bench.results[key]?.env;
    const known = [];
    if (key === "05_random_rows" && bench.parquet) known.push(`Parquet file written by ${esc(bench.parquet.created_by)}`);
    if (key === "05_random_rows" && bench.lance) known.push(`Lance file format ${esc(bench.lance.storage_version)}`);
    const envText = env
      ? `Measured on: ${esc(Object.entries(env).map(([k, v]) => `${k}: ${v}`).join(", "))}.`
      : `${known.length ? `${known.join(", ")}. ` : ""}Machine, operating system and library versions were not recorded with this run, so read absolute times as one machine's numbers.`;
    container.innerHTML = `<div class="provenance">` +
      `<div><b>${esc(b.title)}.</b> Measured by ${repoLink(b.script)} on the dataset from ${repoLink(S.DATASET.script)}. Raw results in ${repoLink(b.results)}, copied into <code>data/bench.js</code> on ${esc(bench.generated)} by ${repoLink("explorer/data/export_bench.py")}.</div>` +
      `<details><summary>How this was measured</summary>` +
      `<p>${esc(S.DATASET.text)}</p><ul>${b.method.map((m) => `<li>${esc(m)}</li>`).join("")}</ul><p>${envText}</p>` +
      `<p>To reproduce, see ${repoLink("bench/README.md")}, then rerun ${repoLink("explorer/data/export_bench.py")}.</p></details></div>`;
  }

  Object.assign(UI, { el, esc, debounce, Explorer, legend, themeToggle, sizeBars, download, tabs, statusChip, COL_HUES, boundaryStrip, offsetPicker, repoLink, sources, benchSource });
})(globalThis);
