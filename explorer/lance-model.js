"use strict";

/**
 * Random row access on the real bench/05 files, Parquet vs Lance, computed
 * from their layout metadata (exported by data/export_bench.py).
 *
 * Parquet: the unit a reader can decode is a page; pyarrow's read_row_group
 * decodes a whole row group. Lance 2.1 mini-block layout: values are stored in
 * small chunks, and a per-page chunk table maps a row to its chunk. The chunk
 * count is inferred from the chunk table size (2 bytes per chunk), which
 * matches 1024 values per chunk for every page in the bench file.
 */
(function (g) {
  const DF = g.DF || (g.DF = {});
  const LM = (DF.LM = {});

  function build(bench) {
    const pq = bench.parquet;
    let row = 0;
    const groups = pq.row_groups.map((rg, i) => {
      const gr = {
        index: i, start: row, rows: rg.rows,
        cols: rg.cols.map(([offset, bytes, raw]) => ({ offset, bytes, raw })),
      };
      row += rg.rows;
      return gr;
    });
    const parquet = { rows: pq.rows, bytes: pq.bytes, columns: pq.columns, groups, codec: pq.codec, hasPageIndex: pq.has_page_index };

    const lc = bench.lance;
    let frow = 0;
    const files = lc.files.map((f, fi) => {
      const cols = f.columns.map((pages) => {
        let prow = 0;
        return pages.map((p) => {
          const [meta, data, dict] = p.buffers.map((b) => b[1]);
          const chunks = Math.max(1, Math.round(meta / 2));
          const page = {
            start: prow, items: p.items, chunks,
            perChunk: Math.ceil(p.items / chunks), chunkBytes: data / chunks,
            metaBytes: meta, dictBytes: p.dictionary ? dict || 0 : 0,
          };
          prow += p.items;
          return page;
        });
      });
      const file = { index: fi, start: frow, rows: f.rows, cols };
      frow += f.rows;
      return file;
    });
    const lance = { rows: lc.rows, bytes: lc.bytes, version: lc.storage_version, fields: lc.fields, files };
    return { parquet, lance };
  }

  function locateGroup(parquet, id) {
    let lo = 0;
    let hi = parquet.groups.length - 1;
    while (lo < hi) {
      const mid = (lo + hi + 1) >> 1;
      if (parquet.groups[mid].start <= id) lo = mid;
      else hi = mid - 1;
    }
    return parquet.groups[lo];
  }

  function locateLance(lance, id, col) {
    let file = lance.files[0];
    for (const f of lance.files) if (f.start <= id) file = f;
    const local = id - file.start;
    const pages = file.cols[col];
    let pi = 0;
    for (let i = 0; i < pages.length; i++) if (pages[i].start <= local) pi = i;
    const page = pages[pi];
    const chunk = Math.floor((local - page.start) / page.perChunk);
    const rowStart = file.start + page.start + chunk * page.perChunk;
    return { file, page, pi, chunk, rowStart, rowEnd: Math.min(rowStart + page.perChunk, file.start + page.start + page.items) };
  }

  /**
   * ids: row ids. mode "point" = one lookup per id with nothing cached in
   * between (bench/05 "point"), "batch" = one call for all ids.
   * parquetUnit "rowgroup" = pyarrow read_row_group (all columns, what
   * bench/05 does), "chunk" = only the selected columns' chunks.
   */
  function simulate(model, ids, { mode = "point", columns, parquetUnit = "rowgroup" } = {}) {
    const cols = columns ?? model.parquet.columns.map((_, i) => i);
    const pqCols = parquetUnit === "rowgroup" ? model.parquet.columns.map((_, i) => i) : cols;
    const pqSeen = new Map();
    const pq = { units: 0, bytes: 0, values: 0, touched: new Map() };
    for (const id of ids) {
      const gr = locateGroup(model.parquet, id);
      for (const c of pqCols) {
        const key = `${gr.index}/${c}`;
        if (mode === "batch" && pqSeen.has(key)) continue;
        pqSeen.set(key, true);
        const ch = gr.cols[c];
        pq.units++;
        pq.bytes += ch.bytes;
        pq.values += gr.rows;
      }
      pq.touched.set(gr.index, (pq.touched.get(gr.index) || 0) + 1);
    }
    const lSeen = new Set();
    const dictSeen = new Set();
    const ln = { units: 0, bytes: 0, values: 0, dictBytes: 0, touched: [] };
    for (const id of ids) {
      for (const c of cols) {
        const loc = locateLance(model.lance, id, c);
        const key = `${loc.file.index}/${c}/${loc.pi}/${loc.chunk}`;
        if (mode === "batch" && lSeen.has(key)) continue;
        lSeen.add(key);
        ln.units++;
        ln.bytes += loc.page.chunkBytes;
        ln.values += loc.rowEnd - loc.rowStart;
        ln.touched.push({ col: c, start: loc.rowStart, end: loc.rowEnd });
        const dk = `${loc.file.index}/${c}/${loc.pi}`;
        if (loc.page.dictBytes && !dictSeen.has(dk)) {
          dictSeen.add(dk);
          ln.dictBytes += loc.page.dictBytes;
        }
      }
    }
    ln.bytes += ln.dictBytes;
    return { parquet: pq, lance: ln, rows: ids.length, columns: cols };
  }

  function randomIds(n, total, seed) {
    const r = DF.rng(seed);
    const seen = new Set();
    while (seen.size < Math.min(n, total)) seen.add(Math.floor(r.next() * total));
    return [...seen];
  }

  Object.assign(LM, { build, simulate, locateGroup, locateLance, randomIds });
})(globalThis);
