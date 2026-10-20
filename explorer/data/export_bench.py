# /// script
# requires-python = ">=3.11"
# dependencies = ["pyarrow", "pylance"]
# ///
"""Export the layout of the bench files and the bench results to bench.js.

The Lance and Arrow explorers simulate lookups on the real 10M-row files from
bench/05 and show the measured numbers from bench/05 and bench/06. Run after
the benchmarks: uv run explorer/data/export_bench.py
"""

import json
import re
from datetime import date
from pathlib import Path

import lance
import pyarrow.parquet as pq
from lance.file import LanceFileReader

ROOT = Path(__file__).resolve().parents[2]
BENCH = ROOT / "bench"
OUT = Path(__file__).with_name("bench.js")


def parquet_layout(path: Path) -> dict:
    """Row groups and column chunks with offsets and sizes, from the footer."""
    md = pq.ParquetFile(path).metadata
    groups = []
    for i in range(md.num_row_groups):
        rg = md.row_group(i)
        cols = []
        for j in range(rg.num_columns):
            c = rg.column(j)
            start = c.dictionary_page_offset if c.dictionary_page_offset is not None else c.data_page_offset
            cols.append([start, c.total_compressed_size, c.total_uncompressed_size])
        groups.append({"rows": rg.num_rows, "cols": cols})
    first = md.row_group(0).column(0)
    return {
        "bytes": path.stat().st_size,
        "rows": md.num_rows,
        "created_by": md.created_by,
        "codec": first.compression,
        "has_page_index": first.has_offset_index,
        "columns": [md.schema.column(j).name for j in range(md.num_columns)],
        "row_groups": groups,
    }


def lance_layout(path: Path) -> dict:
    """Per data file, per column, per page: layout kind, item count and buffers."""
    ds = lance.dataset(path)
    files = []
    for frag in ds.get_fragments():
        for df in frag.data_files():
            p = path / "data" / df.path
            md = LanceFileReader(str(p)).metadata()
            cols = []
            for c in md.columns:
                pages = []
                for pg in c.pages:
                    enc = re.sub(r"\s+", " ", pg.encoding)
                    kind = re.search(r"(MiniBlockLayout|FullZipLayout|ConstantLayout|AllNullLayout)", enc)
                    items = re.search(r"num_items: (\d+)", enc)
                    pages.append({
                        "layout": kind.group(1) if kind else "unknown",
                        "items": int(items.group(1)) if items else None,
                        "dictionary": "dictionary: Some" in enc,
                        "buffers": [[b.position, b.size] for b in pg.buffers],
                    })
                cols.append(pages)
            files.append({"rows": md.num_rows, "bytes": p.stat().st_size, "version": f"{md.major_version}.{md.minor_version}", "columns": cols})
    return {
        "bytes": sum(f.stat().st_size for f in path.rglob("*") if f.is_file()),
        "rows": ds.count_rows(),
        "storage_version": ds.data_storage_version,
        "fields": [{"name": f.name(), "id": f.id()} for f in ds.lance_schema.fields()],
        "files": files,
    }


def main() -> None:
    results = {name: json.loads((BENCH / "results" / f"{name}.json").read_text()) for name in ["05_random_rows", "06_load"]}
    data = {
        "generated": date.today().isoformat(),
        "results": results,
        "parquet": parquet_layout(BENCH / "data" / "rows.parquet"),
        "lance": lance_layout(BENCH / "data" / "rows.lance"),
    }
    OUT.write_text("globalThis.DF_BENCH = " + json.dumps(data, separators=(",", ":")) + ";\n")
    print(f"wrote {OUT} ({OUT.stat().st_size / 1024:.1f} KiB)")


if __name__ == "__main__":
    main()
