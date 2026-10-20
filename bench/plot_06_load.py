"""Bar chart of bench/06 (10M rows into memory): CPU and wall
seconds per path, light and dark variant.

    .venv/bin/python bench/plot_06_load.py
"""

import json

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from common import RESULTS

LABELS = {
    "JSON -> pandas (chunked)": "JSON → pandas\npd.read_json",
    "JSON -> Arrow (pyarrow.json, multithreaded)": "JSON → Arrow\npyarrow.json",
    "Parquet -> Arrow": "Parquet → Arrow\npq.read_table",
    "  Arrow -> pandas (ArrowDtype, no copy)": "Arrow → pandas\nzero-copy",
}

THEMES = {
    "light": dict(surface="#fcfcfb", ink="#0b0b0b", ink2="#52514e", muted="#898781",
                  grid="#e1e0d9", base="#c3c2b7", cpu="#2a78d6", wall="#eb6834"),
    "dark": dict(surface="#1a1a19", ink="#ffffff", ink2="#c3c2b7", muted="#898781",
                 grid="#2c2c2a", base="#383835", cpu="#3987e5", wall="#d95926"),
}


def fmt_s(v: float) -> str:
    if v >= 10:
        return f"{v:.1f} s"
    if v >= 0.1:
        return f"{v:.2f} s"
    if v >= 0.01:
        return f"{v:.3f} s"
    return f"{v:.4f} s"


def plot(r: dict, mode: str, out) -> None:
    c = THEMES[mode]
    items = [(LABELS[k], v) for k, v in r["results"].items() if k in LABELS]
    n = len(items)
    h = 0.36
    gap = 0.04
    xmax = max(max(v["cpu_s"], v["seconds"]) for _, v in items)

    plt.rcParams.update({"font.family": "sans-serif", "font.size": 13})
    fig, ax = plt.subplots(figsize=(13, 6.8), dpi=200)
    fig.patch.set_facecolor(c["surface"])
    ax.set_facecolor(c["surface"])

    for i, (label, v) in enumerate(items):
        y = n - 1 - i
        y_cpu = y + (h + gap) / 2
        y_wall = y - (h + gap) / 2
        ax.barh(y_cpu, v["cpu_s"], height=h, color=c["cpu"], zorder=2)
        ax.barh(y_wall, v["seconds"], height=h, color=c["wall"], zorder=2)
        pad = xmax * 0.008
        ax.text(v["cpu_s"] + pad, y_cpu, fmt_s(v["cpu_s"]), va="center", ha="left",
                color=c["ink"], fontsize=12, fontweight="bold")
        wall_txt = fmt_s(v["seconds"])
        cores = v["cpu_s"] / v["seconds"]
        if cores > 4:
            wall_txt += f"   ({cores:.0f} cores busy)"
        ax.text(v["seconds"] + pad, y_wall, wall_txt, va="center", ha="left",
                color=c["ink2"], fontsize=12)

    ax.set_yticks(range(n))
    ax.set_yticklabels([label for label, _ in reversed(items)], color=c["ink"], fontsize=13)
    ax.set_xlim(0, xmax * 1.18)
    ax.set_ylim(-0.6, n - 0.4)
    ax.set_xlabel("seconds", color=c["muted"], fontsize=12)
    ax.tick_params(axis="x", colors=c["muted"], labelsize=11, length=0)
    ax.tick_params(axis="y", length=0, pad=10)
    ax.grid(axis="x", color=c["grid"], linewidth=0.8, zorder=0)
    for side in ("top", "right", "bottom"):
        ax.spines[side].set_visible(False)
    ax.spines["left"].set_color(c["base"])

    rows = f"{r['rows']:,}".replace(",", " ")
    fig.text(0.015, 0.955, f"{rows} rows into memory", color=c["ink"], fontsize=20,
             fontweight="bold", ha="left", va="top")
    fig.text(0.015, 0.895,
             f"JSON lines {r['jsonl_bytes'] / 2**30:.1f} GiB, Parquet (zstd) {r['parquet_bytes'] / 2**20:.0f} MiB, "
             "page cache warm. CPU summed over all threads.",
             color=c["ink2"], fontsize=12, ha="left", va="top")

    handles = [plt.Rectangle((0, 0), 1, 1, color=c["cpu"]), plt.Rectangle((0, 0), 1, 1, color=c["wall"])]
    leg = fig.legend(handles, ["CPU time", "Wall clock"], loc="upper right", bbox_to_anchor=(0.985, 0.965),
                     ncol=2, frameon=False, fontsize=12, handlelength=1.2, handleheight=1.2)
    for t in leg.get_texts():
        t.set_color(c["ink2"])

    fig.tight_layout(rect=(0, 0, 1, 0.87))
    fig.savefig(out, facecolor=c["surface"])
    plt.close(fig)
    print(f"-> {out.relative_to(RESULTS.parent.parent)}")


if __name__ == "__main__":
    r = json.loads((RESULTS / "06_load.json").read_text())
    for mode in THEMES:
        plot(r, mode, RESULTS / f"06_load_{mode}.png")
