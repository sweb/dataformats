"""Iceberg metadata vs. data over one week of one commit per minute, no
maintenance, from the explorer's growth model. Light theme.

    uv run bench/plot_iceberg_growth.py
"""

import json
import subprocess

import matplotlib

matplotlib.use("Agg")
import matplotlib.pyplot as plt

from common import BENCH, RESULTS

C = dict(surface="#fcfcfb", ink="#0b0b0b", ink2="#52514e", muted="#898781",
         grid="#e1e0d9", base="#c3c2b7", data="#2a78d6", meta="#eb6834")

GIB = 1024**3
MIB = 1024**2


def model(rows: int = 100, commits: int = 10080) -> dict:
    out = subprocess.run(["node", str(BENCH / "iceberg_growth.cjs"), str(rows), str(commits)],
                         check=True, capture_output=True, text=True).stdout
    return json.loads(out)


def de(v: float, digits: int = 0) -> str:
    """German number formatting: 10.080 and 31,3."""
    s = f"{v:,.{digits}f}"
    return s.replace(",", "_").replace(".", ",").replace("_", ".")


def plot(m: dict, out) -> None:
    s = m["series"]
    days = [p["n"] / 1440 for p in s]
    meta = [(p["manifest"] + p["list"] + p["metadata"]) / GIB for p in s]
    data = [p["data"] / GIB for p in s]
    last = s[-1]
    meta_files = last["files"]["manifest"] + last["files"]["list"] + last["files"]["metadata"]
    data_files = last["files"]["data"]

    plt.rcParams.update({"font.family": "sans-serif", "font.size": 13})
    fig, ax = plt.subplots(figsize=(13, 6.8), dpi=200)
    fig.patch.set_facecolor(C["surface"])
    ax.set_facecolor(C["surface"])

    ax.fill_between(days, meta, color=C["meta"], alpha=0.12, linewidth=0, zorder=1)
    ax.plot(days, meta, color=C["meta"], linewidth=4, zorder=3, solid_capstyle="round")
    ax.plot(days, data, color=C["data"], linewidth=5, zorder=4, solid_capstyle="round")

    ymax = max(meta)
    ax.annotate(f"Metadaten: {de(ymax, 1)} GiB, {de(meta_files)} Dateien",
                xy=(days[-1], meta[-1]), xytext=(-24, 0), textcoords="offset points",
                ha="right", va="center", color=C["ink"], fontsize=26, fontweight="bold")
    ax.annotate(f"Daten: {de(last['data'] / MIB)} MiB, {de(data_files)} Dateien",
                xy=(days[-1], data[-1]), xytext=(-4, 12), textcoords="offset points",
                ha="right", va="bottom", color=C["ink"], fontsize=26, fontweight="bold")
    ax.plot([days[-1]], [meta[-1]], "o", ms=14, color=C["meta"], mec=C["surface"], mew=3, zorder=5)
    ax.plot([days[-1]], [data[-1]], "o", ms=14, color=C["data"], mec=C["surface"], mew=3, zorder=5)

    ax.set_xlim(0, 7.08)
    ax.set_ylim(0, ymax * 1.06)
    ax.set_xticks(range(8))
    ax.set_yticks(range(0, int(ymax) + 1, 10))
    ax.set_xlabel("Tage", color=C["ink2"], fontsize=24, labelpad=10)
    ax.set_ylabel("GiB auf dem Object Storage", color=C["ink2"], fontsize=24, labelpad=14)
    ax.yaxis.set_major_formatter(lambda v, _: de(v))
    ax.tick_params(colors=C["ink2"], labelsize=22, length=0, pad=8)
    ax.grid(axis="y", color=C["grid"], linewidth=1.2, zorder=0)
    for side in ("top", "right", "left"):
        ax.spines[side].set_visible(False)
    ax.spines["bottom"].set_color(C["base"])

    fig.subplots_adjust(left=0.1, right=0.98, top=0.96, bottom=0.15)
    fig.savefig(out, facecolor=C["surface"])
    plt.close(fig)


if __name__ == "__main__":
    m = model()
    (RESULTS / "iceberg_growth.json").write_text(json.dumps(m))
    plot(m, RESULTS / "iceberg_growth_light.png")
    print(RESULTS / "iceberg_growth_light.png")
