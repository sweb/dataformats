"""Shared plumbing for the measurement rig: paths, the canonical dataset, and
a filesystem that counts every byte a reader asks for.

The dataset is 10M rows of sensor telemetry, shaped so each mechanism has
something to bite on:

  device_id         int32     high cardinality (50k devices)
  timestamp         ts[us]    high cardinality, arrives in time order
  location          string    4 values, skewed        -> dictionary, partitioning
  firmware_version  string    6 values, skewed        -> dictionary
  status            string    3 values, 90/8/2        -> dictionary + RLE
  temp_c            float32   uniform 15..45, 2 dp    -> resists compression, honestly
"""

import json
import os
import platform
import time
from datetime import datetime
from importlib import metadata
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
from pyarrow.fs import FSSpecHandler, PyFileSystem

BENCH = Path(__file__).resolve().parent
DATA = BENCH / "data"
RESULTS = BENCH / "results"
CANONICAL = DATA / "telemetry.parquet"

LOCATIONS = ["hall-a", "hall-b", "yard", "cold-storage"]
LOCATION_WEIGHTS = [0.4, 0.3, 0.2, 0.1]
FIRMWARE = ["2.3.0", "2.3.1", "2.4.0", "2.4.1", "2.5.0", "3.0.0-rc1"]
FIRMWARE_WEIGHTS = [0.05, 0.10, 0.20, 0.45, 0.18, 0.02]
STATUSES = ["ok", "warn", "fault"]
STATUS_WEIGHTS = [0.90, 0.08, 0.02]

SCHEMA = pa.schema([
    ("device_id", pa.int32()),
    ("timestamp", pa.timestamp("us")),
    ("location", pa.string()),
    ("firmware_version", pa.string()),
    ("status", pa.string()),
    ("temp_c", pa.float32()),
])


def load(rows: int | None = None) -> pa.Table:
    if not CANONICAL.exists():
        raise SystemExit(f"{CANONICAL} missing. Run generate.py first.")
    t = pq.read_table(CANONICAL)
    return t.slice(0, rows) if rows else t


def fmt_bytes(n: float) -> str:
    for unit in ("B", "KiB", "MiB", "GiB", "TiB"):
        if abs(n) < 1024:
            return f"{n:,.1f} {unit}" if unit != "B" else f"{n:,.0f} B"
        n /= 1024
    return f"{n:,.1f} PiB"


PACKAGES = ["pyarrow", "pylance", "pandas", "numpy", "fastavro", "protobuf", "pyiceberg", "confluent-kafka", "zstandard"]


def _cpu_model() -> str:
    try:
        with open("/proc/cpuinfo") as f:
            for line in f:
                if line.startswith("model name"):
                    return line.split(":", 1)[1].strip()
    except OSError:
        pass
    return platform.processor() or platform.machine()


def _mem_total() -> int:
    with open("/proc/meminfo") as f:
        for line in f:
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    raise RuntimeError("no MemTotal in /proc/meminfo")


def environment() -> dict:
    """What a result was measured on: time, machine and library versions. No hostname or user."""
    env = {
        "measured": datetime.now().astimezone().isoformat(timespec="seconds"),
        "cpu": _cpu_model(),
        "cores": str(os.cpu_count()),
        "memory": fmt_bytes(_mem_total()),
        "os": f"{platform.system()} {platform.release()}",
        "python": platform.python_version(),
    }
    for pkg in PACKAGES:
        try:
            env[pkg] = metadata.version(pkg)
        except metadata.PackageNotFoundError:
            pass
    return env


def save_result(name: str, payload: dict, partial: bool = False) -> None:
    """A --rows run writes NN.partial.json so it cannot clobber the full result.
    Every result carries an "env" block saying what it was measured on."""
    RESULTS.mkdir(exist_ok=True)
    out = RESULTS / (f"{name}.partial.json" if partial else f"{name}.json")
    out.write_text(json.dumps({**payload, "env": environment()}, indent=2, default=str) + "\n")
    print(f"\n-> {out.relative_to(BENCH.parent)}")


def warm_cache(*paths) -> None:
    """Pull files into the page cache without holding them in Python memory."""
    for path in paths:
        files = [p for p in path.rglob("*") if p.is_file()] if path.is_dir() else [path]
        for f in files:
            with open(f, "rb") as fh:
                while fh.read(64 * 2**20):
                    pass


class Timer:
    """Wall seconds and CPU seconds. CPU is summed over all threads of the
    process, so a multithreaded reader can show more CPU than wall; that is the
    number a compute bill is proportional to."""

    def __enter__(self):
        self.t0 = time.perf_counter()
        self.c0 = time.process_time()
        return self

    def __exit__(self, *_):
        self.seconds = time.perf_counter() - self.t0
        self.cpu = time.process_time() - self.c0


# ------------------------------------------------------- byte-counting reads
#
# Object storage bills and throttles on bytes fetched, not on bytes "in the
# table". A Parquet reader on S3 issues range GETs for the footer, then for
# exactly the column chunks it needs. This wraps the local filesystem so those
# same reads can be counted without a bucket in the loop.

class CountingFS:
    def __init__(self):
        import fsspec
        self.bytes_read = 0
        self.requests = 0
        self._fs = fsspec.filesystem("file")
        outer = self

        class Counted:
            def __init__(self, f):
                self._f = f

            def read(self, n=-1):
                b = self._f.read(n)
                outer.bytes_read += len(b)
                outer.requests += 1
                return b

            def __getattr__(self, name):
                return getattr(self._f, name)

            def __enter__(self):
                return self

            def __exit__(self, *a):
                self._f.close()

        class Handler(FSSpecHandler):
            def open_input_file(self, path):
                return pa.PythonFile(Counted(outer._fs.open(path, "rb")), mode="r")

            def open_input_stream(self, path, compression=None, buffer_size=None):
                return pa.PythonFile(Counted(outer._fs.open(path, "rb")), mode="r")

        self.arrow_fs = PyFileSystem(Handler(self._fs))

    def reset(self):
        self.bytes_read = 0
        self.requests = 0


# ------------------------------------------------------------- memory guard
#
# 10M rows is small on disk and large in Python objects. A JSON parse of the
# full dataset peaks well above the file size, and this rig once took the
# whole machine down with it. Two defences, both cheap:
#
#   1. refuse to start unless MemAvailable covers the script's budget plus a
#      reserve for everything else on the machine
#   2. a watchdog thread that kills this process, not the machine, if RSS
#      ever exceeds the budget

def _mem_available() -> int:
    with open("/proc/meminfo") as f:
        for line in f:
            if line.startswith("MemAvailable:"):
                return int(line.split()[1]) * 1024
    raise RuntimeError("no MemAvailable in /proc/meminfo")


def _rss() -> int:
    with open("/proc/self/statm") as f:
        return int(f.read().split()[1]) * 4096


def guard(full_gib: float, rows: int | None = None, reserve_gib: float = 4.0) -> None:
    """`full_gib` is the budget for the full 10M rows; a --rows run scales it down."""
    import os
    import sys
    import threading

    budget_gib = full_gib if rows is None else min(full_gib, 1 + full_gib * rows / 10_000_000)
    budget = int(budget_gib * 2**30)
    avail = _mem_available()
    need = budget + int(reserve_gib * 2**30)
    if avail < need:
        sys.exit(f"refusing to start: this script budgets {budget_gib:.0f} GiB and wants "
                 f"{reserve_gib:.0f} GiB left for the rest of the machine, but only "
                 f"{avail / 2**30:.1f} GiB is available. Free ~{(need - avail) / 2**30:.1f} GiB "
                 f"or run with --rows N.")

    def watch():
        import time
        peak = 0
        while True:
            rss = _rss()
            peak = max(peak, rss)
            if rss > budget:
                sys.stderr.write(f"\nmemory guard: RSS {rss / 2**30:.2f} GiB exceeded the budget of "
                                 f"{budget_gib:.0f} GiB, aborting before the machine does.\n")
                sys.stderr.flush()
                os._exit(137)
            time.sleep(0.2)

    threading.Thread(target=watch, daemon=True).start()
    print(f"memory guard: budget {budget_gib:.0f} GiB, {avail / 2**30:.0f} GiB available")
