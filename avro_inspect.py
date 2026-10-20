# /// script
# requires-python = ">=3.11"
# dependencies = ["fastavro"]
# ///
"""Encode the talk payload as Avro and print an annotated byte breakdown."""

import argparse
import io
import json
from pathlib import Path

import fastavro
from fastavro.schema import to_parsing_canonical_form

SCHEMA = {
    "namespace": "conference.avro",
    "type": "record",
    "name": "Talk",
    "fields": [
        {"name": "id", "type": "int"},
        {"name": "title", "type": "string"},
        {"name": "description", "type": "string"},
    ],
}

TALK = {
    "id": 1,
    "title": "A data odyssey",
    "description": "We are going to talk about data formats...",
}

CRC64_EMPTY = 0xC15D213AA4D7A795
BYTES_PER_LINE = 16


def crc64_avro(data: bytes) -> int:
    """CRC-64-AVRO fingerprint as defined in the Avro specification."""
    table = []
    for i in range(256):
        fp = i
        for _ in range(8):
            fp = (fp >> 1) ^ (CRC64_EMPTY & -(fp & 1))
        table.append(fp)
    fp = CRC64_EMPTY
    for b in data:
        fp = (fp >> 8) ^ table[(fp ^ b) & 0xFF]
    return fp


def read_long(buf: bytes, pos: int) -> tuple[int, int]:
    """Decode a zigzag varint starting at pos, return (value, new_pos)."""
    shift = 0
    raw = 0
    while True:
        b = buf[pos]
        raw |= (b & 0x7F) << shift
        pos += 1
        if not b & 0x80:
            break
        shift += 7
    return (raw >> 1) ^ -(raw & 1), pos


def row(buf: bytes, start: int, end: int, meaning: str) -> None:
    """Print bytes buf[start:end] as offset, hex and meaning, wrapping long runs."""
    chunk = buf[start:end]
    for i in range(0, max(len(chunk), 1), BYTES_PER_LINE):
        part = chunk[i : i + BYTES_PER_LINE].hex(" ")
        label = meaning if i == 0 else ""
        print(f"  {start + i:06x}  {part:<{BYTES_PER_LINE * 3}} {label}")


def shorten(text: str, limit: int = 50) -> str:
    """Truncate text for the meaning column."""
    return repr(text) if len(text) <= limit else repr(text[:limit]) + "..."


def annotate_datum(buf: bytes, pos: int, schema: dict) -> int:
    """Annotate one record encoded with the given flat record schema, return new pos."""
    for field in schema["fields"]:
        name, ftype = field["name"], field["type"]
        start = pos
        if ftype in ("int", "long"):
            value, pos = read_long(buf, pos)
            row(buf, start, pos, f"{name}: {ftype} = {value} (zigzag varint)")
        elif ftype in ("string", "bytes"):
            length, pos = read_long(buf, pos)
            row(buf, start, pos, f"{name}: length = {length} (zigzag varint)")
            payload = buf[pos : pos + length]
            shown = shorten(payload.decode()) if ftype == "string" else f"{length} bytes"
            row(buf, pos, pos + length, f"{name}: {shown}")
            pos += length
        else:
            raise ValueError(f"annotation not implemented for type {ftype!r}")
    return pos


def annotate_ocf(buf: bytes, schema: dict) -> None:
    """Annotate header, metadata map, sync marker and data blocks of an OCF."""
    row(buf, 0, 4, f"magic {buf[:4]!r}")
    pos = 4
    codec = "null"
    while True:
        start = pos
        count, pos = read_long(buf, pos)
        if count == 0:
            row(buf, start, pos, "metadata map: end")
            break
        if count < 0:
            count = -count
            _, pos = read_long(buf, pos)
        row(buf, start, pos, f"metadata map: block of {count} entries")
        for _ in range(count):
            start = pos
            klen, pos = read_long(buf, pos)
            row(buf, start, pos, f"key length = {klen}")
            key = buf[pos : pos + klen].decode()
            row(buf, pos, pos + klen, f"key {key!r}")
            pos += klen
            start = pos
            vlen, pos = read_long(buf, pos)
            row(buf, start, pos, f"value length = {vlen}")
            value = buf[pos : pos + vlen]
            row(buf, pos, pos + vlen, f"value {shorten(value.decode())}")
            pos += vlen
            if key == "avro.codec":
                codec = value.decode()
    sync = buf[pos : pos + 16]
    row(buf, pos, pos + 16, "sync marker (random, per file)")
    pos += 16

    block = 0
    while pos < len(buf):
        print(f"  --- block {block} ---")
        start = pos
        records, pos = read_long(buf, pos)
        row(buf, start, pos, f"record count = {records}")
        start = pos
        size, pos = read_long(buf, pos)
        row(buf, start, pos, f"block size = {size} bytes")
        end = pos + size
        if codec == "null":
            for _ in range(records):
                pos = annotate_datum(buf, pos, schema)
        else:
            row(buf, pos, end, f"{records} records, {codec}-compressed")
            pos = end
        marker = buf[pos : pos + 16]
        status = "matches header" if marker == sync else "MISMATCH"
        row(buf, pos, pos + 16, f"sync marker ({status})")
        pos += 16
        block += 1


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--out-dir", type=Path, default=Path("avro-out"))
    parser.add_argument("--codec", default="null", choices=["null", "deflate"])
    parser.add_argument("--records", type=int, default=1, help="copies of the talk in the OCF")
    args = parser.parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)

    parsed = fastavro.parse_schema(SCHEMA)

    datum_buf = io.BytesIO()
    fastavro.schemaless_writer(datum_buf, parsed, TALK)
    datum = datum_buf.getvalue()

    canonical = to_parsing_canonical_form(parsed)
    fingerprint = crc64_avro(canonical.encode())
    soe = b"\xc3\x01" + fingerprint.to_bytes(8, "little") + datum

    ocf_buf = io.BytesIO()
    fastavro.writer(ocf_buf, parsed, [TALK] * args.records, codec=args.codec)
    ocf = ocf_buf.getvalue()

    json_bytes = json.dumps(TALK, separators=(",", ":")).encode()

    files = {"talk.bin": datum, "talk.soe": soe, "talk.avro": ocf}
    for name, content in files.items():
        (args.out_dir / name).write_bytes(content)

    print("== Sizes ==")
    print(f"  JSON (minified)        {len(json_bytes):>6} bytes")
    print(f"  Avro datum             {len(datum):>6} bytes  {args.out_dir / 'talk.bin'}")
    print(f"  Single-object encoding {len(soe):>6} bytes  {args.out_dir / 'talk.soe'}")
    print(f"  Object Container File  {len(ocf):>6} bytes  {args.out_dir / 'talk.avro'}")

    print("\n== Avro datum (no schema, no tags) ==")
    annotate_datum(datum, 0, SCHEMA)

    print("\n== Single-object encoding ==")
    print(f"  Parsing Canonical Form: {canonical}")
    row(soe, 0, 2, "marker C3 01")
    row(soe, 2, 10, f"CRC-64-AVRO fingerprint 0x{fingerprint:016x} (little endian)")
    annotate_datum(soe, 10, SCHEMA)

    print(f"\n== Object Container File (codec={args.codec}, records={args.records}) ==")
    annotate_ocf(ocf, SCHEMA)

    decoded = list(fastavro.reader(io.BytesIO(ocf)))
    assert decoded == [TALK] * args.records
    print("\nRound trip via fastavro.reader: ok")


if __name__ == "__main__":
    main()
