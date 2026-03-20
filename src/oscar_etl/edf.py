"""Hand-rolled EDF parser for ResMed CPAP files.

pyedflib rejects ResMed's non-standard EDF files, so we parse the binary
format directly. This module has no dependencies beyond the Python stdlib.
"""

import re
import struct
from datetime import datetime
from pathlib import Path


def safe_float(s):
    """Parse float from ASCII EDF field, returning 0.0 for empty/unparseable."""
    s = s.strip().rstrip("\x00")
    if not s:
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def safe_int(s):
    """Parse int from ASCII EDF field, returning 0 for empty/unparseable."""
    s = s.strip().rstrip("\x00")
    if not s:
        return 0
    try:
        return int(s)
    except ValueError:
        return 0


def parse_edf(path):
    """Parse a ResMed EDF file by hand.

    Returns dict with keys:
        start: datetime
        num_records: int
        record_duration: float
        signals: dict[str, dict] — keyed by label, each with
            "samples_per_record" (int) and "data" (list[float])
        annotations: list[dict] — each with "onset", "duration", "text"
        warnings: list[str] — any parse warnings
    """
    path = Path(path)
    warnings = []
    with open(path, "rb") as f:
        raw = f.read()

    # --- Fixed header (256 bytes) ---
    date_str = raw[168:176].decode("latin-1").strip()
    time_str = raw[176:184].decode("latin-1").strip()
    header_bytes_raw = raw[184:192].decode("latin-1").strip().rstrip("\x00")
    header_bytes = safe_int(header_bytes_raw)
    num_records_raw = raw[236:244].decode("latin-1").strip().rstrip("\x00")
    num_records = safe_int(num_records_raw)
    record_duration = safe_float(raw[244:252].decode("latin-1"))
    ns_raw = raw[252:256].decode("latin-1").strip().rstrip("\x00")
    ns = safe_int(ns_raw)

    if ns == 0 and ns_raw not in ("0", ""):
        warnings.append(f"Unparseable ns field in {path.name}: '{ns_raw}'")
    if num_records == 0 and num_records_raw not in ("0", ""):
        warnings.append(
            f"Unparseable num_records in {path.name}: '{num_records_raw}'"
        )
    if header_bytes == 0 and header_bytes_raw not in ("0", ""):
        warnings.append(
            f"Unparseable header_bytes in {path.name}: '{header_bytes_raw}'"
        )

    expected_header = 256 + ns * 256
    if header_bytes != expected_header and header_bytes > 0:
        warnings.append(
            f"header_bytes mismatch in {path.name}: "
            f"declared={header_bytes}, expected={expected_header}"
        )
        header_bytes = expected_header
    if header_bytes == 0:
        header_bytes = expected_header

    try:
        dd, mm, yy = date_str.split(".")
        hh, mi, ss = time_str.split(".")
        year = 2000 + int(yy) if int(yy) < 85 else 1900 + int(yy)
        start = datetime(year, int(mm), int(dd), int(hh), int(mi), int(ss))
    except (ValueError, TypeError) as e:
        raise ValueError(
            f"Unparseable EDF date/time: {date_str!r} {time_str!r} in {path}"
        ) from e

    # --- Per-signal headers ---
    off = 256

    def read_field(width):
        nonlocal off
        values = []
        for i in range(ns):
            values.append(
                raw[off + i * width : off + (i + 1) * width].decode("latin-1")
            )
        off += ns * width
        return values

    labels_raw = read_field(16)
    _transducer = read_field(80)
    _phys_dim = read_field(8)
    phys_min_raw = read_field(8)
    phys_max_raw = read_field(8)
    dig_min_raw = read_field(8)
    dig_max_raw = read_field(8)
    _prefilter = read_field(80)
    samples_raw = read_field(8)
    _reserved_sig = read_field(32)

    labels = [s.strip().rstrip("\x00") for s in labels_raw]
    phys_min = [safe_float(s) for s in phys_min_raw]
    phys_max = [safe_float(s) for s in phys_max_raw]
    dig_min = [safe_int(s) for s in dig_min_raw]
    dig_max = [safe_int(s) for s in dig_max_raw]
    samples_per_record = [safe_int(s) for s in samples_raw]

    # --- Data records ---
    data_offset = header_bytes
    signals = {}
    annotations = []
    actual_records = max(0, num_records)

    skip_labels = {"EDF Annotations", "Crc16", ""}

    for i, label in enumerate(labels):
        if label not in skip_labels:
            signals[label] = {
                "samples_per_record": samples_per_record[i],
                "data": [],
            }

    annotation_indices = {
        i for i, lbl in enumerate(labels) if lbl == "EDF Annotations"
    }
    annotation_regex = re.compile(
        r"([+-]\d+(?:\.\d+)?)(?:\x15(\d+(?:\.\d+)?))?\x14([^\x14]*)\x14"
    )

    records_parsed = 0
    for rec in range(actual_records):
        rec_offset = data_offset
        # Buffer this record's data — only commit after full record is validated
        rec_signals = {}
        rec_annotations = []
        truncated = False

        for i in range(ns):
            n_samples = samples_per_record[i]
            byte_count = n_samples * 2

            if rec_offset + byte_count > len(raw):
                truncated = True
                break

            sig_bytes = raw[rec_offset : rec_offset + byte_count]

            if i in annotation_indices:
                text = sig_bytes.decode("latin-1")
                for m in annotation_regex.finditer(text):
                    dur_str = m.group(2)
                    duration = float(dur_str) if dur_str else 0.0
                    rec_annotations.append(
                        {
                            "onset": float(m.group(1)),
                            "duration": duration,
                            "text": m.group(3),
                        }
                    )
            elif labels[i] not in skip_labels:
                label = labels[i]
                scale_denom = dig_max[i] - dig_min[i]
                values = []
                if scale_denom == 0:
                    for j in range(n_samples):
                        val = struct.unpack_from("<h", sig_bytes, j * 2)[0]
                        values.append(float(val))
                else:
                    scale = (phys_max[i] - phys_min[i]) / scale_denom
                    for j in range(n_samples):
                        digital = struct.unpack_from("<h", sig_bytes, j * 2)[0]
                        physical = phys_min[i] + (digital - dig_min[i]) * scale
                        values.append(physical)
                rec_signals[label] = values

            rec_offset += byte_count

        if truncated:
            if records_parsed < actual_records:
                warnings.append(
                    f"Truncated file {path.name}: parsed {records_parsed}/{actual_records} records"
                )
            break

        # Full record validated — commit buffered data
        for label, values in rec_signals.items():
            signals[label]["data"].extend(values)
        annotations.extend(rec_annotations)
        data_offset = rec_offset
        records_parsed += 1

    return {
        "start": start,
        "num_records": records_parsed,
        "record_duration": record_duration,
        "signals": signals,
        "annotations": annotations,
        "warnings": warnings,
    }
