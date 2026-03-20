import struct
from datetime import datetime

import pytest


def build_edf(
    path,
    start=None,
    signals=None,
    annotations=None,
    record_duration=2.0,
    num_records=None,
):
    """Build a synthetic EDF file for testing.

    Args:
        path: Where to write the file.
        start: Start datetime (default: 2026-01-15 23:30:00).
        signals: Dict of label -> list of physical values.
            Each signal gets phys_min/max derived from data, dig range -32768..32767.
        annotations: List of dicts with onset, duration, text.
        record_duration: Duration of each data record in seconds.
        num_records: Override number of records (default: derived from data).
    """
    if start is None:
        start = datetime(2026, 1, 15, 23, 30, 0)
    if signals is None:
        signals = {"Press.2s": [10.0, 10.5, 11.0]}
    if annotations is None:
        annotations = []

    # Build signal list (real signals + annotation channel)
    sig_labels = list(signals.keys())
    has_annotations = len(annotations) > 0
    if has_annotations:
        sig_labels.append("EDF Annotations")

    ns = len(sig_labels)

    # Determine samples_per_record and num_records from data
    # Use 1 sample per record for simplicity, so num_records = max_len
    data_lengths = [len(v) for v in signals.values()]
    max_len = max(data_lengths) if data_lengths else 0

    samples_per_record = []
    for label in sig_labels:
        if label == "EDF Annotations":
            samples_per_record.append(15)  # 30 bytes for annotations
        else:
            samples_per_record.append(1)

    if num_records is None:
        num_records = max(max_len, 1 if has_annotations else 0)

    # --- Fixed header (256 bytes) ---
    def pad(s, width):
        return s.ljust(width)[:width].encode("latin-1")

    header = b""
    header += pad("0", 8)  # version
    header += pad("", 80)  # patient
    header += pad("", 80)  # recording
    header += pad(start.strftime("%d.%m.%y"), 8)  # date dd.mm.yy
    header += pad(start.strftime("%H.%M.%S"), 8)  # time hh.mm.ss
    header_bytes = 256 + ns * 256
    header += pad(str(header_bytes), 8)  # header bytes
    header += pad("", 44)  # reserved
    header += pad(str(num_records), 8)  # num records
    header += pad(str(record_duration), 8)  # record duration
    header += pad(str(ns), 4)  # num signals

    # --- Per-signal headers ---
    # Labels (16 bytes each)
    for label in sig_labels:
        header += pad(label, 16)
    # Transducer type (80 bytes each)
    for _ in sig_labels:
        header += pad("", 80)
    # Physical dimension (8 bytes each)
    for _ in sig_labels:
        header += pad("", 8)

    # Physical min/max
    phys_mins = []
    phys_maxs = []
    for label in sig_labels:
        if label == "EDF Annotations":
            phys_mins.append(-1.0)
            phys_maxs.append(1.0)
        else:
            data = signals[label]
            if data:
                phys_mins.append(min(data) - 1.0)
                phys_maxs.append(max(data) + 1.0)
            else:
                phys_mins.append(-100.0)
                phys_maxs.append(100.0)

    for v in phys_mins:
        header += pad(str(v), 8)
    for v in phys_maxs:
        header += pad(str(v), 8)

    # Digital min/max
    dig_min = -32768
    dig_max = 32767
    for _ in sig_labels:
        header += pad(str(dig_min), 8)
    for _ in sig_labels:
        header += pad(str(dig_max), 8)

    # Prefiltering (80 bytes each)
    for _ in sig_labels:
        header += pad("", 80)
    # Samples per record (8 bytes each)
    for spr in samples_per_record:
        header += pad(str(spr), 8)
    # Reserved per signal (32 bytes each)
    for _ in sig_labels:
        header += pad("", 32)

    assert len(header) == header_bytes

    # --- Data records ---
    data_bytes = b""
    for rec in range(num_records):
        for i, label in enumerate(sig_labels):
            spr = samples_per_record[i]
            if label == "EDF Annotations":
                # Build TAL (Time-stamped Annotation List)
                tal = ""
                for ann in annotations:
                    onset = ann["onset"]
                    duration = ann.get("duration", 0.0)
                    text = ann["text"]
                    if duration > 0:
                        tal += f"+{onset}\x15{duration}\x14{text}\x14"
                    else:
                        tal += f"+{onset}\x14{text}\x14"
                # Only write annotations in first record
                if rec > 0:
                    tal = ""
                # Pad to spr * 2 bytes, encode as int16s
                tal_bytes = tal.encode("latin-1")
                tal_bytes = tal_bytes.ljust(spr * 2, b"\x00")[:spr * 2]
                data_bytes += tal_bytes
            else:
                sig_data = signals[label]
                pmin = phys_mins[i]
                pmax = phys_maxs[i]
                scale = (dig_max - dig_min) / (pmax - pmin) if pmax != pmin else 1.0
                for s in range(spr):
                    idx = rec * spr + s
                    if idx < len(sig_data):
                        digital = int(dig_min + (sig_data[idx] - pmin) * scale)
                        digital = max(dig_min, min(dig_max, digital))
                    else:
                        digital = 0
                    data_bytes += struct.pack("<h", digital)

    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "wb") as f:
        f.write(header)
        f.write(data_bytes)


@pytest.fixture
def tmp_edf(tmp_path):
    """Fixture that returns a builder function for synthetic EDF files."""
    def _build(name="test.edf", **kwargs):
        path = tmp_path / name
        build_edf(path, **kwargs)
        return path
    return _build


@pytest.fixture
def tmp_oscar_dir(tmp_path):
    """Fixture that builds a mock OSCAR_Data directory structure."""
    oscar_dir = tmp_path / "OSCAR_Data"
    profile_name = "Test User"
    machine_serial = "ResMed_12345678901"
    datalog_dir = oscar_dir / "Profiles" / profile_name / machine_serial / "Backup" / "DATALOG"
    datalog_dir.mkdir(parents=True)
    return oscar_dir, datalog_dir
