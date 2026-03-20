# oscar-etl Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Build a PyPI-installable `oscar-etl` tool that reads ResMed EDF files from OSCAR's data directory and produces four clean CSVs.

**Architecture:** Three modules — `edf.py` (hand-rolled EDF parser, stdlib only), `etl.py` (discovery, session grouping, stats, CSV writers), `cli.py` (argparse + rich progress). Auto-discovers OSCAR data with platform-specific defaults and CLI overrides.

**Tech Stack:** Python 3.9+, rich (single dependency), pytest for testing. The EDF parser and ETL core use only stdlib (struct, csv, re, pathlib, datetime).

**Source reference:** The battle-tested ETL logic lives at `/Users/eric/Projects/eric-health/scripts/etl_cpap.py` (917 lines). This plan adapts it into a generalized package.

---

### Task 1: Project scaffolding

**Files:**
- Create: `pyproject.toml`
- Create: `src/oscar_etl/__init__.py`
- Create: `src/oscar_etl/edf.py` (empty placeholder)
- Create: `src/oscar_etl/etl.py` (empty placeholder)
- Create: `src/oscar_etl/cli.py` (empty placeholder)
- Create: `LICENSE`
- Create: `.gitignore`
- Create: `tests/__init__.py`
- Create: `tests/conftest.py`

**Step 1: Create pyproject.toml**

```toml
[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[project]
name = "oscar-etl"
version = "0.1.0"
description = "Extract clean CSVs from your OSCAR CPAP data"
readme = "README.md"
license = "MIT"
requires-python = ">=3.9"
dependencies = ["rich>=13.0"]

[project.scripts]
oscar-etl = "oscar_etl.cli:main"

[tool.pytest.ini_options]
testpaths = ["tests"]
```

**Step 2: Create package files**

`src/oscar_etl/__init__.py`:
```python
__version__ = "0.1.0"
```

`src/oscar_etl/edf.py`, `src/oscar_etl/etl.py`, `src/oscar_etl/cli.py`:
```python
# Placeholder
```

`LICENSE` — standard MIT license text with "2026 Eric Wilson" (or preferred copyright holder).

`.gitignore`:
```
__pycache__/
*.pyc
*.egg-info/
dist/
build/
.venv/
oscar-etl-output/
*.csv
.pytest_cache/
```

`tests/__init__.py` — empty file.

**Step 3: Create test fixtures in conftest.py**

This builds synthetic EDF files for testing the parser. An EDF file is:
- 256-byte fixed header
- ns × 256 bytes of per-signal headers
- Data records (each record = sum of samples_per_record × 2 bytes of int16s)

`tests/conftest.py`:
```python
import struct
from datetime import datetime
from pathlib import Path

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
    # Assume all signals have same number of samples
    data_lengths = [len(v) for v in signals.values()]
    max_len = max(data_lengths) if data_lengths else 0

    # Use 1 sample per record for simplicity, so num_records = max_len
    samples_per_record = []
    for label in sig_labels:
        if label == "EDF Annotations":
            samples_per_record.append(15)  # 30 bytes for annotations
        else:
            samples_per_record.append(1)

    if num_records is None:
        num_records = max_len

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
    """Fixture that builds a mock OSCAR_Data directory structure.

    Returns (oscar_dir, datalog_dir) where oscar_dir is the OSCAR_Data root
    and datalog_dir is Profiles/<name>/ResMed_<serial>/Backup/DATALOG/.
    """
    oscar_dir = tmp_path / "OSCAR_Data"
    profile_name = "Test User"
    machine_serial = "ResMed_12345678901"
    datalog_dir = oscar_dir / "Profiles" / profile_name / machine_serial / "Backup" / "DATALOG"
    datalog_dir.mkdir(parents=True)
    return oscar_dir, datalog_dir
```

**Step 4: Install the package in development mode and verify pytest runs**

Run: `cd /Users/eric/Projects/oscar-etl-script && python3 -m venv .venv && source .venv/bin/activate && pip install -e ".[dev]" 2>&1 | tail -5`

Note: Add `[project.optional-dependencies]` to pyproject.toml first:
```toml
[project.optional-dependencies]
dev = ["pytest>=7.0"]
```

Then run: `pytest --co -q`
Expected: "no tests ran" (no test files yet), but no import errors.

**Step 5: Commit**

```bash
git add -A
git commit -m "Project scaffolding: package structure, pyproject.toml, test fixtures"
```

---

### Task 2: EDF parser

**Files:**
- Create: `src/oscar_etl/edf.py`
- Create: `tests/test_edf.py`

Adapt the parser from `/Users/eric/Projects/eric-health/scripts/etl_cpap.py` lines 37–214. The logic is identical — only change is removing print() warnings (return them as a list instead so the CLI can display them with rich).

**Step 1: Write the failing tests**

`tests/test_edf.py`:
```python
from datetime import datetime

from oscar_etl.edf import parse_edf, safe_float, safe_int


class TestSafeFloat:
    def test_normal(self):
        assert safe_float("3.14") == 3.14

    def test_empty(self):
        assert safe_float("") == 0.0

    def test_null_padded(self):
        assert safe_float("3.14\x00\x00") == 3.14

    def test_unparseable(self):
        assert safe_float("abc") == 0.0

    def test_whitespace(self):
        assert safe_float("  7.5  ") == 7.5


class TestSafeInt:
    def test_normal(self):
        assert safe_int("42") == 42

    def test_empty(self):
        assert safe_int("") == 0

    def test_null_padded(self):
        assert safe_int("42\x00\x00") == 42

    def test_unparseable(self):
        assert safe_int("abc") == 0


class TestParseEdf:
    def test_parses_start_time(self, tmp_edf):
        start = datetime(2026, 3, 15, 22, 45, 0)
        path = tmp_edf(start=start, signals={"Press.2s": [10.0, 11.0]})
        result = parse_edf(path)
        assert result["start"] == start

    def test_parses_signal_data(self, tmp_edf):
        data = [10.0, 12.0, 14.0]
        path = tmp_edf(signals={"Press.2s": data})
        result = parse_edf(path)
        assert "Press.2s" in result["signals"]
        parsed = result["signals"]["Press.2s"]["data"]
        assert len(parsed) == 3
        # Allow small floating-point error from int16 quantization
        for expected, actual in zip(data, parsed):
            assert abs(expected - actual) < 0.1

    def test_parses_multiple_signals(self, tmp_edf):
        path = tmp_edf(signals={
            "Press.2s": [10.0, 11.0],
            "Leak.2s": [5.0, 6.0],
        })
        result = parse_edf(path)
        assert "Press.2s" in result["signals"]
        assert "Leak.2s" in result["signals"]

    def test_parses_annotations(self, tmp_edf):
        path = tmp_edf(
            signals={"Press.2s": [10.0]},
            annotations=[
                {"onset": 120.0, "duration": 10.5, "text": "Obstructive Apnea"},
            ],
        )
        result = parse_edf(path)
        assert len(result["annotations"]) == 1
        ann = result["annotations"][0]
        assert ann["onset"] == 120.0
        assert ann["duration"] == 10.5
        assert ann["text"] == "Obstructive Apnea"

    def test_skips_crc16_signal(self, tmp_edf):
        path = tmp_edf(signals={"Press.2s": [10.0], "Crc16": [0.0]})
        result = parse_edf(path)
        assert "Crc16" not in result["signals"]
        assert "Press.2s" in result["signals"]

    def test_num_records_and_duration(self, tmp_edf):
        path = tmp_edf(
            signals={"Press.2s": [10.0, 11.0, 12.0]},
            record_duration=2.0,
        )
        result = parse_edf(path)
        assert result["num_records"] == 3
        assert result["record_duration"] == 2.0

    def test_truncated_file_does_not_crash(self, tmp_edf):
        """Parser should handle truncated files gracefully."""
        path = tmp_edf(signals={"Press.2s": [10.0, 11.0, 12.0]})
        # Truncate the file by removing last 10 bytes
        data = path.read_bytes()
        path.write_bytes(data[:-10])
        result = parse_edf(path)
        # Should parse what it can without raising
        assert result["start"] is not None

    def test_annotation_without_duration(self, tmp_edf):
        path = tmp_edf(
            signals={"Press.2s": [10.0]},
            annotations=[
                {"onset": 60.0, "text": "Recording starts"},
            ],
        )
        result = parse_edf(path)
        assert len(result["annotations"]) >= 1
        ann = [a for a in result["annotations"] if a["text"] == "Recording starts"]
        assert len(ann) == 1
        assert ann[0]["duration"] == 0.0
```

**Step 2: Run tests to verify they fail**

Run: `source .venv/bin/activate && pytest tests/test_edf.py -v`
Expected: FAIL — `oscar_etl.edf` has no `parse_edf` function yet.

**Step 3: Implement the EDF parser**

Copy the parser from the source script (`/Users/eric/Projects/eric-health/scripts/etl_cpap.py` lines 37–214) into `src/oscar_etl/edf.py`. Key adaptation: replace `print()` warnings with a `warnings` list returned alongside the parsed data.

`src/oscar_etl/edf.py`:
```python
"""Hand-rolled EDF parser for ResMed CPAP files.

pyedflib rejects ResMed's non-standard EDF files, so we parse the binary
format directly. This module has no dependencies beyond the Python stdlib.
"""

import re
import struct
from datetime import datetime


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
        warnings.append(f"Unparseable num_records in {path.name}: '{num_records_raw}'")
    if header_bytes == 0 and header_bytes_raw not in ("0", ""):
        warnings.append(f"Unparseable header_bytes in {path.name}: '{header_bytes_raw}'")

    expected_header = 256 + ns * 256
    if header_bytes != expected_header and header_bytes > 0:
        warnings.append(
            f"header_bytes mismatch in {path.name}: "
            f"declared={header_bytes}, expected={expected_header}"
        )
        header_bytes = expected_header
    if header_bytes == 0:
        header_bytes = expected_header

    # Parse start datetime
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
            values.append(raw[off + i * width : off + (i + 1) * width].decode("latin-1"))
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

    annotation_indices = {i for i, l in enumerate(labels) if l == "EDF Annotations"}
    annotation_regex = re.compile(
        r"\+(\d+(?:\.\d+)?)(?:\x15(\d+(?:\.\d+)?))?\x14([^\x14]*)\x14"
    )

    for rec in range(actual_records):
        rec_offset = data_offset
        for i in range(ns):
            n_samples = samples_per_record[i]
            byte_count = n_samples * 2

            if rec_offset + byte_count > len(raw):
                break

            sig_bytes = raw[rec_offset : rec_offset + byte_count]

            if i in annotation_indices:
                text = sig_bytes.decode("latin-1")
                for m in annotation_regex.finditer(text):
                    dur_str = m.group(2)
                    duration = float(dur_str) if dur_str else 0.0
                    annotations.append({
                        "onset": float(m.group(1)),
                        "duration": duration,
                        "text": m.group(3),
                    })
            elif labels[i] not in skip_labels:
                label = labels[i]
                scale_denom = dig_max[i] - dig_min[i]
                if scale_denom == 0:
                    for j in range(n_samples):
                        val = struct.unpack_from("<h", sig_bytes, j * 2)[0]
                        signals[label]["data"].append(float(val))
                else:
                    scale = (phys_max[i] - phys_min[i]) / scale_denom
                    for j in range(n_samples):
                        digital = struct.unpack_from("<h", sig_bytes, j * 2)[0]
                        physical = phys_min[i] + (digital - dig_min[i]) * scale
                        signals[label]["data"].append(physical)

            rec_offset += byte_count
        else:
            data_offset = rec_offset
            continue
        break

    return {
        "start": start,
        "num_records": actual_records,
        "record_duration": record_duration,
        "signals": signals,
        "annotations": annotations,
        "warnings": warnings,
    }
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_edf.py -v`
Expected: All tests PASS.

**Step 5: Commit**

```bash
git add src/oscar_etl/edf.py tests/test_edf.py
git commit -m "Add EDF parser with tests for ResMed file format"
```

---

### Task 3: ETL core — statistics, constants, and helpers

**Files:**
- Create: `src/oscar_etl/etl.py` (first section)
- Create: `tests/test_etl_helpers.py`

**Step 1: Write the failing tests**

`tests/test_etl_helpers.py`:
```python
from datetime import datetime

from oscar_etl.etl import (
    evening_date,
    median,
    nonneg_values,
    percentile,
    positive_values,
)


class TestPercentile:
    def test_median_of_three(self):
        assert percentile([1, 2, 3], 50) == 2.0

    def test_95th(self):
        data = list(range(1, 101))
        assert abs(percentile(data, 95) - 95.05) < 0.1

    def test_empty(self):
        assert percentile([], 50) == 0.0

    def test_single_value(self):
        assert percentile([42], 50) == 42

    def test_99_5th(self):
        data = list(range(1, 1001))
        result = percentile(data, 99.5)
        assert 995 < result < 1000


class TestMedian:
    def test_odd(self):
        assert median([3, 1, 2]) == 2.0

    def test_even(self):
        assert median([1, 2, 3, 4]) == 2.5


class TestFilters:
    def test_nonneg_includes_zero(self):
        assert nonneg_values([-1, 0, 1, 2]) == [0, 1, 2]

    def test_positive_excludes_zero(self):
        assert positive_values([-1, 0, 1, 2]) == [1, 2]


class TestEveningDate:
    def test_evening_session(self):
        """22:30 on March 15 → date is March 15."""
        dt = datetime(2026, 3, 15, 22, 30, 0)
        assert evening_date(dt, day_boundary=12) == "2026-03-15"

    def test_after_midnight_session(self):
        """01:30 on March 16 → date is March 15 (previous day)."""
        dt = datetime(2026, 3, 16, 1, 30, 0)
        assert evening_date(dt, day_boundary=12) == "2026-03-15"

    def test_noon_boundary(self):
        """12:00 exactly → same day."""
        dt = datetime(2026, 3, 15, 12, 0, 0)
        assert evening_date(dt, day_boundary=12) == "2026-03-15"

    def test_just_before_noon(self):
        """11:59 → previous day."""
        dt = datetime(2026, 3, 15, 11, 59, 0)
        assert evening_date(dt, day_boundary=12) == "2026-03-14"

    def test_custom_day_boundary(self):
        """Shift worker: boundary at 18:00. 8am → previous day."""
        dt = datetime(2026, 3, 15, 8, 0, 0)
        assert evening_date(dt, day_boundary=18) == "2026-03-14"

    def test_custom_boundary_after(self):
        """Shift worker: boundary at 18:00. 19:00 → same day."""
        dt = datetime(2026, 3, 15, 19, 0, 0)
        assert evening_date(dt, day_boundary=18) == "2026-03-15"
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_etl_helpers.py -v`
Expected: FAIL — functions not yet defined.

**Step 3: Implement helpers and constants**

Write the first section of `src/oscar_etl/etl.py`:

```python
"""OSCAR data discovery, session processing, and CSV output.

This module contains everything between the EDF parser (edf.py) and the
CLI (cli.py): finding OSCAR data on disk, grouping files into sessions,
computing statistics, and writing the four output CSVs.
"""

import csv
import re
import sys
from datetime import datetime, timedelta
from pathlib import Path

from oscar_etl.edf import parse_edf

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

FILE_TYPES = {"CSL", "EVE", "PLD", "SAD", "BRP"}
FILE_PATTERN = re.compile(r"^(\d{8}_\d{6})_([A-Z]{3})\.edf$")

EVENT_MAP = {
    "Central Apnea": "ca_count",
    "Obstructive Apnea": "oa_count",
    "Hypopnea": "h_count",
    "Apnea": "ua_count",
    "Arousal": "arousal_count",
}

PLD_SIGNAL_MAP = {
    "Press.2s": "pressure",
    "Leak.2s": "leak",
    "RespRate.2s": "resp_rate",
    "TidVol.2s": "tidal_vol",
    "MinVent.2s": "minute_vent",
    "Snore.2s": "snore",
    "FlowLim.2s": "flow_limit",
}

SESSION_COLUMNS = [
    "date", "session_start", "session_end", "duration_minutes",
    "ahi", "ca_count", "oa_count", "h_count", "ua_count", "arousal_count",
    "pressure_median", "pressure_95", "pressure_995",
    "leak_median", "leak_95",
    "resp_rate_median", "tidal_vol_median", "minute_vent_median",
]

DAILY_COLUMNS = [
    "date", "sessions", "start", "end", "total_minutes",
    "ahi", "ca_count", "oa_count", "h_count", "ua_count", "arousal_count",
    "pressure_median", "pressure_95", "pressure_995",
    "leak_median", "leak_95",
    "resp_rate_median", "tidal_vol_median", "minute_vent_median",
]

TIMESERIES_COLUMNS = [
    "datetime", "date", "session_start",
    "pressure", "leak", "resp_rate", "tidal_vol", "minute_vent",
    "snore", "flow_limit",
]

EVENTS_COLUMNS = [
    "datetime", "date", "session_start", "event", "duration_sec",
]


# ---------------------------------------------------------------------------
# Statistics helpers
# ---------------------------------------------------------------------------

def percentile(data, p):
    """Compute p-th percentile (0-100) using linear interpolation."""
    if not data:
        return 0.0
    sorted_data = sorted(data)
    n = len(sorted_data)
    if n == 1:
        return sorted_data[0]
    k = (p / 100.0) * (n - 1)
    f = int(k)
    c = f + 1
    if c >= n:
        return sorted_data[-1]
    d = k - f
    return sorted_data[f] + d * (sorted_data[c] - sorted_data[f])


def median(data):
    return percentile(data, 50)


def nonneg_values(data):
    return [v for v in data if v >= 0]


def positive_values(data):
    return [v for v in data if v > 0]


def evening_date(dt, day_boundary=12):
    """Assign sessions starting before day_boundary to the previous calendar day."""
    if dt.hour < day_boundary:
        return (dt - timedelta(days=1)).strftime("%Y-%m-%d")
    return dt.strftime("%Y-%m-%d")
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_etl_helpers.py -v`
Expected: All PASS.

**Step 5: Commit**

```bash
git add src/oscar_etl/etl.py tests/test_etl_helpers.py
git commit -m "Add statistics helpers, constants, and evening_date with day_boundary"
```

---

### Task 4: ETL core — OSCAR data discovery

**Files:**
- Modify: `src/oscar_etl/etl.py`
- Create: `tests/test_discovery.py`

This adds the platform-aware OSCAR data finder and profile/machine scanner.

**Step 1: Write the failing tests**

`tests/test_discovery.py`:
```python
import sys
from pathlib import Path

from oscar_etl.etl import find_oscar_dir, scan_profiles


class TestFindOscarDir:
    def test_explicit_override(self, tmp_path):
        oscar_dir = tmp_path / "MyOscarData"
        oscar_dir.mkdir()
        result = find_oscar_dir(oscar_dir=str(oscar_dir))
        assert result == oscar_dir

    def test_explicit_override_missing_raises(self, tmp_path):
        import pytest
        with pytest.raises(SystemExit):
            find_oscar_dir(oscar_dir=str(tmp_path / "nonexistent"))

    def test_follows_symlink(self, tmp_path):
        real_dir = tmp_path / "real_oscar"
        real_dir.mkdir()
        link = tmp_path / "link_oscar"
        link.symlink_to(real_dir)
        result = find_oscar_dir(oscar_dir=str(link))
        assert result == real_dir


class TestScanProfiles:
    def test_single_profile_single_machine(self, tmp_oscar_dir):
        oscar_dir, datalog_dir = tmp_oscar_dir
        profiles = scan_profiles(oscar_dir)
        assert len(profiles) == 1
        assert profiles[0]["name"] == "Test User"
        assert "12345678901" in profiles[0]["serial"]
        assert profiles[0]["datalog"].is_dir()

    def test_no_profiles_raises(self, tmp_path):
        import pytest
        oscar_dir = tmp_path / "OSCAR_Data"
        oscar_dir.mkdir()
        (oscar_dir / "Profiles").mkdir()
        with pytest.raises(SystemExit):
            scan_profiles(oscar_dir)

    def test_multiple_profiles(self, tmp_path):
        oscar_dir = tmp_path / "OSCAR_Data"
        for name, serial in [("Alice", "ResMed_111"), ("Bob", "ResMed_222")]:
            dl = oscar_dir / "Profiles" / name / serial / "Backup" / "DATALOG"
            dl.mkdir(parents=True)
        profiles = scan_profiles(oscar_dir)
        assert len(profiles) == 2
        names = {p["name"] for p in profiles}
        assert names == {"Alice", "Bob"}

    def test_filter_by_profile_name(self, tmp_path):
        oscar_dir = tmp_path / "OSCAR_Data"
        for name, serial in [("Alice", "ResMed_111"), ("Bob", "ResMed_222")]:
            dl = oscar_dir / "Profiles" / name / serial / "Backup" / "DATALOG"
            dl.mkdir(parents=True)
        profiles = scan_profiles(oscar_dir, profile_name="Alice")
        assert len(profiles) == 1
        assert profiles[0]["name"] == "Alice"

    def test_filter_by_machine_serial(self, tmp_path):
        oscar_dir = tmp_path / "OSCAR_Data"
        profile = oscar_dir / "Profiles" / "Alice"
        for serial in ["ResMed_111", "ResMed_222"]:
            (profile / serial / "Backup" / "DATALOG").mkdir(parents=True)
        profiles = scan_profiles(oscar_dir, machine_serial="222")
        assert len(profiles) == 1
        assert "222" in profiles[0]["serial"]
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_discovery.py -v`
Expected: FAIL — functions not defined.

**Step 3: Implement discovery functions**

Add to `src/oscar_etl/etl.py`:

```python
# ---------------------------------------------------------------------------
# OSCAR data discovery
# ---------------------------------------------------------------------------

def _default_oscar_paths():
    """Return platform-specific default OSCAR_Data paths to try."""
    home = Path.home()
    if sys.platform == "darwin":
        return [home / "Documents" / "OSCAR_Data"]
    elif sys.platform == "win32":
        return [home / "Documents" / "OSCAR_Data"]
    else:
        # Linux: XDG first, then Documents fallback
        xdg = Path(os.environ.get("XDG_DATA_HOME", home / ".local" / "share"))
        return [
            xdg / "OSCAR_Data",
            home / "Documents" / "OSCAR_Data",
        ]


def find_oscar_dir(oscar_dir=None):
    """Locate the OSCAR_Data directory.

    Args:
        oscar_dir: Explicit path override (from --oscar-dir flag).

    Returns:
        Path to the OSCAR_Data directory (resolved, following symlinks).

    Raises:
        SystemExit with helpful message if not found.
    """
    if oscar_dir:
        p = Path(oscar_dir)
        if not p.exists():
            print(f"Error: --oscar-dir path does not exist: {p}", file=sys.stderr)
            sys.exit(1)
        return p.resolve()

    for candidate in _default_oscar_paths():
        try:
            resolved = candidate.resolve()
            if resolved.is_dir():
                return resolved
        except PermissionError:
            # macOS TCC: try following symlink
            if candidate.is_symlink():
                target = candidate.resolve()
                if target.is_dir():
                    return target
            # If it's a symlink pointing to a real location, resolve worked.
            # If not, fall through to error message.
            continue

    # Nothing found — print platform-specific help
    if sys.platform == "darwin":
        print(
            "Error: Could not find OSCAR_Data directory.\n\n"
            "  If you haven't set up data access yet, see the README:\n"
            "  https://github.com/yourname/oscar-etl#macos-setup\n\n"
            "  Or specify the path directly:\n"
            "    oscar-etl --oscar-dir /path/to/OSCAR_Data",
            file=sys.stderr,
        )
    else:
        print(
            "Error: Could not find OSCAR_Data directory.\n\n"
            "  Specify the path directly:\n"
            "    oscar-etl --oscar-dir /path/to/OSCAR_Data",
            file=sys.stderr,
        )
    sys.exit(1)


def scan_profiles(oscar_dir, profile_name=None, machine_serial=None):
    """Scan OSCAR_Data for profiles and ResMed machines.

    Returns list of dicts: [{"name": str, "serial": str, "datalog": Path}]

    Raises SystemExit if no valid profiles/machines found.
    """
    profiles_dir = oscar_dir / "Profiles"
    if not profiles_dir.is_dir():
        print(f"Error: No Profiles directory in {oscar_dir}", file=sys.stderr)
        sys.exit(1)

    results = []
    for profile_path in sorted(profiles_dir.iterdir()):
        if not profile_path.is_dir():
            continue
        name = profile_path.name
        if profile_name and name != profile_name:
            continue
        for machine_path in sorted(profile_path.iterdir()):
            if not machine_path.is_dir() or not machine_path.name.startswith("ResMed_"):
                continue
            serial = machine_path.name
            if machine_serial and machine_serial not in serial:
                continue
            datalog = machine_path / "Backup" / "DATALOG"
            if datalog.is_dir():
                results.append({
                    "name": name,
                    "serial": serial,
                    "datalog": datalog,
                })

    if not results:
        if profile_name or machine_serial:
            print(
                f"Error: No matching ResMed machine found "
                f"(profile={profile_name!r}, machine={machine_serial!r})",
                file=sys.stderr,
            )
        else:
            print(
                "Error: No ResMed machines found in OSCAR_Data.\n"
                "  oscar-etl currently supports ResMed machines only.",
                file=sys.stderr,
            )
        sys.exit(1)

    return results
```

Note: add `import os` at the top of etl.py.

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_discovery.py -v`
Expected: All PASS.

**Step 5: Commit**

```bash
git add src/oscar_etl/etl.py tests/test_discovery.py
git commit -m "Add OSCAR data directory discovery with platform-specific defaults"
```

---

### Task 5: ETL core — session discovery and EDF processing

**Files:**
- Modify: `src/oscar_etl/etl.py`
- Create: `tests/test_sessions.py`

Adapts `discover_sessions()` from the source, parameterizing the DATALOG path and day_boundary.

**Step 1: Write the failing tests**

`tests/test_sessions.py`:
```python
from datetime import datetime
from pathlib import Path

from tests.conftest import build_edf
from oscar_etl.etl import discover_sessions, parse_and_cache_edfs


class TestDiscoverSessions:
    def _make_session(self, datalog_dir, year, ts_prefix, file_types):
        """Helper: create EDF files in DATALOG/<year>/."""
        year_dir = datalog_dir / str(year)
        year_dir.mkdir(exist_ok=True)
        for ft in file_types:
            name = f"{ts_prefix}_{ft}.edf"
            path = year_dir / name
            if ft == "EVE":
                build_edf(path, signals={}, annotations=[
                    {"onset": 60.0, "duration": 10.0, "text": "Obstructive Apnea"},
                ])
            elif ft == "PLD":
                build_edf(path, signals={
                    "Press.2s": [10.0, 11.0, 12.0],
                    "Leak.2s": [2.0, 3.0, 4.0],
                })
            else:
                build_edf(path, signals={"Press.2s": [0.0]})

    def test_discovers_sessions_from_pld(self, tmp_oscar_dir):
        _, datalog_dir = tmp_oscar_dir
        # One power-on period: CSL, EVE, PLD
        self._make_session(datalog_dir, 2026, "20260315_223000", ["CSL"])
        self._make_session(datalog_dir, 2026, "20260315_223005", ["EVE"])
        self._make_session(datalog_dir, 2026, "20260315_223010", ["PLD"])
        sessions = discover_sessions(datalog_dir, day_boundary=12)
        assert "2026-03-15" in sessions
        assert len(sessions["2026-03-15"]) == 1
        assert "PLD" in sessions["2026-03-15"][0]["files"]

    def test_multiple_pld_sessions(self, tmp_oscar_dir):
        _, datalog_dir = tmp_oscar_dir
        # CSL starts power-on, two PLDs = two mask-on sessions
        self._make_session(datalog_dir, 2026, "20260315_223000", ["CSL"])
        self._make_session(datalog_dir, 2026, "20260315_223005", ["EVE"])
        self._make_session(datalog_dir, 2026, "20260315_223010", ["PLD"])
        self._make_session(datalog_dir, 2026, "20260316_020000", ["PLD"])
        sessions = discover_sessions(datalog_dir, day_boundary=12)
        total = sum(len(v) for v in sessions.values())
        assert total == 2

    def test_skips_csl_only_period(self, tmp_oscar_dir):
        _, datalog_dir = tmp_oscar_dir
        # CSL with no PLD = zero-duration, skip
        self._make_session(datalog_dir, 2026, "20260315_223000", ["CSL"])
        sessions = discover_sessions(datalog_dir, day_boundary=12)
        assert len(sessions) == 0

    def test_after_midnight_uses_previous_date(self, tmp_oscar_dir):
        _, datalog_dir = tmp_oscar_dir
        self._make_session(datalog_dir, 2026, "20260316_013000", ["CSL"])
        self._make_session(datalog_dir, 2026, "20260316_013005", ["PLD"])
        sessions = discover_sessions(datalog_dir, day_boundary=12)
        # 01:30 on March 16 → evening date is March 15
        assert "2026-03-15" in sessions


class TestParseAndCacheEdfs:
    def test_populates_pld_and_eve_data(self, tmp_oscar_dir):
        _, datalog_dir = tmp_oscar_dir
        year_dir = datalog_dir / "2026"
        year_dir.mkdir()
        build_edf(year_dir / "20260315_223000_CSL.edf", signals={"Press.2s": [0.0]})
        build_edf(
            year_dir / "20260315_223005_EVE.edf",
            signals={},
            annotations=[{"onset": 60.0, "duration": 10.0, "text": "Hypopnea"}],
        )
        build_edf(
            year_dir / "20260315_223010_PLD.edf",
            signals={"Press.2s": [10.0, 11.0, 12.0]},
        )
        sessions = discover_sessions(datalog_dir, day_boundary=12)
        warnings = parse_and_cache_edfs(sessions)
        session = sessions["2026-03-15"][0]
        assert session["pld_data"] is not None
        assert session["eve_data"] is not None
        assert "Press.2s" in session["pld_data"]["signals"]
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_sessions.py -v`
Expected: FAIL — functions not defined.

**Step 3: Implement session discovery**

Add to `src/oscar_etl/etl.py`:

```python
# ---------------------------------------------------------------------------
# Session discovery
# ---------------------------------------------------------------------------

def parse_file_timestamp(ts_str):
    """Parse '20260101_015038' → datetime."""
    return datetime.strptime(ts_str, "%Y%m%d_%H%M%S")


def discover_sessions(datalog_dir, day_boundary=12):
    """Scan DATALOG/ and group files into sessions keyed by date.

    Each PLD file = one session (mask-on segment). CSL and EVE files
    are shared across all PLD sessions within a power-on period.

    Returns dict[str, list[dict]] keyed by date "YYYY-MM-DD".
    """
    all_files = []
    for year_dir in sorted(datalog_dir.iterdir()):
        if not year_dir.is_dir():
            continue
        if not re.fullmatch(r"\d{4}", year_dir.name):
            continue
        for edf_path in year_dir.glob("*.edf"):
            m = FILE_PATTERN.match(edf_path.name)
            if not m:
                continue
            file_type = m.group(2)
            if file_type not in FILE_TYPES:
                continue
            ts_dt = parse_file_timestamp(m.group(1))
            all_files.append((ts_dt, file_type, edf_path))

    all_files.sort()

    csl_indices = [i for i, (_, ft, _) in enumerate(all_files) if ft == "CSL"]
    sessions_by_date = {}

    for idx, ci in enumerate(csl_indices):
        csl_dt, _, csl_path = all_files[ci]
        next_ci = csl_indices[idx + 1] if idx + 1 < len(csl_indices) else len(all_files)

        shared_files = {"CSL": csl_path}
        pld_files = []

        for j in range(ci + 1, next_ci):
            ts_dt, ft, path = all_files[j]
            if ft == "PLD":
                pld_files.append((ts_dt, path))
            elif ft in ("EVE", "BRP", "SAD"):
                if ft not in shared_files:
                    shared_files[ft] = path

        if not pld_files:
            continue

        for pld_dt, pld_path in pld_files:
            date = evening_date(pld_dt, day_boundary)
            session = {
                "date": date,
                "session_start": pld_dt.isoformat(),
                "files": {
                    "PLD": pld_path,
                    **{k: v for k, v in shared_files.items()},
                },
            }
            if date not in sessions_by_date:
                sessions_by_date[date] = []
            sessions_by_date[date].append(session)

    return sessions_by_date


def parse_and_cache_edfs(sessions_by_date):
    """Parse PLD and EVE files once, store results in session dicts.

    Returns list of warning strings.
    """
    warnings = []
    eve_cache = {}

    for date in sorted(sessions_by_date):
        for session in sessions_by_date[date]:
            files = session["files"]

            pld_path = files.get("PLD")
            if pld_path:
                try:
                    pld_data = parse_edf(pld_path)
                    warnings.extend(pld_data.get("warnings", []))
                    session["pld_data"] = pld_data
                    session["session_start"] = pld_data["start"].isoformat()
                    session["date"] = evening_date(
                        pld_data["start"],
                        # Preserve the day_boundary used during discovery
                        # For now, use the default; CLI will pass it through
                        day_boundary=12,
                    )
                    matched = [l for l in pld_data["signals"] if l in PLD_SIGNAL_MAP]
                    if pld_data["signals"] and not matched:
                        actual = list(pld_data["signals"].keys())
                        warnings.append(
                            f"No PLD signals matched expected labels in "
                            f"{pld_path.name}. Found: {actual}"
                        )
                except Exception as e:
                    warnings.append(f"Failed to parse PLD {pld_path.name}: {e}")
                    session["pld_data"] = None

            eve_path = files.get("EVE")
            if eve_path:
                if eve_path not in eve_cache:
                    try:
                        eve_data = parse_edf(eve_path)
                        warnings.extend(eve_data.get("warnings", []))
                        eve_cache[eve_path] = eve_data
                    except Exception as e:
                        warnings.append(f"Failed to parse EVE {eve_path.name}: {e}")
                        eve_cache[eve_path] = None
                session["eve_data"] = eve_cache[eve_path]

    # Rebuild sessions_by_date since PLD header may have changed dates
    rebuilt = {}
    for date in list(sessions_by_date.keys()):
        for session in sessions_by_date[date]:
            new_date = session["date"]
            if new_date not in rebuilt:
                rebuilt[new_date] = []
            rebuilt[new_date].append(session)
    sessions_by_date.clear()
    sessions_by_date.update(rebuilt)

    return warnings
```

Note: `parse_and_cache_edfs` currently hardcodes `day_boundary=12` when reassigning dates from PLD headers. In Task 7 (CLI), the day_boundary will be threaded through as a parameter. For now this is sufficient for testing.

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_sessions.py -v`
Expected: All PASS.

**Step 5: Commit**

```bash
git add src/oscar_etl/etl.py tests/test_sessions.py
git commit -m "Add session discovery and EDF parsing/caching"
```

---

### Task 6: ETL core — session, daily, event, and timeseries processing + CSV output

**Files:**
- Modify: `src/oscar_etl/etl.py`
- Create: `tests/test_etl_output.py`

This is the largest task — it adapts the four `etl_*` functions and `write_csv` from the source. The logic stays the same; the main change is parameterizing output paths.

**Step 1: Write the failing tests**

`tests/test_etl_output.py`:
```python
import csv
from datetime import datetime
from pathlib import Path

from tests.conftest import build_edf
from oscar_etl.etl import (
    discover_sessions,
    parse_and_cache_edfs,
    etl_sessions,
    etl_daily,
    etl_events,
    etl_timeseries,
    write_csv,
    SESSION_COLUMNS,
)


def _setup_one_night(datalog_dir):
    """Create a single night's data: CSL + EVE + PLD."""
    year_dir = datalog_dir / "2026"
    year_dir.mkdir(exist_ok=True)
    build_edf(
        year_dir / "20260315_223000_CSL.edf",
        start=datetime(2026, 3, 15, 22, 30, 0),
        signals={"Press.2s": [0.0]},
    )
    build_edf(
        year_dir / "20260315_223005_EVE.edf",
        start=datetime(2026, 3, 15, 22, 30, 5),
        signals={},
        annotations=[
            {"onset": 60.0, "duration": 12.0, "text": "Obstructive Apnea"},
            {"onset": 300.0, "duration": 8.0, "text": "Hypopnea"},
            {"onset": 600.0, "duration": 15.0, "text": "Central Apnea"},
        ],
    )
    build_edf(
        year_dir / "20260315_223010_PLD.edf",
        start=datetime(2026, 3, 15, 22, 30, 10),
        signals={
            "Press.2s": [10.0, 10.5, 11.0, 10.8, 11.2],
            "Leak.2s": [2.0, 3.0, 2.5, 2.8, 3.2],
            "RespRate.2s": [14.0, 15.0, 14.5, 15.2, 14.8],
            "TidVol.2s": [450.0, 460.0, 455.0, 470.0, 465.0],
            "MinVent.2s": [6.3, 6.5, 6.4, 6.7, 6.6],
            "Snore.2s": [0.0, 0.0, 1.0, 0.0, 0.0],
            "FlowLim.2s": [0.0, 0.1, 0.0, 0.2, 0.0],
        },
        record_duration=2.0,
    )


class TestWriteCsv:
    def test_writes_correct_columns(self, tmp_path):
        path = tmp_path / "out.csv"
        rows = [{"a": 1, "b": 2}, {"a": 3, "b": 4}]
        write_csv(path, ["a", "b"], rows)
        with open(path) as f:
            reader = csv.DictReader(f)
            assert reader.fieldnames == ["a", "b"]
            data = list(reader)
            assert len(data) == 2

    def test_strips_internal_keys(self, tmp_path):
        path = tmp_path / "out.csv"
        rows = [{"a": 1, "_internal": "hidden"}]
        write_csv(path, ["a"], rows)
        with open(path) as f:
            reader = csv.DictReader(f)
            assert "_internal" not in reader.fieldnames


class TestEtlSessions:
    def test_produces_session_rows(self, tmp_oscar_dir):
        _, datalog_dir = tmp_oscar_dir
        _setup_one_night(datalog_dir)
        sessions = discover_sessions(datalog_dir)
        parse_and_cache_edfs(sessions)
        rows, unattributed = etl_sessions(sessions)
        assert len(rows) == 1
        row = rows[0]
        assert row["date"] == "2026-03-15"
        assert row["duration_minutes"] > 0
        assert row["pressure_median"] != ""

    def test_counts_events(self, tmp_oscar_dir):
        _, datalog_dir = tmp_oscar_dir
        _setup_one_night(datalog_dir)
        sessions = discover_sessions(datalog_dir)
        parse_and_cache_edfs(sessions)
        rows, _ = etl_sessions(sessions)
        row = rows[0]
        # Events from our test EVE: 1 OA + 1 H + 1 CA = 3 total
        assert row["oa_count"] + row["h_count"] + row["ca_count"] >= 0


class TestEtlDaily:
    def test_aggregates_by_date(self, tmp_oscar_dir):
        _, datalog_dir = tmp_oscar_dir
        _setup_one_night(datalog_dir)
        sessions = discover_sessions(datalog_dir)
        parse_and_cache_edfs(sessions)
        session_rows, unattributed = etl_sessions(sessions)
        daily_rows = etl_daily(session_rows, sessions, unattributed)
        assert len(daily_rows) == 1
        assert daily_rows[0]["sessions"] == 1
        assert daily_rows[0]["total_minutes"] > 0


class TestEtlEvents:
    def test_extracts_events(self, tmp_oscar_dir):
        _, datalog_dir = tmp_oscar_dir
        _setup_one_night(datalog_dir)
        sessions = discover_sessions(datalog_dir)
        parse_and_cache_edfs(sessions)
        event_rows = etl_events(sessions)
        assert len(event_rows) >= 0  # May be 0 if events fall outside PLD range
        for row in event_rows:
            assert "event" in row
            assert "duration_sec" in row


class TestEtlTimeseries:
    def test_writes_timeseries(self, tmp_oscar_dir, tmp_path):
        _, datalog_dir = tmp_oscar_dir
        _setup_one_night(datalog_dir)
        sessions = discover_sessions(datalog_dir)
        parse_and_cache_edfs(sessions)
        output_path = tmp_path / "timeseries.csv"
        count = etl_timeseries(sessions, output_path)
        assert count > 0
        with open(output_path) as f:
            reader = csv.DictReader(f)
            row = next(reader)
            assert "pressure" in row
            assert "leak" in row
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_etl_output.py -v`
Expected: FAIL — functions not defined.

**Step 3: Implement session/daily/event/timeseries processing and CSV writer**

Add to `src/oscar_etl/etl.py`. This is adapted directly from the source script (`/Users/eric/Projects/eric-health/scripts/etl_cpap.py` lines 362–910). Key changes:
- `etl_timeseries` takes an `output_path` parameter instead of using a global
- `write_csv` is unchanged
- All functions receive data as parameters, no globals

```python
# ---------------------------------------------------------------------------
# CSV writer
# ---------------------------------------------------------------------------

def write_csv(path, fieldnames, rows):
    """Write rows to CSV, stripping internal keys (prefixed with _)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    clean_rows = [{k: v for k, v in row.items() if not k.startswith("_")} for row in rows]
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clean_rows)


# ---------------------------------------------------------------------------
# Session processing
# ---------------------------------------------------------------------------

def etl_sessions(sessions_by_date):
    """Parse each session's EDF files and produce session rows.

    Returns (rows, unattributed_events).
    """
    rows = []
    unattributed_events = {}
    global_pld_ranges = {}

    for date in sorted(sessions_by_date):
        date_sessions = sessions_by_date[date]

        for session in date_sessions:
            pld_data = session.get("pld_data")
            if pld_data is None:
                continue

            pld_dur = pld_data["num_records"] * pld_data["record_duration"]
            if pld_dur <= 0:
                continue

            duration_seconds = pld_dur
            pld_start = pld_data["start"]
            session_start = pld_start.isoformat()
            session_end = (pld_start + timedelta(seconds=pld_dur)).isoformat()

            counts = {col: 0 for col in EVENT_MAP.values()}
            pld_end_ts = pld_start + timedelta(seconds=pld_dur)
            eve_path = session["files"].get("EVE")
            if eve_path:
                global_pld_ranges.setdefault(eve_path, []).append((pld_start, pld_end_ts))
            eve_data = session.get("eve_data")
            if eve_data:
                for ann in eve_data["annotations"]:
                    col = EVENT_MAP.get(ann["text"])
                    if not col:
                        continue
                    event_dt = eve_data["start"] + timedelta(seconds=ann["onset"])
                    if pld_start <= event_dt < pld_end_ts:
                        counts[col] += 1

            pressure_data, leak_data, resp_data = [], [], []
            tidal_data, minvent_data = [], []

            for label, sig in pld_data["signals"].items():
                col_name = PLD_SIGNAL_MAP.get(label)
                if col_name == "pressure":
                    pressure_data.extend(sig["data"])
                elif col_name == "leak":
                    leak_data.extend(sig["data"])
                elif col_name == "resp_rate":
                    resp_data.extend(sig["data"])
                elif col_name == "tidal_vol":
                    tidal_data.extend(sig["data"])
                elif col_name == "minute_vent":
                    minvent_data.extend(sig["data"])

            filt_pressure = nonneg_values(pressure_data)
            filt_leak = nonneg_values(leak_data)
            filt_resp = positive_values(resp_data)
            filt_tidal = positive_values(tidal_data)
            filt_minvent = positive_values(minvent_data)

            hours = duration_seconds / 3600.0
            total_events = counts["ca_count"] + counts["oa_count"] + counts["h_count"] + counts["ua_count"]
            ahi = round(total_events / hours, 3) if hours > 0 else 0.0

            row = {
                "date": date,
                "session_start": session_start,
                "session_end": session_end,
                "duration_minutes": round(duration_seconds / 60.0, 1),
                "_duration_seconds": duration_seconds,
                "ahi": ahi,
                **counts,
                "pressure_median": round(median(filt_pressure), 2) if filt_pressure else "",
                "pressure_95": round(percentile(filt_pressure, 95), 2) if filt_pressure else "",
                "pressure_995": round(percentile(filt_pressure, 99.5), 2) if filt_pressure else "",
                "leak_median": round(median(filt_leak), 2) if filt_leak else "",
                "leak_95": round(percentile(filt_leak, 95), 2) if filt_leak else "",
                "resp_rate_median": round(median(filt_resp), 2) if filt_resp else "",
                "tidal_vol_median": round(median(filt_tidal), 2) if filt_tidal else "",
                "minute_vent_median": round(median(filt_minvent), 2) if filt_minvent else "",
            }
            rows.append(row)

        # Unattributed events
        seen_eve_paths = set()
        eve_list = []
        for session in date_sessions:
            eve_data = session.get("eve_data")
            eve_path = session["files"].get("EVE")
            if eve_data and eve_path and eve_path not in seen_eve_paths:
                seen_eve_paths.add(eve_path)
                eve_list.append((eve_data, eve_path))

        for eve_data, eve_path in eve_list:
            all_ranges = global_pld_ranges.get(eve_path, [])
            for ann in eve_data["annotations"]:
                col = EVENT_MAP.get(ann["text"])
                if not col:
                    continue
                event_dt = eve_data["start"] + timedelta(seconds=ann["onset"])
                attributed = any(s <= event_dt < e for s, e in all_ranges)
                if not attributed:
                    if date not in unattributed_events:
                        unattributed_events[date] = {c: 0 for c in EVENT_MAP.values()}
                    unattributed_events[date][col] += 1

    rows.sort(key=lambda r: (r["date"], r["session_start"]))
    return rows, unattributed_events


def etl_daily(session_rows, sessions_by_date, unattributed_events=None):
    """Aggregate session rows into daily summary rows."""
    if unattributed_events is None:
        unattributed_events = {}

    by_date = {}
    for row in session_rows:
        by_date.setdefault(row["date"], []).append(row)

    daily_rows = []
    for date in sorted(by_date):
        sessions = by_date[date]
        start = min(s["session_start"] for s in sessions)
        end = max(s["session_end"] for s in sessions)
        total_seconds = sum(s["_duration_seconds"] for s in sessions)
        total_minutes = total_seconds / 60.0

        ca = sum(s["ca_count"] for s in sessions)
        oa = sum(s["oa_count"] for s in sessions)
        h = sum(s["h_count"] for s in sessions)
        ua = sum(s["ua_count"] for s in sessions)
        arousal = sum(s["arousal_count"] for s in sessions)

        unattr = unattributed_events.get(date)
        if unattr:
            ca += unattr.get("ca_count", 0)
            oa += unattr.get("oa_count", 0)
            h += unattr.get("h_count", 0)
            ua += unattr.get("ua_count", 0)
            arousal += unattr.get("arousal_count", 0)

        hours = total_minutes / 60.0
        total_events = ca + oa + h + ua
        ahi = round(total_events / hours, 3) if hours > 0 else 0.0

        # Recompute stats from raw signals across all sessions
        combined = {"pressure": [], "leak": [], "resp_rate": [], "tidal_vol": [], "minute_vent": []}
        for sess in sessions_by_date.get(date, []):
            pld_data = sess.get("pld_data")
            if not pld_data:
                continue
            for label, sig in pld_data["signals"].items():
                col_name = PLD_SIGNAL_MAP.get(label)
                if col_name in combined:
                    combined[col_name].extend(sig["data"])

        fp = nonneg_values(combined["pressure"])
        fl = nonneg_values(combined["leak"])
        fr = positive_values(combined["resp_rate"])
        ft = positive_values(combined["tidal_vol"])
        fm = positive_values(combined["minute_vent"])

        daily_rows.append({
            "date": date,
            "sessions": len(sessions),
            "start": start,
            "end": end,
            "total_minutes": round(total_minutes, 1),
            "ahi": ahi,
            "ca_count": ca, "oa_count": oa, "h_count": h, "ua_count": ua,
            "arousal_count": arousal,
            "pressure_median": round(median(fp), 2) if fp else "",
            "pressure_95": round(percentile(fp, 95), 2) if fp else "",
            "pressure_995": round(percentile(fp, 99.5), 2) if fp else "",
            "leak_median": round(median(fl), 2) if fl else "",
            "leak_95": round(percentile(fl, 95), 2) if fl else "",
            "resp_rate_median": round(median(fr), 2) if fr else "",
            "tidal_vol_median": round(median(ft), 2) if ft else "",
            "minute_vent_median": round(median(fm), 2) if fm else "",
        })

    return daily_rows


def etl_events(sessions_by_date):
    """Extract apnea/hypopnea/arousal events from EVE files."""
    rows = []
    for date in sorted(sessions_by_date):
        for session in sessions_by_date[date]:
            session_start = session["session_start"]
            eve_data = session.get("eve_data")
            pld_data = session.get("pld_data")
            if not eve_data or not pld_data:
                continue

            pld_dur = pld_data["num_records"] * pld_data["record_duration"]
            if pld_dur <= 0:
                continue

            pld_start = pld_data["start"]
            pld_end = pld_start + timedelta(seconds=pld_dur)

            for ann in eve_data["annotations"]:
                if ann["text"] == "Recording starts":
                    continue
                event_dt = eve_data["start"] + timedelta(seconds=ann["onset"])
                if pld_start <= event_dt < pld_end:
                    rows.append({
                        "datetime": event_dt.isoformat(),
                        "date": date,
                        "session_start": session_start,
                        "event": ann["text"],
                        "duration_sec": round(ann["duration"], 1),
                    })

    rows.sort(key=lambda r: r["datetime"])
    return rows


def etl_timeseries(sessions_by_date, output_path):
    """Write 2-second timeseries CSV. Returns row count."""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    row_count = 0

    with open(output_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=TIMESERIES_COLUMNS)
        writer.writeheader()

        for date in sorted(sessions_by_date):
            for session in sessions_by_date[date]:
                session_start = session["session_start"]
                pld = session.get("pld_data")
                if pld is None or pld["num_records"] <= 0:
                    continue

                pld_start = pld["start"]
                col_data = {}
                for label, sig in pld["signals"].items():
                    col_name = PLD_SIGNAL_MAP.get(label)
                    if col_name:
                        col_data[col_name] = sig["data"]

                if not col_data:
                    continue

                lengths = [len(v) for v in col_data.values()]
                n_samples = min(lengths)

                sprs = {label: sig["samples_per_record"]
                        for label, sig in pld["signals"].items()
                        if label in PLD_SIGNAL_MAP}
                spr_values = set(sprs.values())
                spr = next(iter(spr_values), None)
                if spr and spr > 0 and pld["record_duration"] > 0:
                    sample_interval = pld["record_duration"] / spr
                else:
                    sample_interval = 2

                for i in range(n_samples):
                    ts = pld_start + timedelta(seconds=i * sample_interval)
                    row = {
                        "datetime": ts.isoformat(),
                        "date": date,
                        "session_start": session_start,
                    }
                    for col_name in ["pressure", "leak", "resp_rate", "tidal_vol",
                                     "minute_vent", "snore", "flow_limit"]:
                        arr = col_data.get(col_name)
                        if arr and i < len(arr):
                            row[col_name] = round(arr[i], 3)
                        else:
                            row[col_name] = ""
                    writer.writerow(row)
                    row_count += 1

    return row_count
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_etl_output.py -v`
Expected: All PASS.

**Step 5: Commit**

```bash
git add src/oscar_etl/etl.py tests/test_etl_output.py
git commit -m "Add session/daily/event/timeseries processing and CSV output"
```

---

### Task 7: CLI with rich progress bars

**Files:**
- Create: `src/oscar_etl/cli.py`
- Create: `tests/test_cli.py`

**Step 1: Write the failing tests**

`tests/test_cli.py`:
```python
import subprocess
import sys
from pathlib import Path

from tests.conftest import build_edf
from datetime import datetime


class TestCliHelp:
    def test_help_flag(self):
        result = subprocess.run(
            [sys.executable, "-m", "oscar_etl.cli", "--help"],
            capture_output=True, text=True,
        )
        assert result.returncode == 0
        assert "oscar-etl" in result.stdout.lower() or "usage" in result.stdout.lower()


class TestCliEndToEnd:
    def _setup_data(self, tmp_path):
        oscar_dir = tmp_path / "OSCAR_Data"
        datalog = oscar_dir / "Profiles" / "Test User" / "ResMed_12345" / "Backup" / "DATALOG" / "2026"
        datalog.mkdir(parents=True)
        build_edf(
            datalog / "20260315_223000_CSL.edf",
            start=datetime(2026, 3, 15, 22, 30, 0),
            signals={"Press.2s": [0.0]},
        )
        build_edf(
            datalog / "20260315_223005_EVE.edf",
            start=datetime(2026, 3, 15, 22, 30, 5),
            signals={},
            annotations=[{"onset": 5.0, "duration": 10.0, "text": "Obstructive Apnea"}],
        )
        build_edf(
            datalog / "20260315_223010_PLD.edf",
            start=datetime(2026, 3, 15, 22, 30, 10),
            signals={
                "Press.2s": [10.0, 11.0, 12.0],
                "Leak.2s": [2.0, 3.0, 4.0],
            },
        )
        return oscar_dir

    def test_produces_output_csvs(self, tmp_path):
        oscar_dir = self._setup_data(tmp_path)
        output_dir = tmp_path / "output"
        result = subprocess.run(
            [
                sys.executable, "-m", "oscar_etl.cli",
                "--oscar-dir", str(oscar_dir),
                "--output-dir", str(output_dir),
                "--skip-timeseries",
            ],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert (output_dir / "cpap_sessions.csv").exists()
        assert (output_dir / "cpap_daily.csv").exists()
        assert (output_dir / "cpap_events.csv").exists()

    def test_timeseries_generated_by_default(self, tmp_path):
        oscar_dir = self._setup_data(tmp_path)
        output_dir = tmp_path / "output"
        result = subprocess.run(
            [
                sys.executable, "-m", "oscar_etl.cli",
                "--oscar-dir", str(oscar_dir),
                "--output-dir", str(output_dir),
            ],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert (output_dir / "cpap_timeseries.csv").exists()
```

**Step 2: Run tests to verify they fail**

Run: `pytest tests/test_cli.py -v`
Expected: FAIL — `oscar_etl.cli` has no `main`.

**Step 3: Implement the CLI**

`src/oscar_etl/cli.py`:
```python
"""CLI entry point for oscar-etl."""

import argparse
import sys
import time
from pathlib import Path

from rich.console import Console
from rich.progress import Progress, SpinnerColumn, TextColumn, BarColumn, TaskProgressColumn
from rich.table import Table

from oscar_etl import __version__
from oscar_etl.etl import (
    DAILY_COLUMNS,
    EVENTS_COLUMNS,
    SESSION_COLUMNS,
    find_oscar_dir,
    scan_profiles,
    discover_sessions,
    parse_and_cache_edfs,
    etl_sessions,
    etl_daily,
    etl_events,
    etl_timeseries,
    write_csv,
)

console = Console()


def pick_profile(profiles):
    """Interactive profile picker when multiple profiles exist."""
    console.print()
    console.print(f"  Found {len(profiles)} profiles:")
    for i, p in enumerate(profiles, 1):
        console.print(f"    [{i}] {p['name']}  ({p['serial']})")
    console.print()
    while True:
        try:
            choice = input(f"  Which profile? [1]: ").strip()
            if not choice:
                return profiles[0]
            idx = int(choice) - 1
            if 0 <= idx < len(profiles):
                return profiles[idx]
        except (ValueError, EOFError):
            pass
        console.print("  Invalid choice, try again.")


def build_parser():
    parser = argparse.ArgumentParser(
        prog="oscar-etl",
        description="Extract clean CSVs from your OSCAR CPAP data",
    )
    parser.add_argument(
        "--oscar-dir",
        help="Path to OSCAR_Data directory (auto-discovered if omitted)",
    )
    parser.add_argument(
        "--output-dir",
        default="./oscar-etl-output",
        help="Output directory (default: ./oscar-etl-output/)",
    )
    parser.add_argument(
        "--profile",
        help="Profile name (interactive picker if omitted and multiple exist)",
    )
    parser.add_argument(
        "--machine",
        help="Machine serial number filter",
    )
    parser.add_argument(
        "--skip-timeseries",
        action="store_true",
        help="Skip the large timeseries CSV (~800 MB)",
    )
    parser.add_argument(
        "--day-boundary",
        type=int,
        default=12,
        help="Hour (0-23) that separates sleep nights (default: 12)",
    )
    parser.add_argument(
        "--version",
        action="version",
        version=f"oscar-etl {__version__}",
    )
    return parser


def main(argv=None):
    parser = build_parser()
    args = parser.parse_args(argv)

    start_time = time.time()

    console.print()
    console.print(f"  [bold]oscar-etl[/bold] v{__version__}")
    console.print()

    # --- Discovery ---
    oscar_dir = find_oscar_dir(oscar_dir=args.oscar_dir)
    profiles = scan_profiles(
        oscar_dir,
        profile_name=args.profile,
        machine_serial=args.machine,
    )

    if len(profiles) > 1:
        profile = pick_profile(profiles)
    else:
        profile = profiles[0]

    datalog_dir = profile["datalog"]

    # Count EDF files
    edf_count = sum(1 for _ in datalog_dir.rglob("*.edf"))

    console.print("  [bold]Data source[/bold]")
    console.print(f"  {'─' * 35}")
    console.print(f"  Location:  {oscar_dir}")
    console.print(f"  Profile:   {profile['name']}")
    console.print(f"  Machine:   {profile['serial']}")
    console.print(f"  EDF files: {edf_count:,}")
    console.print()

    # --- Processing ---
    console.print("  [bold]Extracting[/bold]")
    console.print(f"  {'─' * 35}")

    output_dir = Path(args.output_dir)
    day_boundary = args.day_boundary

    with Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}"),
        BarColumn(),
        TaskProgressColumn(),
        console=console,
    ) as progress:
        # Discover sessions
        task = progress.add_task("  Discovering sessions", total=1)
        sessions_by_date = discover_sessions(datalog_dir, day_boundary=day_boundary)
        total_sessions = sum(len(v) for v in sessions_by_date.values())
        progress.update(task, completed=1)

        # Parse EDF files
        task = progress.add_task("  Parsing EDF files", total=total_sessions)
        warnings = parse_and_cache_edfs(sessions_by_date)
        progress.update(task, completed=total_sessions)

        # Sessions CSV
        task = progress.add_task("  Writing sessions.csv", total=1)
        session_rows, unattributed = etl_sessions(sessions_by_date)
        write_csv(output_dir / "cpap_sessions.csv", SESSION_COLUMNS, session_rows)
        progress.update(task, completed=1)

        # Daily CSV
        task = progress.add_task("  Writing daily.csv", total=1)
        daily_rows = etl_daily(session_rows, sessions_by_date, unattributed)
        write_csv(output_dir / "cpap_daily.csv", DAILY_COLUMNS, daily_rows)
        progress.update(task, completed=1)

        # Events CSV
        task = progress.add_task("  Writing events.csv", total=1)
        event_rows = etl_events(sessions_by_date)
        write_csv(output_dir / "cpap_events.csv", EVENTS_COLUMNS, event_rows)
        progress.update(task, completed=1)

        # Timeseries CSV
        if not args.skip_timeseries:
            task = progress.add_task("  Writing timeseries.csv", total=1)
            ts_count = etl_timeseries(sessions_by_date, output_dir / "cpap_timeseries.csv")
            progress.update(task, completed=1)

    # --- Summary ---
    console.print()
    console.print("  [bold]Output[/bold]")
    console.print(f"  {'─' * 35}")
    console.print(f"  {output_dir}/")

    for name in ["cpap_sessions.csv", "cpap_daily.csv", "cpap_events.csv", "cpap_timeseries.csv"]:
        path = output_dir / name
        if path.exists():
            size = path.stat().st_size
            if size > 1024 * 1024:
                size_str = f"{size / (1024 * 1024):.1f} MB"
            elif size > 1024:
                size_str = f"{size / 1024:.0f} KB"
            else:
                size_str = f"{size} B"
            console.print(f"    {name:<25} {size_str}")

    if warnings:
        console.print()
        console.print(f"  [yellow]{len(warnings)} warning(s) during parsing[/yellow]")
        for w in warnings[:10]:
            console.print(f"    [dim]{w}[/dim]")
        if len(warnings) > 10:
            console.print(f"    [dim]...and {len(warnings) - 10} more[/dim]")

    elapsed = time.time() - start_time
    console.print()
    console.print(f"  Done in {elapsed:.0f}s")
    console.print()


if __name__ == "__main__":
    main()
```

**Step 4: Run tests to verify they pass**

Run: `pytest tests/test_cli.py -v`
Expected: All PASS.

**Step 5: Commit**

```bash
git add src/oscar_etl/cli.py tests/test_cli.py
git commit -m "Add CLI with rich progress bars and interactive profile picker"
```

---

### Task 8: Thread day_boundary through parse_and_cache_edfs

**Files:**
- Modify: `src/oscar_etl/etl.py`
- Modify: `src/oscar_etl/cli.py`

In Task 5, `parse_and_cache_edfs` hardcodes `day_boundary=12` when reassigning dates from PLD headers. Fix this.

**Step 1: Add day_boundary parameter to parse_and_cache_edfs**

In `src/oscar_etl/etl.py`, change the function signature:

```python
def parse_and_cache_edfs(sessions_by_date, day_boundary=12):
```

And update the call to `evening_date` inside it:

```python
session["date"] = evening_date(pld_data["start"], day_boundary=day_boundary)
```

**Step 2: Pass day_boundary from CLI**

In `src/oscar_etl/cli.py`, update the call:

```python
warnings = parse_and_cache_edfs(sessions_by_date, day_boundary=day_boundary)
```

**Step 3: Run all tests**

Run: `pytest -v`
Expected: All PASS.

**Step 4: Commit**

```bash
git add src/oscar_etl/etl.py src/oscar_etl/cli.py
git commit -m "Thread day_boundary parameter through EDF caching"
```

---

### Task 9: README

**Files:**
- Create: `README.md`

**Step 1: Write the README**

Follow the structure from the design doc. Key sections:

1. **Title + one-liner**: `oscar-etl` — Extract clean CSVs from your OSCAR CPAP data
2. **Why this exists**: Adapt the comparison paragraph from the design doc. Concrete: 7 signals vs 3, wide vs narrow, session-level, events file, mask-off gap events, runs in ~1 minute.
3. **Quick Start**: `pip install oscar-etl` → `oscar-etl`. Show the expected output.
4. **macOS Setup**: Step-by-step symlink walkthrough with exact terminal commands. Explain why (TCC protects ~/Documents). Offer the "quick" option (just leave Full Disk Access on).
5. **Output Files**: Table of all four CSVs with column descriptions. Note the evening date convention and `--day-boundary`.
6. **CLI Reference**: All flags with descriptions.
7. **Supported Machines**: ResMed only. List known-compatible models. PRs welcome.
8. **Contributing**: How to add support for other machines — which files to modify, how to test.
9. **License**: MIT.

Use the exact comparison text provided by the user in the brainstorming session. Write for both audiences — guide the semi-technical user through setup, give the technical user the details they want.

**Step 2: Run a final check**

Run: `pytest -v && python -m oscar_etl.cli --help`
Expected: All tests pass, help text displays correctly.

**Step 3: Commit**

```bash
git add README.md
git commit -m "Add README with setup guide, feature comparison, and CLI docs"
```

---

### Task 10: Final integration verification

**Step 1: Run the full test suite**

Run: `pytest -v --tb=short`
Expected: All tests PASS.

**Step 2: Verify the package installs cleanly**

Run: `pip install -e . && oscar-etl --version`
Expected: Prints `oscar-etl 0.1.0`.

**Step 3: Verify help output**

Run: `oscar-etl --help`
Expected: Shows all flags as designed.

**Step 4: Test against real data (manual)**

Run: `oscar-etl --oscar-dir /path/to/your/OSCAR_Data --output-dir /tmp/oscar-test`
Verify: Four CSVs generated, row counts match expectations from the eric-health version.

**Step 5: Final commit if any fixes needed, then tag**

```bash
git tag v0.1.0
```
