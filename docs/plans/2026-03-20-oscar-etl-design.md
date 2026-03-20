# oscar-etl: Design Document

**Goal:** Extract the CPAP ETL script from `eric-health` into a standalone, open-source PyPI package that reads ResMed EDF files from OSCAR's data directory and produces clean CSVs.

**Date:** 2026-03-20

---

## Why This Exists

OSCAR's manual export gives you three flat CSV dumps — a daily summary, per-session stats, and a massive interleaved details file mixing pressure readings with apnea events in a narrow format. It includes only three signals (pressure, EPAP, flow limitation), omits leak, respiratory rate, tidal volume, minute ventilation, and snore entirely, and uses BiPAP column headers that are always zero for APAP machines. Session boundaries exist only in the OSCAR GUI — the CSVs don't make it easy to correlate events with specific mask-on segments.

oscar-etl reads the same raw EDF files OSCAR does, but produces cleaner, more complete output. It splits sessions by each mask-on segment (matching OSCAR's internal model), gives you seven signals in a wide-format timeseries CSV (one row per 2-second timestamp instead of one row per measurement), and separates events into their own file for easy correlation with sleep-tracker data. Daily stats are recomputed from the raw signal data across all sessions — not approximated from the longest session or averaged. It also captures events that happen during mask-off gaps (which OSCAR's export silently drops), and the whole thing runs in about a minute with no manual clicking.

---

## Target Audience

Both technical and semi-technical CPAP users:

- **Technical users** feed the CSVs into their own scripts, Jupyter notebooks, or data tools.
- **Semi-technical users** follow step-by-step terminal instructions to get clean CSVs for Excel, Google Sheets, or AI tools like ChatGPT/Claude.

The README serves both: a Quick Start for the semi-technical user, and CLI Reference / Contributing sections for the technical one.

---

## Distribution

PyPI package. Install with `pip install oscar-etl`, run with the `oscar-etl` command.

**Single dependency:** `rich` for progress bars and formatted output. The EDF parser core uses only the Python standard library (`struct`, `csv`, `re`, `pathlib`, `datetime`).

---

## Project Structure

```
oscar-etl/
├── pyproject.toml          # Package metadata, rich dependency, CLI entry point
├── README.md               # Setup guide, feature comparison, output docs
├── LICENSE                  # MIT
├── src/
│   └── oscar_etl/
│       ├── __init__.py     # Version string
│       ├── cli.py          # argparse, rich progress, entry point
│       ├── edf.py          # Hand-rolled EDF parser (pyedflib rejects ResMed files)
│       └── etl.py          # Discovery, session grouping, stats, CSV writers
```

Three modules:

- **`edf.py`** — The core. A general-purpose ResMed EDF parser, independent of the rest. Battle-tested against 6 years of real data through 3 review rounds.
- **`etl.py`** — Everything else: OSCAR data discovery, profile/machine resolution, session grouping (CSL = power-on, PLD = mask-on), event attribution, statistics, and CSV output.
- **`cli.py`** — Thin shell wiring argparse and `rich` progress bars to `etl.py`.

---

## Data Discovery

The tool auto-discovers OSCAR's data directory. Platform defaults:

| Platform | Primary path | Fallback |
|----------|-------------|----------|
| macOS | `~/Documents/OSCAR_Data/` | Follow symlink to real path |
| Windows | `~/Documents/OSCAR_Data/` | — |
| Linux | `~/.local/share/OSCAR_Data/` | `~/Documents/OSCAR_Data/` |
| All | `--oscar-dir` CLI override | — |

> **Note:** The Linux path follows the freedesktop.org XDG standard but has not been verified firsthand. Corrections welcome.

### Discovery flow

1. `--oscar-dir` passed? Use it directly.
2. No flag? Try platform default.
3. macOS permission denied? Check if it's a symlink, follow to real path.
4. Still nothing? Print error with setup instructions.

### Multiple profiles or machines

The OSCAR data structure is `Profiles/<Name>/ResMed_<serial>/Backup/DATALOG/`.

- One profile + one machine: use it, print what was found.
- Multiple: show a numbered list, let the user pick interactively (`[1]`, `[2]`, etc.). Default to `[1]` so Enter works for the common case.
- `--profile` and `--machine` flags exist for scripting/automation.

### macOS permissions

`~/Documents/` is TCC-protected. The README provides two options:

- **Option A (recommended):** Grant Terminal Full Disk Access temporarily, move `OSCAR_Data` out of `~/Documents/`, symlink it back, revoke Full Disk Access. The tool then reads from the real path permanently.
- **Option B (quick):** Grant Terminal Full Disk Access and leave it on.

Windows and Linux require no special permissions.

---

## CLI Interface

```
oscar-etl [OPTIONS]

Options:
  --oscar-dir PATH       Path to OSCAR_Data directory (auto-discovered if omitted)
  --output-dir PATH      Output directory (default: ./oscar-etl-output/)
  --profile NAME         Profile name (interactive picker if omitted and multiple exist)
  --machine SERIAL       Machine serial (interactive picker if omitted and multiple exist)
  --skip-timeseries      Skip the large timeseries CSV (~800 MB)
  --day-boundary HOUR    Hour (0-23) that separates sleep nights (default: 12)
```

### User experience

Rich-formatted output with progress bars:

```
$ oscar-etl

  oscar-etl v0.1.0

  Data source
  ───────────────────────────────────
  Location:  ~/OSCAR_Data (via symlink)
  Profile:   Eric Wilson
  Machine:   ResMed 23192261096
  EDF files: 2,847

  Extracting
  ───────────────────────────────────
  Parsing EDF files    ━━━━━━━━━━━━━━━━━━━━ 840/840   100%
  Writing sessions.csv ━━━━━━━━━━━━━━━━━━━━ 840 rows   ✓
  Writing daily.csv    ━━━━━━━━━━━━━━━━━━━━ 840 rows   ✓
  Writing events.csv   ━━━━━━━━━━━━━━━━━━━━ 12,431 rows ✓
  Writing timeseries   ━━━━━━━━━━━━━━━━━━━━ 4.1M rows  ✓

  Output
  ───────────────────────────────────
  ./oscar-etl-output/
    cpap_sessions.csv    1.2 MB
    cpap_daily.csv       48 KB
    cpap_events.csv      312 KB
    cpap_timeseries.csv  833 MB

  Done in 54s
```

Error messages are actionable, with platform-specific guidance for macOS permission issues.

---

## Output Files

All CSVs use `date` as a join key. The `date` column uses an "evening date" convention: sessions starting before noon are attributed to the previous calendar day, matching how OSCAR and sleep trackers report dates. Shift workers with non-standard schedules can adjust this with `--day-boundary`.

### cpap_sessions.csv — one row per mask-on segment

```
date, session_start, session_end, duration_minutes,
ahi, ca_count, oa_count, h_count, ua_count, arousal_count,
pressure_median, pressure_95, pressure_995,
leak_median, leak_95,
resp_rate_median, tidal_vol_median, minute_vent_median
```

### cpap_daily.csv — one row per night

```
date, sessions, start, end, total_minutes,
ahi, ca_count, oa_count, h_count, ua_count, arousal_count,
pressure_median, pressure_95, pressure_995,
leak_median, leak_95,
resp_rate_median, tidal_vol_median, minute_vent_median
```

Daily stats are recomputed from raw signal data across all sessions for the night — not averaged from session-level percentiles. Mask-off gap events are included in daily totals.

### cpap_events.csv — one row per event

```
datetime, date, session_start, event, duration_sec
```

Events: Central Apnea, Obstructive Apnea, Hypopnea, Apnea (unclassified), Arousal.

### cpap_timeseries.csv — one row per 2-second sample (wide format)

```
datetime, date, session_start,
pressure, leak, resp_rate, tidal_vol, minute_vent, snore, flow_limit
```

Seven signals at 2-second resolution. Skippable with `--skip-timeseries`. Expect ~800 MB for several years of data.

---

## Scope

**ResMed only.** Tested against ResMed AirSense 10 data. Other ResMed models (AirSense 11, etc.) use the same EDF format but may have different signal labels — the tool warns on unrecognized labels rather than crashing.

PRs welcome from users with other machines (Philips, DeVilbiss, etc.) who can build and test against their own data.

---

## Changes From the Original Script

| Area | Original (eric-health) | oscar-etl |
|------|----------------------|-----------|
| Paths | Hardcoded to Eric Wilson / ResMed_23192261096 | Auto-discovered with CLI overrides |
| Output dir | `data/cpap/` relative to repo | `./oscar-etl-output/` |
| Dependencies | Zero | `rich` |
| Distribution | Script in a monorepo | PyPI package with `oscar-etl` CLI |
| UI | `print()` statements | `rich` progress bars and tables |
| Multi-profile | N/A | Interactive picker or `--profile` flag |
| Evening date | Hardcoded noon cutoff | `--day-boundary` flag (default 12) |
| Timeseries | Always generated | `--skip-timeseries` flag |
| Structure | Single 917-line file | 3 modules: edf.py, etl.py, cli.py |

**What stays the same:** The EDF parser, session discovery logic, all four CSV schemas, event attribution, daily aggregation from raw signals, and mask-off gap event capture.

---

## README Structure

1. One-line description
2. Why this exists (ETL vs OSCAR manual export comparison)
3. Quick Start (`pip install oscar-etl` → `oscar-etl`)
4. macOS Setup (symlink walkthrough)
5. Output Files (what each CSV contains, example rows, evening date convention, `--day-boundary`)
6. CLI Reference (all flags)
7. Supported Machines (ResMed only, PRs welcome)
8. Contributing (how to add support for other machines)
9. License (MIT)
