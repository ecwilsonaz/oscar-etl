"""OSCAR data discovery, session processing, and CSV output."""

import csv
import os
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
            try:
                if candidate.is_symlink():
                    target = Path(os.readlink(candidate))
                    if not target.is_absolute():
                        target = candidate.parent / target
                    if target.is_dir():
                        return target
            except (PermissionError, OSError):
                pass
            continue

    if sys.platform == "darwin":
        print(
            "Error: Could not find OSCAR_Data directory.\n\n"
            "  If you haven't set up data access yet, see the macOS Setup\n"
            "  section in the README.\n\n"
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


def parse_and_cache_edfs(sessions_by_date, day_boundary=12):
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
                    session["date"] = evening_date(pld_data["start"], day_boundary)
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

    # Second pass: find unattributed events using the fully-built global_pld_ranges
    seen_eve_paths = set()
    for date in sorted(sessions_by_date):
        for session in sessions_by_date[date]:
            eve_data = session.get("eve_data")
            eve_path = session["files"].get("EVE")
            if not eve_data or not eve_path or eve_path in seen_eve_paths:
                continue
            seen_eve_paths.add(eve_path)

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
        ft_ = positive_values(combined["tidal_vol"])
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
            "tidal_vol_median": round(median(ft_), 2) if ft_ else "",
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
                    sample_interval = 2  # fallback for unknown sample rate
                    print(
                        f"  WARN: Could not determine sample interval for session "
                        f"{session_start}, defaulting to 2s",
                        file=sys.stderr,
                    )

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
