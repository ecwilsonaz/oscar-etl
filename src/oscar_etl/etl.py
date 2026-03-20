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
            if candidate.is_symlink():
                target = candidate.resolve()
                if target.is_dir():
                    return target
            continue

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
