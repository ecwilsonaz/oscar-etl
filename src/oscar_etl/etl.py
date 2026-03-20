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
