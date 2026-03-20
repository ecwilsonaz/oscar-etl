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
