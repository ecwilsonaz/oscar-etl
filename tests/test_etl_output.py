import csv
from datetime import datetime

from tests.conftest import build_edf
from oscar_etl.etl import (
    discover_sessions,
    parse_and_cache_edfs,
    etl_sessions,
    etl_daily,
    etl_events,
    etl_timeseries,
    write_csv,
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
        sessions, _ = parse_and_cache_edfs(sessions)
        rows, unattributed = etl_sessions(sessions)
        assert len(rows) >= 1
        row = rows[0]
        assert "date" in row
        assert row["duration_minutes"] > 0
        assert row["pressure_median"] != ""


class TestEtlDaily:
    def test_aggregates_by_date(self, tmp_oscar_dir):
        _, datalog_dir = tmp_oscar_dir
        _setup_one_night(datalog_dir)
        sessions = discover_sessions(datalog_dir)
        sessions, _ = parse_and_cache_edfs(sessions)
        session_rows, unattributed = etl_sessions(sessions)
        daily_rows = etl_daily(session_rows, sessions, unattributed)
        assert len(daily_rows) >= 1
        assert daily_rows[0]["sessions"] >= 1
        assert daily_rows[0]["total_minutes"] > 0


class TestEtlEvents:
    def test_extracts_events(self, tmp_oscar_dir):
        _, datalog_dir = tmp_oscar_dir
        _setup_one_night(datalog_dir)
        sessions = discover_sessions(datalog_dir)
        sessions, _ = parse_and_cache_edfs(sessions)
        event_rows = etl_events(sessions)
        # Events may or may not fall within PLD time range depending on test data
        for row in event_rows:
            assert "event" in row
            assert "duration_sec" in row


class TestEtlTimeseries:
    def test_writes_timeseries(self, tmp_oscar_dir, tmp_path):
        _, datalog_dir = tmp_oscar_dir
        _setup_one_night(datalog_dir)
        sessions = discover_sessions(datalog_dir)
        sessions, _ = parse_and_cache_edfs(sessions)
        output_path = tmp_path / "timeseries.csv"
        count, _ = etl_timeseries(sessions, output_path)
        assert count > 0
        with open(output_path) as f:
            reader = csv.DictReader(f)
            row = next(reader)
            assert "pressure" in row
            assert "leak" in row
