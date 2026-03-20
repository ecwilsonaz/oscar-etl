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
        self._make_session(datalog_dir, 2026, "20260315_223000", ["CSL"])
        self._make_session(datalog_dir, 2026, "20260315_223005", ["EVE"])
        self._make_session(datalog_dir, 2026, "20260315_223010", ["PLD"])
        sessions = discover_sessions(datalog_dir, day_boundary=12)
        assert "2026-03-15" in sessions
        assert len(sessions["2026-03-15"]) == 1
        assert "PLD" in sessions["2026-03-15"][0]["files"]

    def test_multiple_pld_sessions(self, tmp_oscar_dir):
        _, datalog_dir = tmp_oscar_dir
        self._make_session(datalog_dir, 2026, "20260315_223000", ["CSL"])
        self._make_session(datalog_dir, 2026, "20260315_223005", ["EVE"])
        self._make_session(datalog_dir, 2026, "20260315_223010", ["PLD"])
        self._make_session(datalog_dir, 2026, "20260316_020000", ["PLD"])
        sessions = discover_sessions(datalog_dir, day_boundary=12)
        total = sum(len(v) for v in sessions.values())
        assert total == 2

    def test_skips_csl_only_period(self, tmp_oscar_dir):
        _, datalog_dir = tmp_oscar_dir
        self._make_session(datalog_dir, 2026, "20260315_223000", ["CSL"])
        sessions = discover_sessions(datalog_dir, day_boundary=12)
        assert len(sessions) == 0

    def test_after_midnight_uses_previous_date(self, tmp_oscar_dir):
        _, datalog_dir = tmp_oscar_dir
        self._make_session(datalog_dir, 2026, "20260316_013000", ["CSL"])
        self._make_session(datalog_dir, 2026, "20260316_013005", ["PLD"])
        sessions = discover_sessions(datalog_dir, day_boundary=12)
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
        warnings = sessions, _ = parse_and_cache_edfs(sessions)
        # Find the session (date may shift based on PLD header time)
        all_sessions = [s for date_sessions in sessions.values() for s in date_sessions]
        assert len(all_sessions) == 1
        session = all_sessions[0]
        assert session["pld_data"] is not None
        assert session["eve_data"] is not None
        assert "Press.2s" in session["pld_data"]["signals"]
