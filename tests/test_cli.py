import subprocess
import sys
from datetime import datetime

from tests.conftest import build_edf


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
        assert result.returncode == 0, f"stderr: {result.stderr}\nstdout: {result.stdout}"
        assert (output_dir / "cpap_sessions.csv").exists()
        assert (output_dir / "cpap_daily.csv").exists()
        assert (output_dir / "cpap_events.csv").exists()

    def test_main_module_invocation(self, tmp_path):
        """Verify python -m oscar_etl works."""
        oscar_dir = self._setup_data(tmp_path)
        output_dir = tmp_path / "output"
        result = subprocess.run(
            [
                sys.executable, "-m", "oscar_etl",
                "--oscar-dir", str(oscar_dir),
                "--output-dir", str(output_dir),
                "--skip-timeseries",
            ],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"
        assert (output_dir / "cpap_sessions.csv").exists()

    def test_day_boundary_flag(self, tmp_path):
        """Verify --day-boundary is accepted."""
        oscar_dir = self._setup_data(tmp_path)
        output_dir = tmp_path / "output"
        result = subprocess.run(
            [
                sys.executable, "-m", "oscar_etl.cli",
                "--oscar-dir", str(oscar_dir),
                "--output-dir", str(output_dir),
                "--skip-timeseries",
                "--day-boundary", "6",
            ],
            capture_output=True, text=True,
        )
        assert result.returncode == 0, f"stderr: {result.stderr}"

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
        assert result.returncode == 0, f"stderr: {result.stderr}\nstdout: {result.stdout}"
        assert (output_dir / "cpap_timeseries.csv").exists()
