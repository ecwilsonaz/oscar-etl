import pytest

from oscar_etl.etl import (
    OscarDataNotFoundError,
    NoProfilesFoundError,
    find_oscar_dir,
    scan_profiles,
)


class TestFindOscarDir:
    def test_explicit_override(self, tmp_path):
        oscar_dir = tmp_path / "MyOscarData"
        oscar_dir.mkdir()
        result = find_oscar_dir(oscar_dir=str(oscar_dir))
        assert result == oscar_dir

    def test_explicit_override_missing_raises(self, tmp_path):
        with pytest.raises(OscarDataNotFoundError):
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
        oscar_dir = tmp_path / "OSCAR_Data"
        oscar_dir.mkdir()
        (oscar_dir / "Profiles").mkdir()
        with pytest.raises(NoProfilesFoundError):
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
