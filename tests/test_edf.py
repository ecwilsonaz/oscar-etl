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
        path = tmp_edf(signals={"Press.2s": [10.0, 11.0, 12.0]})
        data = path.read_bytes()
        path.write_bytes(data[:-10])
        result = parse_edf(path)
        assert result["start"] is not None

    def test_annotation_without_duration(self, tmp_edf):
        path = tmp_edf(
            signals={"Press.2s": [10.0]},
            annotations=[
                {"onset": 60.0, "text": "Recording starts"},
            ],
        )
        result = parse_edf(path)
        ann = [a for a in result["annotations"] if a["text"] == "Recording starts"]
        assert len(ann) == 1
        assert ann[0]["duration"] == 0.0

    def test_returns_warnings_list(self, tmp_edf):
        path = tmp_edf(signals={"Press.2s": [10.0]})
        result = parse_edf(path)
        assert "warnings" in result
        assert isinstance(result["warnings"], list)
