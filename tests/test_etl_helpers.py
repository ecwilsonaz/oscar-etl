from datetime import datetime

from oscar_etl.etl import (
    evening_date,
    median,
    nonneg_values,
    percentile,
    positive_values,
)


class TestPercentile:
    def test_median_of_three(self):
        assert percentile([1, 2, 3], 50) == 2.0

    def test_95th(self):
        data = list(range(1, 101))
        assert abs(percentile(data, 95) - 95.05) < 0.1

    def test_empty(self):
        assert percentile([], 50) == 0.0

    def test_single_value(self):
        assert percentile([42], 50) == 42

    def test_99_5th(self):
        data = list(range(1, 1001))
        result = percentile(data, 99.5)
        assert 995 < result < 1000


class TestMedian:
    def test_odd(self):
        assert median([3, 1, 2]) == 2.0

    def test_even(self):
        assert median([1, 2, 3, 4]) == 2.5


class TestFilters:
    def test_nonneg_includes_zero(self):
        assert nonneg_values([-1, 0, 1, 2]) == [0, 1, 2]

    def test_positive_excludes_zero(self):
        assert positive_values([-1, 0, 1, 2]) == [1, 2]


class TestEveningDate:
    def test_evening_session(self):
        dt = datetime(2026, 3, 15, 22, 30, 0)
        assert evening_date(dt, day_boundary=12) == "2026-03-15"

    def test_after_midnight_session(self):
        dt = datetime(2026, 3, 16, 1, 30, 0)
        assert evening_date(dt, day_boundary=12) == "2026-03-15"

    def test_noon_boundary(self):
        dt = datetime(2026, 3, 15, 12, 0, 0)
        assert evening_date(dt, day_boundary=12) == "2026-03-15"

    def test_just_before_noon(self):
        dt = datetime(2026, 3, 15, 11, 59, 0)
        assert evening_date(dt, day_boundary=12) == "2026-03-14"

    def test_custom_day_boundary(self):
        dt = datetime(2026, 3, 15, 8, 0, 0)
        assert evening_date(dt, day_boundary=18) == "2026-03-14"

    def test_custom_boundary_after(self):
        dt = datetime(2026, 3, 15, 19, 0, 0)
        assert evening_date(dt, day_boundary=18) == "2026-03-15"
