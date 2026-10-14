import math

import pytest

pytest.importorskip("scipy", reason="Install .[notebooks] to test statistical analysis")
pytest.importorskip("statsmodels", reason="Install .[notebooks] to test statistical analysis")

from scipy.stats import fisher_exact

from ecommerce_analytics.inference import (  # noqa: E402
    compare_conversion_groups,
    conversion_interval,
)


def test_known_wilson_interval_and_invalid_counts():
    assert conversion_interval(50, 100) == pytest.approx((0.40383153, 0.59616847))
    assert all(math.isnan(value) for value in conversion_interval(0, 0))
    with pytest.raises(ValueError):
        conversion_interval(11, 10)


def test_strong_difference_and_direction():
    comparison = compare_conversion_groups(100, 1000, 200, 1000)
    assert comparison["method"] == "Pearson chi-square"
    assert comparison["difference"] == pytest.approx(0.1)
    assert comparison["ci_low"] > 0
    assert comparison["p_value"] < 0.0001
    reverse = compare_conversion_groups(200, 1000, 100, 1000)
    assert reverse["p_value"] == pytest.approx(comparison["p_value"])
    assert reverse["ci_low"] == pytest.approx(-comparison["ci_high"])


def test_sparse_and_degenerate_tables_use_exact_test():
    result = compare_conversion_groups(1, 10, 4, 10)
    assert result["method"] == "Fisher exact"
    assert result["p_value"] == pytest.approx(fisher_exact([[1, 9], [4, 6]]).pvalue)
    assert compare_conversion_groups(0, 10, 0, 10)["p_value"] == 1
    assert compare_conversion_groups(10, 10, 10, 10)["p_value"] == 1


def test_empty_group_is_reported_as_unrun():
    result = compare_conversion_groups(0, 0, 5, 20)
    assert result["status"] == "insufficient_data"
    assert result["method"] == "not_run"
    assert math.isnan(result["p_value"])
