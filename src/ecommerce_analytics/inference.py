"""Visitor-level conversion inference; requires the project's notebooks extra."""

import math

from scipy.stats import chi2_contingency, fisher_exact
from statsmodels.stats.proportion import proportion_confint


def conversion_interval(successes: int, total: int) -> tuple[float, float]:
    """95% Wilson interval; no eligible observations produces an undefined interval."""
    if total < 0 or successes < 0 or successes > total:
        raise ValueError("Counts must satisfy 0 <= successes <= total")
    if total == 0:
        return math.nan, math.nan
    low, high = proportion_confint(successes, total, alpha=0.05, method="wilson")
    return float(low), float(high)


def compare_conversion_groups(
    successes_a: int, total_a: int, successes_b: int, total_b: int
) -> dict[str, float | str]:
    """Two-sided test and Newcombe-Wilson 95% interval for rate B minus rate A.

    Use independent visitor units, not repeated sessions. Choose Pearson's chi-square
    when all expected cell counts are >= 5; otherwise use Fisher's exact test.
    This comparison describes an association, not a causal or randomized A/B effect.
    """
    lower_a, upper_a = conversion_interval(successes_a, total_a)
    lower_b, upper_b = conversion_interval(successes_b, total_b)
    if total_a == 0 or total_b == 0:
        return {
            "status": "insufficient_data",
            "method": "not_run",
            "p_value": math.nan,
            "rate_a": successes_a / total_a if total_a else math.nan,
            "rate_b": successes_b / total_b if total_b else math.nan,
            "difference": math.nan,
            "ci_low": math.nan,
            "ci_high": math.nan,
        }
    rate_a, rate_b = successes_a / total_a, successes_b / total_b
    table = [[successes_a, total_a - successes_a], [successes_b, total_b - successes_b]]
    combined = total_a + total_b
    expected = [
        row_total * col_total / combined
        for row_total in [total_a, total_b]
        for col_total in [successes_a + successes_b, combined - successes_a - successes_b]
    ]
    if min(expected) >= 5:
        p_value = float(chi2_contingency(table, correction=False).pvalue)
        method = "Pearson chi-square"
    else:
        p_value = float(fisher_exact(table, alternative="two-sided").pvalue)
        method = "Fisher exact"
    difference = rate_b - rate_a
    return {
        "status": "tested",
        "method": method,
        "p_value": p_value,
        "rate_a": rate_a,
        "rate_b": rate_b,
        "difference": difference,
        "ci_low": difference - math.sqrt((rate_b - lower_b) ** 2 + (upper_a - rate_a) ** 2),
        "ci_high": difference + math.sqrt((upper_b - rate_b) ** 2 + (rate_a - lower_a) ** 2),
    }
