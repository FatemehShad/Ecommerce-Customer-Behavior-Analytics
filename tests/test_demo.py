import csv

import pytest

from ecommerce_analytics.demo import generate_events


def test_demo_is_deterministic_and_protects_existing_files(tmp_path):
    first, second = tmp_path / "a.csv", tmp_path / "b.csv"
    generate_events(str(first), rows=100)
    generate_events(str(second), rows=100)
    assert first.read_bytes() == second.read_bytes()
    with first.open() as handle:
        assert len(list(csv.DictReader(handle))) == 100
    with pytest.raises(FileExistsError):
        generate_events(str(first))
    with pytest.raises(ValueError, match="positive"):
        generate_events(str(tmp_path / "bad.csv"), rows=0)
