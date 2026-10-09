import pytest

from ecommerce_analytics.spark import local_spark


@pytest.fixture(scope="session")
def spark():
    session = local_spark("local[2]", 2)
    yield session
    session.stop()


@pytest.fixture
def raw_events(spark):
    # Ordered funnel, duplicate, wrong-order purchase, new session, and invalid input.
    rows = [
        ("2025-01-01T10:00:00Z", "u1", "view", "p1", ""),
        ("2025-01-01T10:00:00Z", "u1", "view", "p1", ""),
        ("2025-01-01T10:01:00Z", "u1", "addtocart", "p1", ""),
        ("2025-01-01T10:02:00Z", "u1", "transaction", "p1", "t1"),
        ("2025-01-01T11:00:00Z", "u1", "view", "p2", ""),
        ("2025-01-01T10:00:00Z", "u2", "transaction", "p1", "t2"),
        ("2025-01-01T10:01:00Z", "u2", "view", "p1", ""),
        ("2025-01-01T10:02:00Z", "u2", "addtocart", "p1", ""),
        ("bad-time", "u3", "view", "p1", ""),
        ("2025-01-01T10:00:00Z", "", "view", "p1", ""),
        ("2025-01-01T10:00:00Z", "u3", "click", "p1", ""),
        ("2025-01-01T10:00:00Z", "u3", "view", "", ""),
    ]
    return spark.createDataFrame(
        rows,
        "timestamp string, visitorid string, event string, itemid string, transactionid string",
    )
