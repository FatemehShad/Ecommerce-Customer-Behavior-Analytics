# Local validation record

Validated on 9 October 2026 with Python 3.12.14, Java 21, PySpark 4.0.1, and Delta Lake 4.0.1.

| Check | Observed result |
| --- | --- |
| `ruff check .` | Passed |
| `pytest -q` | 8 passed; zero failed or skipped |
| Package wheel build | Passed |
| Delta integration | Eight actual Delta tables created; repeated full refresh preserved expected counts |
| Synthetic large batch | 2,700,000 raw events; 2,700,000 valid unique events |
| Rejected / duplicate events in synthetic batch | 0 / 0 |
| Customer sessions in synthetic batch | 1,810,659 |
| Ordered cart / converted sessions | 635,447 / 253,894 |
| Output consistency and SQL examples | Passed; all 3 queries executed and returned rows |

The large batch used the generator's seed 42, a 4 GB Spark driver, `local[4]`, and 16 shuffle
partitions. It completed the entire bronze/silver/gold pipeline. The test fixture separately
exercises bad data, duplicates, and incorrect funnel ordering; the clean benchmark is not a
substitute for those checks.

These are **synthetic** local validation results. No original customer dataset was supplied,
and no real customer behaviour or revenue findings are claimed. The Databricks notebook uses
the same Python transformations, but execution in a Databricks workspace was not tested.
GitHub Actions is configured; this record does not assert a completed hosted CI run.

Reproduce the full workload with the commands in README.md. Run
`python scripts/validate_outputs.py --output output/synthetic-2700000 --expected-valid-rows 2700000`
to verify curated quality, reconcile session counts, check funnel stage consistency, and
execute all three SQL analysis examples against the generated Delta tables.
