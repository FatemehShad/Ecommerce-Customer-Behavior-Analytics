# Local validation record

Validated on 9 October 2026 with Python 3.12.14, Java 21, PySpark 4.0.1, and Delta Lake 4.0.1.

## Initial synthetic workload

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

These are **synthetic** local validation results from the initial implementation. The user
subsequently identified the source as the Retailrocket recommender system dataset on Kaggle.
Downloading its `events.csv` in this environment is currently blocked by the network proxy;
the required Kaggle/storage domains have been saved in the environment configuration draft.
No real Retailrocket customer behaviour or revenue findings are claimed. The Databricks notebook uses
the same Python transformations, but execution in a Databricks workspace was not tested.
GitHub Actions is configured; this record does not assert a completed hosted CI run.

Reproduce the full workload with the commands in the [setup guide](usage.md). Run
`python scripts/validate_outputs.py --output output/synthetic-2700000 --expected-valid-rows 2700000`
to verify curated quality, reconcile session counts, check funnel stage consistency, and
execute all three SQL analysis examples against the generated Delta tables.

## Retailrocket integration

After the dataset-specific updates, `ruff check .` passed and the complete Pytest suite
reported **11 passed, zero failed or skipped**.

The project now explicitly targets Kaggle's `retailrocket/ecommerce-dataset` and downloads
only `events.csv`. Download handling is checked using simulated raw CSV and ZIP responses,
header rejection, preservation of existing files, and reproducible SHA-256 calculation.
These offline tests do not establish successful download from Kaggle.

The session boundary calculation now retains timestamp precision: exactly 30 minutes stays
in a session, while 30 minutes plus one millisecond starts another. The regression fixture
checks both cases. Real-data execution remains pending network access to `www.kaggle.com`
and Kaggle download storage at `storage.googleapis.com`.

## Product KPI notebook — 14 October 2026

`notebooks/product_kpis_and_conversion.ipynb` was executed on the retained 2,700,000-event
synthetic lakehouse. All 10 code cells completed and five charts were embedded. The notebook
defines KPI denominators, analyses session/product funnel losses, segments observed visitor
behaviour, and reports visitor-level conversion confidence intervals and hypothesis tests.

The primary timing comparison used one reproducibly sampled eligible session per visitor:
71,780 weekday visitors and 28,220 weekend visitors. The synthetic example's two-sided
chi-square p-value was approximately 0.8503, with a weekend-minus-weekday difference of
approximately 0.05 percentage points (95% interval approximately −0.43 to 0.53 percentage
points). All six exploratory day comparisons completed with Holm correction. These are
demonstration results, not Retailrocket business findings or randomized experiment results.

The complete test suite reported **15 passed, zero failed or skipped** with the notebook
extra installed. Tests cover Wilson intervals, effect direction, sparse exact tests,
degenerate outcomes, and empty groups alongside the existing pipeline checks. Ruff passed.

The CI smoke workflow was also exercised locally: 1,000 synthetic events produced 669
sessions, then all 10 notebook code cells completed with four charts. Its missing timing
groups were correctly reported as unrun tests, and products below the minimum-volume
threshold did not produce an empty ranking chart. GitHub Actions now runs this workflow
and uploads the executed notebook; this record does not claim a completed hosted CI run.

### Notebook readability revision

The notebook now uses a numbered walkthrough with 18 shorter code cells, worked KPI examples,
explicit numerators and denominators, and percentage units. Hypothesis results use a compact
table and a plain-language conclusion; the additional weekday tests are marked optional.
Spark terminal progress bars are disabled for notebook runs, while warnings remain visible.

The revised calculations were executed on both the full synthetic lakehouse and the
1,000-event smoke lakehouse. All 18 code cells completed in each case, with five and four
charts respectively. The full run reproduced the earlier KPI totals, the 71,780/28,220
visitor comparison, and the p-value of approximately 0.8503. The small-data run correctly
reported missing groups as untested. Notebook schema validation, Ruff, and formatting checks
passed. The underlying statistical methods and data provenance are unchanged.
