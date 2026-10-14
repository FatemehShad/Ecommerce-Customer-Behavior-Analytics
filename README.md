# E-commerce Customer Behaviour Analytics

A Python, SQL, PySpark, and Delta Lake personal project for exploring customer journeys,
product interactions, and conversion funnels at millions-of-events scale. Pytest validates
transformation contracts and actual Delta writes; GitHub Actions runs those checks on pushes
and pull requests.

**Input dataset:** [Retailrocket recommender system dataset on Kaggle](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset).
The analytics pipeline uses its `events.csv`, containing approximately 2.7 million customer
interactions. Download and run that file using the instructions below. Generated demo data
is synthetic and separate from Retailrocket; the initial local benchmark used synthetic data.
See [the local validation record](docs/validation.md) for the tested workload and its limits.

```mermaid
flowchart LR
    CSV[Event CSV] --> Bronze[Bronze: raw Delta]
    Bronze --> Silver[Silver: validated and deduplicated]
    Bronze --> Quarantine[Quarantine: rejected rows and reasons]
    Silver --> Sessions[Customer sessions]
    Sessions --> Journeys[Ordered product journeys]
    Journeys --> Gold[Gold: daily funnels and product metrics]
    Gold --> SQL[SQL analysis]
```

## Quick start

Requires Python 3.10–3.13 and Java 17 or 21. Tested dependencies are pinned in `pyproject.toml`.
Local Spark downloads the Delta JVM artifacts from Maven Central on first use; allow
`repo.maven.apache.org` and `repo1.maven.org` in addition to PyPI.

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'
export SPARK_LOCAL_IP=127.0.0.1
ecommerce-analytics generate --output data/demo.csv --rows 10000
ecommerce-analytics run --input data/demo.csv --output output/demo
ruff check .
pytest -q
```

The generator refuses to overwrite existing data. Reuse its CSV to rerun the pipeline.
If JVM artifact downloads cannot use your network proxy, prefetch verified jars with Python:

```bash
export SPARK_DELTA_JARS="$(python scripts/fetch_delta_jars.py --directory /tmp/ecommerce-delta-jars)"
```

This uses normal HTTPS verification and Maven Central's published artifact checksums.
Set the same variable for subsequent tests and CLI commands; the jar cache is reusable.

The pipeline **replaces the complete output tables** with each run. Always pass the complete
event corpus, use a dedicated output directory, and keep input and output separate. This is
a batch/full-refresh project, not an incremental or streaming ingestion system. Each Delta
table commits atomically, but a whole run is not a multi-table transaction: a failed run can
leave outputs from different runs, so consume results only after successful completion.

## Input contract

CSV files require a header. A file, directory, or Spark-supported glob can be supplied.
The input schema is Retailrocket's `events.csv`:

| Field | Meaning |
| --- | --- |
| `timestamp` | 13-digit Unix epoch milliseconds, or an ISO timestamp; UTC is used |
| `visitorid` | Customer identifier, retained as a string |
| `event` | `view`, `addtocart`/`cart`, or `transaction`/`purchase` |
| `itemid` | Product identifier, retained as a string |
| `transactionid` | Optional transaction identifier |

Missing customer/product IDs, malformed timestamps, and unknown event types are quarantined
with reasons. Malformed CSV structure fails ingestion instead of silently dropping rows.
Silver deduplicates on a SHA-256 hash of normalized customer, product, timestamp, event type,
and transaction ID. Legitimate identical events at the same timestamp cannot be distinguished
without a source event identifier. Purchase events are item interactions, not distinct orders.
No prices or quantities are available, so this project does not calculate revenue or AOV.

## Download and run Retailrocket

```bash
ecommerce-analytics download-retailrocket --output data/retailrocket/events.csv
export PYSPARK_SUBMIT_ARGS='--driver-memory 4g pyspark-shell'
ecommerce-analytics run --input data/retailrocket/events.csv \
  --output output/retailrocket --master 'local[4]' --shuffle-partitions 16
python scripts/validate_outputs.py --output output/retailrocket
```

The downloader targets only `events.csv` using Kaggle's public dataset endpoint, accepts CSV
or ZIP responses, validates the five-column header, and prints the source URL, file size,
and CSV SHA-256 for your provenance record. It retains HTTPS verification and refuses to
overwrite existing input. Download availability depends on Kaggle's current access rules.
If authentication or network restrictions prevent it, download from the linked dataset page,
unzip it locally, and place `events.csv` at `data/retailrocket/events.csv` before the run command.
Do not paste Kaggle credentials into code or chat.

Cloud network access needs `www.kaggle.com` and `storage.googleapis.com` for Kaggle and its
download storage. These additions have been saved in the environment draft; they must be
applied through environment settings before the blocked cloud download can be retried.
The initial attempt returned a proxy policy rejection, so real-data results are not yet
reported in this repository. Synthetic examples and offline tests require no Kaggle access.

The archive also includes `item_properties_part1.csv`, `item_properties_part2.csv`, and
`category_tree.csv`. They are optional enrichment sources and are not read by this event
analytics pipeline. `visitorid` identifies an anonymous visitor, not a verified customer
account; session and customer labels here describe observed visitor behaviour. Transaction
IDs occur on purchase events; blank transaction IDs on views/carts are expected. The anonymized
item-property files do not supply usable order revenue for this implementation.

Use the dataset under the terms and license on its Kaggle page. Raw customer interaction
data, downloaded archives, and derived Delta outputs remain Git-ignored.

## Analytics definitions and Delta tables

A session ends when the next event for that customer arrives **more than 30 minutes** later.
The threshold is configurable using `--session-minutes`. Exactly 30 minutes stays in one
session. IDs are stable for the same complete corpus; adding earlier events can change sessions.

Conversion requires a view, a subsequent cart, and a subsequent purchase for the **same product
in the same session**. Nondecreasing timestamps count; equal timestamps have no reliable
source ordering. Purchases before the qualifying cart do not count as funnel conversions.
Daily metrics attribute the whole journey to its session start date in UTC, including sessions
crossing midnight. A session contributes at most once to each daily funnel stage.

| Output under `--output` | Contents |
| --- | --- |
| `bronze/events` | Raw CSV fields |
| `silver/events` | Valid unique canonical events, partitioned by event date |
| `silver/quarantine` | Rejected raw records and reasons |
| `gold/session_events` | Event-to-session assignments, partitioned by event date |
| `gold/sessions` | Durations, event counts, product breadth, stage counts |
| `gold/session_products` | Ordered stage timestamps per session/product |
| `gold/daily_funnel` | Eligible session counts and conversion rates |
| `gold/product_metrics` | Product interaction and ordered conversion counts |

Daily view-to-cart rate = carted sessions / viewed sessions; cart-to-purchase rate = converted
sessions / carted sessions. NULL rates indicate an empty denominator. Gold product metrics
count session/product pairs. `sql/customer_behaviour.sql` provides repeat-customer, product
conversion, and cart abandonment queries. The executable daily SQL is `DAILY_FUNNEL_SQL` in
`src/ecommerce_analytics/pipeline.py` and is covered by tests.

## 2.7-million-event synthetic workload

```bash
ecommerce-analytics generate --output data/synthetic-2700000.csv --rows 2700000 --seed 42
export PYSPARK_SUBMIT_ARGS='--driver-memory 4g pyspark-shell'
ecommerce-analytics run --input data/synthetic-2700000.csv \
  --output output/synthetic-2700000 --master 'local[4]' --shuffle-partitions 16
python scripts/validate_outputs.py --output output/synthetic-2700000 \
  --expected-valid-rows 2700000
```

Generation streams to disk and transformations stay distributed. Spark collects only scalar
quality counts and summaries on the driver. Size the cluster, driver/executor memory, and
shuffle partitions to fit real data; the sample command is a local demonstration, not a
production cluster sizing recommendation. Generated data and Delta outputs are Git-ignored.

## Product KPI and statistical analysis notebook

Open [product_kpis_and_conversion.ipynb](notebooks/product_kpis_and_conversion.ipynb) in Jupyter
or VS Code. Follow its numbered steps from loading data to interpreting results. Short code
cells explain each calculation, KPI tables show their numerators and denominators, and rates
are displayed as percentages. It contains KPI definitions and a scorecard, funnel losses,
daily conversion trends, product cart abandonment rankings, and visitor segments. Statistical
analysis includes Wilson confidence intervals, a weekend/weekday conversion hypothesis, and weekday
comparisons with Holm correction. The committed notebook includes five charts and executed
**synthetic** example outputs, with the input source displayed throughout.
The hypothesis results include a plain-language interpretation, with a final reference table
for the statistical methods.

Inference uses one eligible viewed session per anonymous visitor, selected with a fixed,
outcome-independent hash rule. It reports effect sizes and uncertainty, chooses chi-square
or Fisher's exact test according to expected counts, and handles empty groups as unrun tests.
Calendar comparisons are observational associations; there is no randomized experiment
assignment in this dataset. Behavioural and product rankings are descriptive.

Install notebook dependencies and register a kernel in your local Python environment:

```bash
python -m pip install -e '.[dev,notebooks]'
python -m ipykernel install --user --name ecommerce-analytics --display-name 'Ecommerce Analytics'
export PYSPARK_SUBMIT_ARGS='--driver-memory 4g pyspark-shell'
# Requires complete Retailrocket Delta outputs from the earlier pipeline command.
python scripts/execute_kpi_notebook.py --source retailrocket
```

The runner writes a successfully executed copy to `artifacts/product_kpis.executed.ipynb`.
It fails when the selected source is missing; it never automatically substitutes synthetic
data. To reproduce the demonstration after generating the synthetic lakehouse above:

```bash
python scripts/execute_kpi_notebook.py --source synthetic
```

Use `--lakehouse <directory>` to read another complete pipeline output and `--kernel <name>`
to choose another registered kernel. For interactive execution, choose the registered kernel
and set `ECOMMERCE_DATA_SOURCE` to `retailrocket` or `synthetic` before starting it; the default
is `retailrocket`. `ECOMMERCE_OUTPUT_PATH` optionally overrides the corresponding output path.
Keep the source label consistent with the actual files. Spark processes the full event data;
Pandas receives only bounded product/segment aggregates and the daily summary.

The statistical helper tests require `.[notebooks]`; they are skipped when that optional extra
is absent. CI installs both extras, runs all tests, executes the notebook on a small synthetic
lakehouse, and uploads that executed notebook as an artifact.

## Databricks

1. Build a wheel: `python -m pip wheel --no-deps . -w dist`.
2. Upload it to a Unity Catalog Volume and install it as a notebook/cluster library with
   `--no-deps`, retaining Databricks' managed PySpark and Delta packages. Use a runtime with
   Spark 4.0 and Delta 4.0 compatibility; local versions are PySpark 4.0.1 and Delta 4.0.1.
3. Import `notebooks/databricks_pipeline.py` as a Python source notebook.
4. Set its `input_path`, `output_path`, and `session_minutes` widgets. The cluster identity
   needs read access to the source Volume and write access to the destination.
5. Run all cells. The notebook uses the runtime's existing `spark` session and registers
   temporary Gold views. Run the SQL examples against those views in the same session.

No Databricks deployment or cloud credentials are embedded. Databricks execution requires
your own workspace and compute; local Spark exercises the same transformation code.

## Automated validation

`pytest -q` checks aliases and timestamps, quarantine reasons, deduplication, stable session
boundaries, ordered funnels, cross-product exclusion, expected conversion rates, empty/invalid
data failures, synthetic generation, actual Delta tables, and repeatable full refreshes.
GitHub Actions installs the dependencies, runs Ruff and Pytest, and uploads its test report.
It does not deploy to Databricks. CI success and cloud execution must be verified separately.
