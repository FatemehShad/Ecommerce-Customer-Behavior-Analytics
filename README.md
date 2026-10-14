# E-commerce Customer Behaviour Analytics

Analyse visitor journeys, product interactions, and conversion funnels using Python, SQL,
PySpark, and Delta Lake. Includes data quality checks, KPI charts, visitor segmentation,
confidence intervals, and hypothesis tests.

**Dataset:** [Retailrocket on Kaggle](https://www.kaggle.com/datasets/retailrocket/ecommerce-dataset),
using `events.csv` (approximately 2.7 million events). Saved notebook outputs use synthetic
data; real Retailrocket and Databricks execution remain unverified.

## Setup

Requires **Python 3.10–3.13** and **Java 17 or 21**. Run from the repository root:

```bash
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev,notebooks]'
python -m ipykernel install --user --name ecommerce-analytics
export SPARK_LOCAL_IP=127.0.0.1
```

## Run Retailrocket

```bash
ecommerce-analytics download-retailrocket --output data/retailrocket/events.csv
export PYSPARK_SUBMIT_ARGS='--driver-memory 4g pyspark-shell'
ecommerce-analytics run --input data/retailrocket/events.csv \
  --output output/retailrocket --master 'local[4]' --shuffle-partitions 16
python scripts/validate_outputs.py --output output/retailrocket
```

If downloading fails, obtain `events.csv` from Kaggle and place it at the same input path.
Each pipeline run replaces its output tables: supply the complete event dataset and keep
input and output directories separate. Downloads and generated outputs are Git-ignored.

For a small synthetic example:

```bash
ecommerce-analytics generate --output data/demo.csv --rows 10000
ecommerce-analytics run --input data/demo.csv --output output/demo
```

Generation refuses to overwrite existing input; reuse the CSV when rerunning.

## Explore the results

- [KPI notebook](notebooks/product_kpis_and_conversion.ipynb): explained calculations,
  funnel drop-off, product rankings, segmentation, and statistical comparisons.
- [SQL queries](sql/customer_behaviour.sql): returning visitors, conversion, and cart abandonment.
- [Databricks notebook](notebooks/databricks_pipeline.py): run the pipeline using managed Spark.

Execute the KPI notebook on real outputs:

```bash
python scripts/execute_kpi_notebook.py --source retailrocket
```

For the demo, use `--source synthetic --lakehouse output/demo`. The executed copy is saved to
`artifacts/product_kpis.executed.ipynb`.

Sessions end after an inactivity gap greater than 30 minutes. Funnel conversion requires
view → cart → purchase for the same product in the same session. Statistical comparisons
are observational associations.

## Databricks

1. Build a wheel: `python -m pip wheel --no-deps . -w dist`.
2. Upload the wheel and `events.csv` to a Unity Catalog Volume.
3. Install with `%pip install --no-deps <wheel-path>` on Spark 4.0 / Delta 4.0-compatible compute.
4. Import the Databricks notebook, set its input/output widgets, and run all cells.

The KPI notebook needs adaptation to Databricks' existing Spark session.

## Checks and documentation

```bash
ruff check .
pytest -q
```

GitHub Actions runs tests and a synthetic notebook smoke check.
See the [setup and analysis guide](docs/usage.md) for schemas, proxy setup, benchmark commands,
and notebook options, and the [validation record](docs/validation.md) for verified results.
