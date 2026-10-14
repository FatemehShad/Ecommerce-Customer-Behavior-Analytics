"""Execute the KPI notebook with an explicit data source and save successful outputs."""

import argparse
import os
from pathlib import Path

import nbformat
from nbclient import NotebookClient


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", choices=["retailrocket", "synthetic"], default="retailrocket")
    parser.add_argument(
        "--lakehouse", type=Path, help="Override the source's Delta output directory"
    )
    parser.add_argument("--kernel", default="ecommerce-analytics")
    parser.add_argument(
        "--output", type=Path, default=Path("artifacts/product_kpis.executed.ipynb")
    )
    args = parser.parse_args()
    root = Path(__file__).resolve().parents[1]
    notebook_path = root / "notebooks/product_kpis_and_conversion.ipynb"
    os.environ["ECOMMERCE_DATA_SOURCE"] = args.source
    if args.lakehouse:
        os.environ["ECOMMERCE_OUTPUT_PATH"] = str(args.lakehouse.resolve())
    else:
        os.environ.pop("ECOMMERCE_OUTPUT_PATH", None)
    notebook = nbformat.read(notebook_path, as_version=4)
    for cell in notebook.cells:
        if cell.cell_type == "code":
            cell.outputs = []
            cell.execution_count = None
    client = NotebookClient(
        notebook,
        timeout=600,
        kernel_name=args.kernel,
        record_timing=False,
        resources={"metadata": {"path": str(root)}},
    )
    client.execute()
    nbformat.validate(notebook)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    nbformat.write(notebook, args.output)
    code_cells = [cell for cell in notebook.cells if cell.cell_type == "code"]
    charts = sum(
        "image/png" in output.get("data", {}) for cell in code_cells for output in cell.outputs
    )
    print(f"Executed {len(code_cells)} code cells and {charts} charts; source={args.source}")
    print(f"Saved {args.output}")


if __name__ == "__main__":
    main()
