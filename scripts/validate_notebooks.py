"""Validate all notebooks, optionally execute selected labs against real Kafka.

Examples:
  python -m scripts.validate_notebooks
  python -m scripts.validate_notebooks --execute --only 00
  python -m scripts.validate_notebooks --execute
  python -m scripts.validate_notebooks --execute --only 15 --include-cluster
"""
import argparse
import ast
from pathlib import Path

from IPython.core.inputtransformer2 import TransformerManager
import nbformat

ROOT = Path(__file__).resolve().parents[1]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--execute", action="store_true", help="Execute using a real notebook kernel.")
    parser.add_argument("--only", nargs="+", help="Notebook prefixes, e.g. 00 06 12.")
    parser.add_argument("--include-cluster", action="store_true", help="Include optional notebook 15 in execution.")
    args = parser.parse_args()
    paths = sorted((ROOT / "notebooks").glob("*.ipynb"))
    if len(paths) != 16 or {p.name[:2] for p in paths} != {f"{i:02}" for i in range(16)}:
        raise AssertionError("Expected one notebook for every lesson 00 through 15.")
    if args.only:
        paths = [p for p in paths if p.name[:2] in args.only]
        if not paths:
            parser.error("No notebooks matched --only.")
    transformer = TransformerManager()
    total_cells = 0
    for path in paths:
        notebook = nbformat.read(path, as_version=4)
        nbformat.validate(notebook)
        for index, cell in enumerate(notebook.cells, 1):
            if cell.cell_type == "code":
                ast.parse(transformer.transform_cell(cell.source), filename=f"{path.name}:cell{index}")
                if cell.source.startswith("%%django_sync\n"):
                    ast.parse(cell.source.split("\n", 1)[1], filename=f"{path.name}:cell{index}:django-body")
                if cell.outputs or cell.execution_count is not None:
                    raise AssertionError(f"{path.name}: committed notebooks must have clean outputs.")
            total_cells += 1
        print(f"VALID {path.name}: {len(notebook.cells)} cells")
        if args.execute:
            if path.name.startswith("15") and not args.include_cluster:
                print("SKIP execution: 15 requires the separate three-broker cluster.")
                continue
            from nbclient import NotebookClient
            destination = ROOT / "artifacts" / "executed" / path.name
            destination.parent.mkdir(parents=True, exist_ok=True)
            client = NotebookClient(notebook, timeout=180, kernel_name="kafka-workshop",
                                    resources={"metadata": {"path": str(ROOT)}}, allow_errors=False)
            try:
                client.execute()
            finally:
                nbformat.write(notebook, destination)
            print(f"EXECUTED {path.name}; output in artifacts/executed/")
    print(f"Validated {len(paths)} notebook(s), {total_cells} cells.")


if __name__ == "__main__":
    main()
