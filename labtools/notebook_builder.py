"""Build normal Jupyter notebooks with the standard library; no hidden outputs."""
from pathlib import Path
from textwrap import dedent
from uuid import uuid4
import json

BOOTSTRAP = '''
from pathlib import Path
import sys
from uuid import uuid4

ROOT = next((p for p in [Path.cwd(), *Path.cwd().parents]
             if (p / "pyproject.toml").exists() and (p / "labtools").is_dir()), None)
if ROOT is None:
    raise RuntimeError("Open Jupyter from the kafka-django workshop folder.")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
from labtools.kafka import (
    bootstrap_servers, client_config, require_broker, new_topic,
    producer, consumer, publish, poll_records, collect, decode,
)
RUN = uuid4().hex[:8]
print("Workshop:", ROOT.name, "| run:", RUN)
'''

def _cell(kind, text):
    cell = {"cell_type": kind, "id": uuid4().hex[:8], "metadata": {},
            "source": dedent(text).strip() + "\n"}
    if kind == "code":
        cell.update(execution_count=None, outputs=[])
    return cell

def md(text):
    return _cell("markdown", text)

def code(text):
    return _cell("code", text)

def write_notebook(path, cells):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"cells": cells, "metadata": {
        "kernelspec": {"display_name": "Python (Kafka Workshop)", "language": "python", "name": "kafka-workshop"},
        "language_info": {"name": "python", "version": "3.13"},
    }, "nbformat": 4, "nbformat_minor": 5}
    path.write_text(json.dumps(payload, indent=1, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"Wrote {path.name} ({len(cells)} cells)")

