"""Root-level single-command launcher; reuses the project's virtual environment."""

from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
VENV_PYTHON = (
    ROOT / ".venv" / "Scripts" / "python.exe"
    if os.name == "nt" else ROOT / ".venv" / "bin" / "python"
)

if sys.prefix == sys.base_prefix and VENV_PYTHON.is_file():
    os.execv(str(VENV_PYTHON), [str(VENV_PYTHON), str(Path(__file__).resolve()), *sys.argv[1:]])

sys.path.insert(0, str(ROOT))

from src.orchestrator.executer import main

if __name__ == "__main__":
    raise SystemExit(main())
