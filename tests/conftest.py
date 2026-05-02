from __future__ import annotations

import sys
from pathlib import Path

# Add backend directory to path so tests can import backend modules directly
BACKEND_DIR = Path(__file__).resolve().parent.parent / "backend"
if str(BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(BACKEND_DIR))
