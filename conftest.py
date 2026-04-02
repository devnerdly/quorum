"""Root conftest: add the trading root and services to sys.path."""

from __future__ import annotations

import sys
from pathlib import Path

# Project root — enables:
#   'shared.*' (if installed editable or via sys.path)
#   'services.sentiment.sources.*' (since services/ is a sub-directory here)
ROOT = Path(__file__).parent

if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
