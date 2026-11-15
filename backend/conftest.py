"""Root conftest — adds the backend directory to sys.path so `app` is importable."""
import sys
import os

sys.path.insert(0, os.path.dirname(__file__))
