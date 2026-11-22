# Databricks notebook source
import sys
from pathlib import Path

# Find the project root and add "src" to the path
current_path = Path.cwd().resolve()
project_root = None

for candidate in (current_path, *current_path.parents):
    if (candidate / "databricks.yml").is_file() and (candidate / "src" ).is_dir():
        project_root = candidate
        break

if project_root is not None: 
    src_path = project_root / "src"
    if str(src_path) not in sys.path:
        sys.path.insert(0, str(src_path))
