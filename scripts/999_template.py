#!/usr/bin/env python3
"""
Template Script for Pipeline Integration
----------------------------------------

Use this template when creating a new script for the pipeline.  
All parameters and paths must come from config (default + override).  
No command-line arguments are allowed.

Inputs:
  - Defined in config (under cfg.data.* or cfg.run.*)
Outputs:
  - Defined in config and written into PWD (never EXEC)
Logs:
  - Write status messages into PWD/logs/

Behavior:
  - Deterministic (same input + config → same output)
  - Fail fast on missing inputs or bad config
"""

from __future__ import annotations
import sys, os, json
from pathlib import Path
from typing import Dict, Any

# --- 1) Load Config ---
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config

cfg = load_config()

# Directories
out_dir = Path(cfg.data.output_dir)
out_dir.mkdir(exist_ok=True, parents=True)

log_dir = Path(out_dir) / "logs"
log_dir.mkdir(exist_ok=True, parents=True)

# Example input/output files from config
INPUT_FILE  = out_dir / cfg.data.contract   # replace with correct key
OUTPUT_FILE = out_dir / "example_output.json"  # replace with correct key

# --- 2) Logging Helper ---
def log(msg: str):
    log_path = log_dir / "template.log"
    with log_path.open("a", encoding="utf-8") as f:
        f.write(msg + "\n")
    print(msg)

# --- 3) Validate Inputs ---
if not INPUT_FILE.exists():
    log(f"[FATAL] Missing required input file: {INPUT_FILE}")
    sys.exit(1)

log("[INFO] Starting template script")
log(f"[INFO] Using config keys from: {cfg}")

# --- 4) Core Work (replace with real logic) ---
def process_contract(path: Path) -> Dict[str, Any]:
    """Example: load JSON contract and echo metadata."""
    data = json.loads(path.read_text(encoding="utf-8"))
    result = {
        "summary": f"Contract contains {len(data.get('filenames', {}))} files",
        "git_commit": cfg.run.get("git_commit", "unknown"),
    }
    return result

result = process_contract(INPUT_FILE)

# --- 5) Save Outputs ---
OUTPUT_FILE.write_text(json.dumps(result, indent=2), encoding="utf-8")
log(f"[INFO] Wrote output: {OUTPUT_FILE}")

# --- 6) Exit Cleanly ---
log("[INFO] Completed successfully")
sys.exit(0)
