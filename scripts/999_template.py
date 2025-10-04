#!/usr/bin/env python3
"""
Template Script for Pipeline Integration
----------------------------------------

Use this template when creating a new script for the pipeline.

Requirements:
  • All parameters and paths must come from config (default + override).
  • No command-line arguments allowed.
  • Deterministic: same input + config → same output.
  • Fail fast on missing inputs or bad config.
  • Logs written into <output>/logs/ (step-specific).
"""

from __future__ import annotations
import sys, os, json, time
from pathlib import Path
from typing import Dict, Any

# --- 1) Load Config ---
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config

CFG = load_config()
STEP_NAME = os.environ["STEP_NAME"]
STEP_CFG  = CFG.pipeline.steps[STEP_NAME]

# --- 2) Directories ---
OUT_DIR = Path(CFG.data.output_dir); OUT_DIR.mkdir(parents=True, exist_ok=True)
LOG_DIR = OUT_DIR / "logs"; LOG_DIR.mkdir(parents=True, exist_ok=True)

# Example input/output (replace with actual keys for your step)
INPUT_FILE  = OUT_DIR / CFG.data.contract        # e.g. data_contract.json
OUTPUT_FILE = OUT_DIR / f"{STEP_NAME}_output.json"

# --- 3) Logging Helper ---
LOG_PATH = LOG_DIR / f"{STEP_NAME}.log"
def log(msg: str):
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    full = f"[{stamp}] {msg}"
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(full + "\n")
    print(full)

# --- 4) Validate Inputs ---
if not INPUT_FILE.exists():
    log(f"[FATAL] Missing required input file: {INPUT_FILE}")
    sys.exit(1)

log(f"[INFO] Starting step {STEP_NAME}")
log(f"[INFO] Using config pipeline step: {STEP_CFG}")

# --- 5) Core Work (replace with real logic) ---
def process_contract(path: Path) -> Dict[str, Any]:
    """Example: load JSON contract and echo metadata."""
    data = json.loads(path.read_text(encoding="utf-8"))
    result = {
        "summary": f"Contract contains {len(data.get('filenames', {}))} files",
        "git_commit": getattr(CFG.run, "git_commit", "unknown"),
    }
    return result

result = process_contract(INPUT_FILE)

# --- 6) Save Outputs ---
OUTPUT_FILE.write_text(json.dumps(result, indent=2), encoding="utf-8")
log(f"[INFO] Wrote output: {OUTPUT_FILE}")

# --- 7) Exit Cleanly ---
log(f"[INFO] Completed step {STEP_NAME} successfully")
sys.exit(0)
