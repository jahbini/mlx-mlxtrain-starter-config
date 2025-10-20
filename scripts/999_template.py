#!/usr/bin/env python3
"""
999_template.py  —  Pipeline-compliant script skeleton
-----------------------------------------------------

Purpose:
    A minimal boilerplate for new Python pipeline steps.
    This version adheres exactly to the Celarien/MLX pipeline rules.

Usage:
    Invoked only by pipeline_runner.coffee; never directly.

Contract:
    • All configuration comes from default+override config via load_config().
    • No CLI args.
    • Deterministic, self-contained, reproducible.
    • Writes logs under <output>/logs/.
    • Exits non-zero on missing inputs or invalid config.
"""

from __future__ import annotations
import sys, os, json, time
from pathlib import Path
from typing import Dict, Any

# --- 1) Load Config -------------------------------------------------
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config

CFG = load_config()
STEP_NAME = os.environ["STEP_NAME"]
STEP_CFG  = CFG[STEP_NAME]
PARAMS    = STEP_CFG

# --- 2) Resolve Paths ------------------------------------------------
ROOT      = Path(os.getenv("EXEC", Path(__file__).parent)).resolve()
OUT_DIR   = Path( CFG.data.output_dir).resolve()
LOG_DIR   = OUT_DIR / "logs"
for d in [OUT_DIR, LOG_DIR]:
    d.mkdir(parents=True, exist_ok=True)

INPUT_FILE  = Path( OUT_DIR / STEP_CFG.data.contract)
OUTPUT_FILE = Path( OUT_DIR / f"{STEP_NAME}_output.json")

# --- 3) Logging ------------------------------------------------------
LOG_PATH = LOG_DIR / f"{STEP_NAME}.log"
def log(msg: str):
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    full = f"[{stamp}] {msg}"
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(full + "\n")
    print(full, flush=True)

# --- 4) Validate Inputs ---------------------------------------------
if not INPUT_FILE.exists():
    log(f"[FATAL] Missing required input file: {INPUT_FILE}")
    sys.exit(1)

log(f"[INFO] Starting step '{STEP_NAME}'")
log(f"[INFO] Using output directory: {OUT_DIR}")
log(f"[INFO] Step parameters: {json.dumps(PARAMS, indent=2)}")

# --- 5) Core Logic (replace this section) ----------------------------
def process_contract(path: Path) -> Dict[str, Any]:
    """Example: load JSON contract and summarize contents."""
    data = json.loads(path.read_text(encoding="utf-8"))
    summary = f"Contract includes {len(data.get('filenames', {}))} items"
    return {
        "summary": summary,
        "timestamp": time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()),
        "git_commit": getattr(CFG.run, "git_commit", "unknown"),
    }

result = process_contract(INPUT_FILE)

# --- 6) Save Outputs -------------------------------------------------
OUTPUT_FILE.write_text(json.dumps(result, indent=2), encoding="utf-8")
log(f"[INFO] Wrote output: {OUTPUT_FILE}")

# --- 7) Clean Exit ---------------------------------------------------
log(f"[INFO] Completed step '{STEP_NAME}' successfully")
sys.exit(0)
