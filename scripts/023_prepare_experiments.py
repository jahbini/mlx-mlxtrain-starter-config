# scripts/022_prepare_prompts.py
from __future__ import annotations
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config
cfg = load_config()
# STEP 6 — Experiment Matrix
# Purpose:
#   - Define models + core hyperparams ONCE.
#   - Resolve dataset sizes from Step 2/3 outputs.
#   - Estimate MLX `--iters` (since mlx_lm.lora is iteration-based).
#   - Emit a clean experiments.csv (one row per model).
#
# Inputs:
#   - data_contract.json (Step 2)
#   - data_catalog.json  (Step 2)  [preferred for counts]
#   - data_report.json   (Step 3)  [fallback if catalog missing]
#
# Outputs:
#   - experiments.csv

import json, math, csv, time
from pathlib import Path
from typing import Dict, Any, Tuple, List

out_dir = Path(cfg.data.output_dir); out_dir.mkdir(exist_ok=True)
CONTRACT    = out_dir / cfg.data.contract
CATALOG     = out_dir / cfg.data.catalog
POLICY      = out_dir / cfg.data.policy
REPORT      = out_dir / cfg.data.report

RUN_DIR       = Path(cfg.run.output_dir)  # where per-model outputs will go
EXPERIMENTS_CSV = RUN_DIR / cfg.data.experiments_csv

# ---------- EDITABLE BLOCK ----------
# List your MLX-compatible base models here
EXPERIMENTS = cfg.experiments

# Core hyperparameters (shared across all rows; you can later copy/edit specific rows)
EPOCHS          = 1               # convenient, we’ll convert to iters
BATCH_SIZE      = 1
GRAD_ACCUM      = 8
MAX_SEQ_LENGTH  = 512
LEARNING_RATE   = 2e-4
BF16            = True
# Optional: override `iters` directly (0 = auto from dataset & epochs)
ITERS_OVERRIDE  = 0
# -----------------------------------

def load_contract() -> Dict[str, Any]:
    return json.loads(CONTRACT.read_text(encoding="utf-8"))

def get_counts_from_catalog() -> Tuple[int, int]:
    if not CATALOG.exists():
        return None, None  # signal fallback
    c = json.loads(CATALOG.read_text(encoding="utf-8"))
    train = c["entries"]["train"]["stats"]["num_valid_examples"]
    # 'valid' key name may be 'valid' or 'val' depending on contract; try both
    val_entry = c["entries"].get("valid") or c["entries"].get("val")
    valid = val_entry["stats"]["num_valid_examples"] if val_entry else 0
    return int(train), int(valid)

def get_counts_from_report() -> Tuple[int, int]:
    r = json.loads(REPORT.read_text(encoding="utf-8"))
    train = r["splits"]["train"]["valid_examples"]
    val_entry = r["splits"].get("valid") or r["splits"].get("val")
    valid = val_entry["valid_examples"] if val_entry else 0
    return int(train), int(valid)

def resolve_files_from_contract(ct: Dict[str, Any]) -> Dict[str, str]:
    files = {k: v["resolved"] for k, v in ct["filenames"].items() if v.get("resolved")}
    # normalize key for validation split
    if "valid" in files:
        files["validation"] = files["valid"]
    elif "val" in files:
        files["validation"] = files["val"]
    return files

def estimate_iters(num_train: int, epochs: int, batch: int, accum: int) -> int:
    # MLX lora uses --iters; here we approximate: steps ≈ epochs * num_train / (batch * accum)
    steps = max(1, math.ceil((epochs * max(1, num_train)) / max(1, batch * accum)))
    # also guard a reasonable floor so very tiny sets still do some learning
    return max(10000, steps)

# 1) Load metadata and counts
ct = load_contract()
files = resolve_files_from_contract(ct)

train_count, valid_count = get_counts_from_catalog()
if train_count is None:
    train_count, valid_count = get_counts_from_report()

data_dir = Path(ct["data_dir"])

# 2) Build experiment rows
rows: List[Dict[str, Any]] = []
timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

for model_id in EXPERIMENTS:
    model_tag = model_id.replace("/", "--")
    out_root  = RUN_DIR / model_tag
    adapter_path = out_root / "adapter"
    logs_dir     = out_root / "logs"

    iters = ITERS_OVERRIDE or estimate_iters(
        num_train=train_count,
        epochs=EPOCHS,
        batch=BATCH_SIZE,
        accum=GRAD_ACCUM,
    )

    # token budget (very rough): max_seq_length * batch * accum * iters
    est_tokens = MAX_SEQ_LENGTH * BATCH_SIZE * GRAD_ACCUM * iters

    rows.append({
        "created_utc": timestamp,
        "model_id": model_id,
        "data_dir": str(data_dir),
        "train_file": files.get("train"),
        "valid_file": files.get("validation"),
        "train_examples": train_count,
        "valid_examples": valid_count,
        "epochs": EPOCHS,
        "iters": iters,
        "batch_size": BATCH_SIZE,
        "grad_accum": GRAD_ACCUM,
        "max_seq_length": MAX_SEQ_LENGTH,
        "learning_rate": LEARNING_RATE,
        "bf16": int(bool(BF16)),
        "adapter_path": str(adapter_path),
        "log_dir": str(logs_dir),
        "est_tokens": est_tokens
    })

# 3) Write experiments.csv
EXPERIMENTS_CSV.parent.mkdir(parents=True, exist_ok=True)
fieldnames = list(rows[0].keys()) if rows else []
with EXPERIMENTS_CSV.open("w", newline="", encoding="utf-8") as f:
    w = csv.DictWriter(f, fieldnames=fieldnames)
    w.writeheader()
    for r in rows:
        w.writerow(r)

# 4) Console summary
print("=== EXPERIMENT MATRIX ===")
print(f"Data dir: {data_dir}")
print(f"Counts: train={train_count} valid={valid_count}")
print(f"Wrote: {EXPERIMENTS_CSV}\n")
for r in rows:
    print(f"- {r['model_id']}")
    print(f"   iters={r['iters']}  bs={r['batch_size']}  accum={r['grad_accum']}  max_len={r['max_seq_length']}  lr={r['learning_rate']}  bf16={r['bf16']}")
    print(f"   est_tokens≈{r['est_tokens']:,}  adapter={r['adapter_path']}")
