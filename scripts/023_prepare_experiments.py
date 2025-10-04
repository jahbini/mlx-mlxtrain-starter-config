# scripts/022_experiment_matrix.py
from __future__ import annotations
import sys, os, json, math, csv, time
from pathlib import Path
from typing import Dict, Any, Tuple, List

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config

# --- STEP-AWARE CONFIG ---
CFG = load_config()
STEP_NAME = os.environ["STEP_NAME"]
STEP_CFG  = CFG.pipeline.steps[STEP_NAME]
PARAMS    = getattr(STEP_CFG, "params", {})

# Resolve paths
OUT_DIR  = Path(getattr(PARAMS, "output_dir", CFG.data.output_dir)); OUT_DIR.mkdir(exist_ok=True)
CONTRACT = OUT_DIR / getattr(PARAMS, "contract", CFG.data.contract)
CATALOG  = OUT_DIR / getattr(PARAMS, "catalog", CFG.data.catalog)
POLICY   = OUT_DIR / getattr(PARAMS, "policy", CFG.data.policy)
REPORT   = OUT_DIR / getattr(PARAMS, "report", CFG.data.report)

RUN_DIR  = Path(getattr(PARAMS, "run_dir", CFG.run.output_dir))
EXPERIMENTS_CSV = RUN_DIR / getattr(PARAMS, "experiments_csv", CFG.data.experiments_csv)

# ---------- EDITABLE BLOCK (overridable via params) ----------
EXPERIMENTS      = PARAMS.get("experiments", CFG.experiments)
EPOCHS           = PARAMS.get("epochs", 1)
BATCH_SIZE       = PARAMS.get("batch_size", 1)
GRAD_ACCUM       = PARAMS.get("grad_accum", 8)
MAX_SEQ_LENGTH   = PARAMS.get("max_seq_length", 512)
LEARNING_RATE    = PARAMS.get("learning_rate", 2e-4)
BF16             = PARAMS.get("bf16", True)
ITERS_OVERRIDE   = PARAMS.get("iters_override", 0)
# ------------------------------------------------------------

def load_contract() -> Dict[str, Any]:
    return json.loads(CONTRACT.read_text(encoding="utf-8"))

def get_counts_from_catalog() -> Tuple[int, int]:
    if not CATALOG.exists():
        return None, None
    c = json.loads(CATALOG.read_text(encoding="utf-8"))
    train = c["entries"]["train"]["stats"]["num_valid_examples"]
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
    if "valid" in files:
        files["validation"] = files["valid"]
    elif "val" in files:
        files["validation"] = files["val"]
    return files

def estimate_iters(num_train: int, epochs: int, batch: int, accum: int) -> int:
    steps = max(1, math.ceil((epochs * max(1, num_train)) / max(1, batch * accum)))
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
    print(f"   iters={r['iters']}  bs={r['batch_size']}  accum={r['grad_accum']}  "
          f"max_len={r['max_seq_length']}  lr={r['learning_rate']}  bf16={r['bf16']}")
    print(f"   est_tokens≈{r['est_tokens']:,}  adapter={r['adapter_path']}")
