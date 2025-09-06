from __future__ import annotations
from pathlib import Path
import sys, os
from datasets import load_from_disk
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config
# STEP 7 (PATCH) — MLX LoRA command fix for 0.26.x
# - Switch to `python -m mlx_lm lora` subcommand form
# - Remove unsupported flags: --gradient-accumulation, --log-dir, --bf16
# - Keep your computed `iters`, batch size, lr, max-seq-length
# - Optional: add reporting/eval knobs that lora *does* support

import csv, shlex, subprocess, sys
from pathlib import Path
from typing import Dict, Any, List, Optional

cfg = load_config()

out_dir = Path(cfg.data.output_dir ); out_dir.mkdir(exist_ok=True)


RUN_DIR       = Path(cfg.run.output_dir)  # where per-model outputs will go
EXPERIMENTS_CSV = RUN_DIR / cfg.data.experiments_csv      # monkey


# ---- Controls ----
DRY_RUN = False
ONLY_MODEL_ID = ""              # or set to a specific model_id string
ONLY_ROW = None                 # or an integer index
# Optional lora reporting/eval settings (set to 0 to skip passing)
STEPS_PER_REPORT = 1000
STEPS_PER_EVAL   = 5000
VAL_BATCHES      = 1
# ------------------

def load_rows(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = [dict(x) for x in r]
    for x in rows:
        for k in ("epochs", "iters", "batch_size", "grad_accum", "max_seq_length", "bf16"):
            if k in x and x[k] != "":
                x[k] = int(float(x[k]))
        for k in ("learning_rate",):
            if k in x and x[k] != "":
                x[k] = float(x[k])
    return rows

def select_rows(rows: List[Dict[str, Any]], only_model: str, only_row_idx: Optional[int]) -> List[Dict[str, Any]]:
    if only_row_idx is not None:
        return [rows[only_row_idx]]
    if only_model:
        return [r for r in rows if r.get("model_id") == only_model]
    return rows

def ensure_dirs(row: Dict[str, Any]):
    Path(row["adapter_path"]).mkdir(parents=True, exist_ok=True)
    Path(row["log_dir"]).mkdir(parents=True, exist_ok=True)

def build_cmd(row: Dict[str, Any]) -> str:
    py = shlex.quote(sys.executable)
    model = shlex.quote(row["model_id"])
    data_dir = shlex.quote(row["data_dir"])
    iters = int(row["iters"])
    bs = int(row["batch_size"])
    maxlen = int(row["max_seq_length"])
    lr = float(row["learning_rate"])
    adapter = shlex.quote(row["adapter_path"])

    # NOTE: no --gradient-accumulation / --bf16 / --log-dir
    parts = [
        f"{py} -m mlx_lm lora",
        f"--model {model}",
        f"--data {data_dir}",
        "--train",
        "--fine-tune-type lora",
        f"--batch-size {bs}",
        f"--iters {iters}",
        f"--learning-rate {lr}",
        f"--max-seq-length {maxlen}",
        f"--adapter-path {adapter}",
        f"--num-layers -1",
    ]
    if VAL_BATCHES:      parts += [f"--val-batches {int(VAL_BATCHES)}"]
    if STEPS_PER_REPORT: parts += [f"--steps-per-report {int(STEPS_PER_REPORT)}"]
    if STEPS_PER_EVAL:   parts += [f"--steps-per-eval {int(STEPS_PER_EVAL)}"]
    return " ".join(parts)

def run_cmd(cmd: str) -> int:
    print("\n[MLX train]", cmd)
    if DRY_RUN:
        print("DRY_RUN=True -> not executing.")
        return 0
    return subprocess.run(cmd, shell=True).returncode

rows = load_rows(EXPERIMENTS_CSV)
todo = select_rows(rows, ONLY_MODEL_ID, ONLY_ROW)

print(f"Found {len(rows)} rows; running {len(todo)} row(s). DRY_RUN={DRY_RUN}")
for i, row in enumerate(todo):
    print(f"\n=== RUN {i+1}/{len(todo)} ===")
    ensure_dirs(row)
    rc = run_cmd(build_cmd(row))
    if rc != 0:
        print(f"❌ Training failed with returncode={rc}")
        break
    print("✅ Training launched.")
