# scripts/03_train.py  (LoRA training patch for MLX 0.26.x)

from __future__ import annotations
import sys, os, csv, shlex, subprocess
from pathlib import Path
from typing import Dict, Any, List, Optional

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config

# --- STEP-AWARE CONFIG ---
CFG = load_config()
STEP_NAME = os.environ["STEP_NAME"]
STEP_CFG  = CFG.pipeline.steps[STEP_NAME]
PARAMS    = getattr(STEP_CFG, "params", {})

# Resolve from params > global config
OUT_DIR = Path(getattr(PARAMS, "output_dir", CFG.data.output_dir)); OUT_DIR.mkdir(exist_ok=True)
RUN_DIR = Path(getattr(PARAMS, "run_dir", CFG.run.output_dir))
EXPERIMENTS_CSV = RUN_DIR / getattr(PARAMS, "experiments_csv", CFG.data.experiments_csv)

# ---- Controls (can be overridden by step.params) ----
DRY_RUN          = PARAMS.get("dry_run", False)
ONLY_MODEL_ID    = PARAMS.get("only_model_id", "")
ONLY_ROW         = PARAMS.get("only_row", None)
STEPS_PER_REPORT = PARAMS.get("steps_per_report", 1000)
STEPS_PER_EVAL   = PARAMS.get("steps_per_eval", 5000)
VAL_BATCHES      = PARAMS.get("val_batches", 1)
# ------------------------------------------------------

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

    # NOTE: MLX lora subcommand (no grad-accum, bf16, log-dir)
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
        "--num-layers -1",
    ]
    if VAL_BATCHES:      parts += [f"--val-batches {int(VAL_BATCHES)}"]
    if STEPS_PER_REPORT: parts += [f"--steps-per-report {int(STEPS_PER_REPORT)}"]
    if STEPS_PER_EVAL:   parts += [f"--steps-per-eval {int(STEPS_PER_EVAL)}"]
    return " ".join(parts)

def run_cmd(cmd: str, log_path: str = "run/lora_last.log") -> int:
    print("\n[MLX train]", cmd)
    if DRY_RUN:
        print("DRY_RUN=True -> not executing.")
        return 0

    with open(log_path, "w", encoding="utf-8") as log_file:
        process = subprocess.run(
            cmd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )
        log_file.write(process.stdout)

    if process.returncode != 0:
        print(f"❌ Training failed. See log: {log_path}")
    else:
        print(f"✅ Training completed. Log: {log_path}")
    return process.returncode

# --- MAIN ---
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
