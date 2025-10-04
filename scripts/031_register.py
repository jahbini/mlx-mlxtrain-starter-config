# scripts/031_register.py
from __future__ import annotations
import sys, os, json, hashlib, time, csv
from pathlib import Path
from typing import Dict, Any, List

# --- Config loader ---
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config

# --- STEP-AWARE CONFIG ---
CFG = load_config()
STEP_NAME = os.environ["STEP_NAME"]
STEP_CFG  = CFG.pipeline.steps[STEP_NAME]
PARAMS    = getattr(STEP_CFG, "params", {})

# Resolve paths (params > global cfg)
OUT_DIR  = Path(getattr(PARAMS, "output_dir", CFG.data.output_dir)); OUT_DIR.mkdir(exist_ok=True)
RUN_DIR  = Path(getattr(PARAMS, "run_dir", CFG.run.output_dir))
EXPERIMENTS_CSV = RUN_DIR / getattr(PARAMS, "experiments_csv", CFG.data.experiments_csv)
ARTIFACTS = RUN_DIR / getattr(PARAMS, "artifacts", CFG.data.artifacts)

# --------------------------
# Utilities
# --------------------------
def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def gather_dir_files(root: Path) -> List[Dict[str, Any]]:
    out = []
    if not root.exists():
        return out
    for p in sorted(root.rglob("*")):
        if p.is_file():
            out.append({
                "path": str(p.resolve()),
                "rel": str(p.relative_to(root)),
                "bytes": p.stat().st_size,
                "sha256": sha256_file(p),
                "mtime_utc": time.strftime(
                    "%Y-%m-%dT%H:%M:%SZ",
                    time.gmtime(p.stat().st_mtime)
                ),
            })
    return out

def load_rows(path: Path) -> List[Dict[str, Any]]:
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        return [dict(x) for x in r]

# --------------------------
# Main
# --------------------------
if not EXPERIMENTS_CSV.exists():
    raise SystemExit("experiments.csv not found (run Step 6).")

rows = load_rows(EXPERIMENTS_CSV)
registry: Dict[str, Any] = {
    "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "runs": []
}

for r in rows:
    model_id   = r["model_id"]
    model_tag  = model_id.replace("/", "--")
    out_root   = RUN_DIR / model_tag
    adapter_dir = Path(r["adapter_path"])
    logs_dir    = Path(r["log_dir"])

    fused_dir     = out_root / "fused" / "model"
    quantized_dir = out_root / "quantized" / "model"
    fused_dir.parent.mkdir(parents=True, exist_ok=True)
    quantized_dir.parent.mkdir(parents=True, exist_ok=True)

    # create handy symlinks
    try:
        (out_root / "latest_adapter").unlink(missing_ok=True)
        (out_root / "latest_adapter").symlink_to(adapter_dir.name)
    except Exception:
        pass
    try:
        (out_root / "latest_logs").unlink(missing_ok=True)
        (out_root / "latest_logs").symlink_to(logs_dir.name)
    except Exception:
        pass

    entry = {
        "model_id": model_id,
        "output_root": str(out_root.resolve()),
        "adapter_dir": str(adapter_dir.resolve()),
        "logs_dir": str(logs_dir.resolve()),
        "fused_dir": str(fused_dir.resolve()),
        "quantized_dir": str(quantized_dir.resolve()),
        "files": {
            "adapter": gather_dir_files(adapter_dir),
            "logs": gather_dir_files(logs_dir),
        },
        "training_params": {
            "iters": int(float(r.get("iters", 0) or 0)),
            "batch_size": int(float(r.get("batch_size", 0) or 0)),
            "max_seq_length": int(float(r.get("max_seq_length", 0) or 0)),
        }
    }

    registry["runs"].append(entry)

# Write to artifacts.json
ARTIFACTS.write_text(json.dumps(registry, indent=2), encoding="utf-8")
print(f"[OK] Wrote artifact registry: {ARTIFACTS}")
