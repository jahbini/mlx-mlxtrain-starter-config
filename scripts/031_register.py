from __future__ import annotations
from pathlib import Path
import sys, json, hashlib, os, time
from datasets import load_from_disk
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config
cfg = load_config()
# STEP 8 — Artifact Registry
# Reads experiments.csv and registers produced artifacts (adapters, logs).
# - Computes SHA256 & sizes
# - Writes artifacts.json
# - Creates per-model symlinks: latest_adapter -> adapter , latest_logs -> logs2

from typing import Dict, Any, List
import csv

out_dir = Path(cfg.data.output_dir); out_dir.mkdir(exist_ok=True)
RUN_DIR       = Path(cfg.run.output_dir)  # where per-model outputs will go
EXPERIMENTS_CSV = RUN_DIR / cfg.data.experiments_csv
ARTIFACTS     = RUN_DIR / cfg.data.artifacts


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
                "mtime_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(p.stat().st_mtime)),
            })
    return out

def load_rows(path: Path):
    with path.open("r", encoding="utf-8") as f:
        r = csv.DictReader(f)
        rows = [dict(x) for x in r]
    return rows

if not EXPERIMENTS_CSV.exists():
    raise SystemExit("experiments.csv not found (run Step 6).")

rows = load_rows(EXPERIMENTS_CSV)
registry: Dict[str, Any] = {
    "created_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "runs": []
}

for r in rows:
    model_id = r["model_id"]
    model_tag = model_id.replace("/", "--")
    out_root = Path(r["adapter_path"]).parent.parent  # runs/<model_tag>
    adapter_dir = Path(r["adapter_path"])
    logs_dir    = Path(r["log_dir"])

    # create handy symlinks
    try:
        (out_root / "latest_adapter").unlink(missing_ok=True)
        (out_root / "latest_adapter").symlink_to(adapter_dir.name)
    except Exception: pass
    try:
        (out_root / "latest_logs").unlink(missing_ok=True)
        (out_root / "latest_logs").symlink_to(logs_dir.name)
    except Exception: pass

    entry = {
        "model_id": model_id,
        "output_root": str(out_root.resolve()),
        "adapter_dir": str(adapter_dir.resolve()),
        "logs_dir": str(logs_dir.resolve()),
        "files": {
            "adapter": gather_dir_files(adapter_dir),
            "logs": gather_dir_files(logs_dir),
        },
        "training_params": {
            "iters": int(float(r.get("iters", 0) or 0)),
            "batch_size": int(float(r.get("batch_size", 0) or 0)),
            "max_seq_length": int(float(r.get("max_seq_length", 0) or 0)),
            "learning_rate": float(r.get("learning_rate", 0.0) or 0.0),
        }
    }
    registry["runs"].append(entry)

ARTIFACTS.write_text(json.dumps(registry, indent=2), encoding="utf-8")

# Console summary
print("=== ARTIFACT REGISTRY ===")
print("Wrote:", ARTIFACTS)
for run in registry["runs"]:
    adap_files = run["files"]["adapter"]
    n = len(adap_files)
    sizes = sum(f["bytes"] for f in adap_files)
    print(f"- {run['model_id']}")
    print("   adapter_dir:", run["adapter_dir"])
    print("   logs_dir:   ", run["logs_dir"])
    print(f"   adapter files: {n}  total bytes: {sizes:,}")
    if n:
        print("   latest:", adap_files[-1]["rel"], adap_files[-1]["bytes"], "bytes")
    print("   symlinks: latest_adapter, latest_logs")
