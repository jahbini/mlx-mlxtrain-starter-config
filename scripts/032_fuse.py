from __future__ import annotations
from pathlib import Path
import sys, os
from datasets import load_from_disk
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
# STEP 9 — Fuse & Quantize (final, clean/idempotent)
# - Reuses experiments.csv + artifacts.json from Steps 6–8
# - If needed, fuses adapter -> fused/model  (mlx_lm fuse)
# - Quantizes fused -> quantized/ (mlx_lm convert with explicit flags)
# - Removes any pre-existing quantized dir to avoid MLX "already exists" error
# - Updates artifacts.json

import json, hashlib, time, shlex, subprocess, sys, shutil
from typing import Dict, Any, List

from config_loader import load_config
cfg = load_config()
out_dir = Path(cfg.data.output_dir); out_dir.mkdir(exist_ok=True)
RUN_DIR       = Path(cfg.run.output_dir)  # where per-model outputs will go
EXPERIMENTS = RUN_DIR / cfg.run.experiments
ARTIFACTS     = RUN_DIR / cfg.run.artifacts

# ---- Controls ----
DO_FUSE   = True           # set False to skip fusing (if already fused)
Q_BITS    = 4              # 4 or 8
Q_GROUP   = 64             # e.g., 32, 64, 128
DTYPE     = cfg.model.dtype     # float16 | bfloat16 | float32
DRY_RUN   = False
# -------------------

def run_cmd(cmd: str) -> int:
    print("[MLX]", cmd)
    if DRY_RUN:
        print("DRY_RUN=True -> not executing.")
        return 0
    return subprocess.run(cmd, shell=True).returncode

def sha256_file(p: Path) -> str:
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024*1024), b""):
            h.update(chunk)
    return h.hexdigest()

def list_files(root: Path) -> List[Dict[str, Any]]:
    out = []
    if not root.exists(): return out
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

# Load artifacts (for adapter paths) and experiments (for model_ids)
if not ARTIFACTS.exists():
    raise SystemExit("artifacts.json not found. Run Steps 8/7/6 first.")

registry = json.loads(ARTIFACTS.read_text(encoding="utf-8"))
runs = registry.get("runs", [])
if not runs:
    raise SystemExit("No runs found in artifacts.json.")

py = shlex.quote(sys.executable)
updated = False

for entry in runs:
    model_id   = entry["model_id"]
    output_dir = Path(entry["output_root"])
    adapter_dir = Path(entry["adapter_dir"])
    fused_dir   = Path(entry.get("fused_dir") or (output_dir / "fused" / "model"))

    # 1) Fuse (optional / idempotent)
    if DO_FUSE and not fused_dir.exists():
        fused_dir.parent.mkdir(parents=True, exist_ok=True)
        cmd_fuse = (
            f"{py} -m mlx_lm.fuse "
            f"--model {shlex.quote(model_id)} "
            f"--adapter-path {shlex.quote(str(adapter_dir))} "
            f"--save-path {shlex.quote(str(fused_dir))}"
        )
        print("\n=== FUSE ===")
        rc = run_cmd(cmd_fuse)
        if rc != 0:
            print(f"❌ Fuse failed for {model_id}")
            continue
        entry["fused_dir"] = str(fused_dir.resolve())
        entry.setdefault("files", {})["fused"] = list_files(fused_dir)
        updated = True
    elif fused_dir.exists():
        entry["fused_dir"] = str(fused_dir.resolve())
        entry.setdefault("files", {})["fused"] = list_files(fused_dir)

    if not fused_dir.exists():
        print(f"Skipping quantize for {model_id}: fused_dir missing.")
        continue

    # 2) Quantize (idempotent + clean)
    q_dir = output_dir / "quantized"
    if q_dir.exists():
        print(f"Removing pre-existing quantized dir: {q_dir}")
        shutil.rmtree(q_dir)
    #q_dir.mkdir(parents=True, exist_ok=True)

    cmd_q = (
        f"{py} -m mlx_lm.convert "
        f"--hf-path {shlex.quote(str(fused_dir))} "
        f"--mlx-path {shlex.quote(str(q_dir))} "
        f"--q-bits {int(Q_BITS)} "
        f"--q-group-size {int(Q_GROUP)} "
        f"--dtype {shlex.quote(DTYPE)} "
        f"-q"
    )
    print("\n=== QUANTIZE ===")
    rc = run_cmd(cmd_q)
    if rc != 0:
        print(f"❌ Quantize failed for {model_id}")
        continue

    entry["quantized_dir"] = str(q_dir.resolve())
    entry["quantize_bits"] = int(Q_BITS)
    entry["q_group_size"]  = int(Q_GROUP)
    entry.setdefault("files", {})["quantized"] = list_files(q_dir)
    updated = True

# Save updated artifacts
if updated:
    registry["updated_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    ARTIFACTS.write_text(json.dumps(registry, indent=2), encoding="utf-8")

# Summary
print("\n=== FUSE/QUANTIZE SUMMARY ===")
print("Wrote:", ARTIFACTS)
for entry in registry.get("runs", []):
    print(f"- {entry['model_id']}")
    if "fused_dir" in entry:
        print("   fused_dir:    ", entry['fused_dir'], f"({len(entry.get('files',{}).get('fused',[]))} files)")
    if "quantized_dir" in entry:
        print("   quantized_dir:", entry['quantized_dir'],
              f"(q{entry.get('quantize_bits')}, group={entry.get('q_group_size')})",
              f"files={len(entry.get('files',{}).get('quantized',[]))}")
