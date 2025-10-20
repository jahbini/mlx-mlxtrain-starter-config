#!/usr/bin/env python3
"""
032_fuse.py  —  Fuse and Quantize Models
----------------------------------------

Fuses LoRA adapters into base models and performs MLX quantization.

Pipeline compliance:
  • All parameters from config
  • Deterministic, idempotent
  • Logs written under <run_dir>/logs/
"""

from __future__ import annotations
from pathlib import Path
import sys, os, json, hashlib, time, shlex, subprocess, shutil
from typing import Dict, Any, List

# --- Config loader --------------------------------------------------
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config

CFG = load_config()
STEP_NAME = os.environ["STEP_NAME"]
STEP_CFG  = CFG[STEP_NAME]
PARAMS    = STEP_CFG

# --- Directories ----------------------------------------------------
RUN_DIR   = Path( CFG.run.output_dir)
DATA_DIR = Path( CFG.run.data_dir)
ARTIFACTS = DATA_DIR / CFG.run.artifacts
LOG_DIR   = DATA_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH  = LOG_DIR / f"{STEP_NAME}.log"

def log(msg: str):
    stamp = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
    line  = f"[{stamp}] {msg}"
    with LOG_PATH.open("a", encoding="utf-8") as f:
        f.write(line + "\n")
    print(line, flush=True)

# --- Controls -------------------------------------------------------
DO_FUSE = STEP_CFG["do_fuse"]
Q_BITS  = int(STEP_CFG["q_bits"])
Q_GROUP = int(STEP_CFG["q_group"])
DTYPE   = STEP_CFG["dtype"]
DRY_RUN = bool(STEP_CFG["dry_run"])

def run_cmd(cmd: str) -> int:
    log(f"[MLX] {cmd}")
    if DRY_RUN:
        log("DRY_RUN=True → not executing.")
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
    if not root.exists():
        return out
    for p in sorted(root.rglob("*")):
        if p.is_file():
            stat = p.stat()
            out.append({
                "path": str(p.resolve()),
                "rel": str(p.relative_to(root)),
                "bytes": stat.st_size,
                "sha256": sha256_file(p),
                "mtime_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime(stat.st_mtime)),
            })
    return out

# --- Artifact Load --------------------------------------------------
if not ARTIFACTS.exists():
    raise SystemExit("artifacts.json not found. Run training steps first.")

registry = json.loads(ARTIFACTS.read_text(encoding="utf-8"))
runs = registry.get("runs", [])
if not runs:
    raise SystemExit("No runs found in artifacts.json.")

py = shlex.quote(sys.executable)
updated = False

# --- Main Loop ------------------------------------------------------
for entry in runs:
    model_id   = entry["model_id"]
    output_dir = Path(entry["output_root"])
    adapter_dir = Path(entry["adapter_dir"])
    fused_dir   = Path(entry.get("fused_dir") or (output_dir / "fused" / "model"))

    # 1) Fuse --------------------------------------------------------
    if DO_FUSE and not fused_dir.exists():
        fused_dir.parent.mkdir(parents=True, exist_ok=True)
        cmd_fuse = (
            f"{py} -m mlx_lm fuse "
            f"--model {shlex.quote(model_id)} "
            f"--adapter-path {shlex.quote(str(adapter_dir))} "
            f"--save-path {shlex.quote(str(fused_dir))}"
        )
        log("=== FUSE ===")
        rc = run_cmd(cmd_fuse)
        if rc != 0:
            log(f"❌ Fuse failed for {model_id}")
            continue
        entry["fused_dir"] = str(fused_dir.resolve())
        entry.setdefault("files", {})["fused"] = list_files(fused_dir)
        updated = True
    elif fused_dir.exists():
        entry["fused_dir"] = str(fused_dir.resolve())
        entry.setdefault("files", {})["fused"] = list_files(fused_dir)

    if not fused_dir.exists():
        log(f"Skipping quantize for {model_id}: fused_dir missing.")
        continue

    # 2) Quantize ----------------------------------------------------
    q_dir = output_dir / "quantized"
    if q_dir.exists():
        log(f"Removing pre-existing quantized dir: {q_dir}")
        shutil.rmtree(q_dir)

    cmd_q = (
        f"{py} -m mlx_lm convert "
        f"--hf-path {shlex.quote(str(fused_dir))} "
        f"--mlx-path {shlex.quote(str(q_dir))} "
        f"--q-bits {Q_BITS} "
        f"--q-group-size {Q_GROUP} "
        f"--dtype {shlex.quote(DTYPE)} -q"
    )
    log("=== QUANTIZE ===")
    rc = run_cmd(cmd_q)
    if rc != 0:
        log(f"❌ Quantize failed for {model_id}")
        continue

    entry["quantized_dir"] = str(q_dir.resolve())
    entry["quantize_bits"] = Q_BITS
    entry["q_group_size"]  = Q_GROUP
    entry.setdefault("files", {})["quantized"] = list_files(q_dir)
    updated = True

# --- Save Updated Artifacts ----------------------------------------
if updated:
    registry["updated_utc"] = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
    ARTIFACTS.write_text(json.dumps(registry, indent=2), encoding="utf-8")

log("=== FUSE/QUANTIZE SUMMARY ===")
log(f"Wrote: {ARTIFACTS}")
for entry in registry.get("runs", []):
    log(f"- {entry['model_id']}")
    if "fused_dir" in entry:
        log(f"   fused_dir:    {entry['fused_dir']} "
            f"({len(entry.get('files',{}).get('fused',[]))} files)")
    if "quantized_dir" in entry:
        log(f"   quantized_dir: {entry['quantized_dir']} "
            f"(q{entry.get('quantize_bits')}, group={entry.get('q_group_size')}) "
            f"files={len(entry.get('files',{}).get('quantized',[]))}")
