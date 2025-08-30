# scripts/repl.py
# Simple REPL to test the current trained build.
# Uses config_loader.load_config() to resolve paths.

import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))  # allow repo-root imports

from pathlib import Path
from config_loader import load_config
from mlx_lm import load as mlx_load, generate as mlx_generate

# Load config
cfg = load_config()

# --- resolver: base+adapter ONLY (no fused/quantized) ---

RUN_DIR = Path(cfg.run.output_dir)            # e.g., "run"

ARTIFACTS = RUN_DIR / cfg.run.artifacts  # e.g., run/artifacts.json

import json


def resolve_model_and_adapter():
    """
    Load artifacts.json (object with a 'runs' list), pick the newest run that has
    both model_id and adapter_dir, preferring ones that actually exist on disk.
    Fallback to cfg.snapshot.base if none found.
    """
    if ARTIFACTS.exists():
        data = json.loads(ARTIFACTS.read_text(encoding="utf-8"))
        runs = data.get("runs") if isinstance(data, dict) else None

        if isinstance(runs, list) and runs:
            # 1) prefer most recent run whose adapter_dir exists
            for run in reversed(runs):
                model_id   = (run.get("model_id") or "").strip()
                adapter_dir = (run.get("adapter_dir") or "").strip()
                if model_id and adapter_dir and Path(adapter_dir).exists():
                    return model_id, adapter_dir

            # 2) otherwise, return most recent run that simply has fields
            for run in reversed(runs):
                model_id   = (run.get("model_id") or "").strip()
                adapter_dir = (run.get("adapter_dir") or "").strip()
                if model_id and adapter_dir:
                    return model_id, adapter_dir

    # fallback: config snapshot base (HF id) with no adapter
    return cfg.snapshot.base, None

SNAP_DIR = RUN_DIR / cfg.snapshot.output_dir  # e.g., "run/snapshots"

# Prefer quantized → fused → base
candidates = [
    SNAP_DIR / cfg.snapshot.quant,
    SNAP_DIR / cfg.snapshot.fused,
    cfg.snapshot.base,
]

model_path = None
for c in candidates:
    p = Path(c)
    if p.exists():
        model_path = str(p)
        break
    if isinstance(c, str) and not p.exists():
        # fall back to HF model id
        model_path = c
        break

model_path, adapter_path = resolve_model_and_adapter()
#adapter_path = "/Users/theaiguy/mlx-mlxtrain-starter-config/run/microsoft--Phi-3-mini-4k-instruct/adapter"
#adapter_path = None
print("JIM",model_path, adapter_path)
print(f"[repl] base+adapter → model={model_path} adapter={adapter_path}")
model, tok = mlx_load(model_path, adapter_path=adapter_path)

if not model_path:
    raise SystemExit("No usable snapshot found. Did you run the training/snapshot step?")

print(f"[repl] Using model: {model_path}")
model, tok = mlx_load(model_path, adapter_path=None)

max_new = int(cfg.snapshot.max_new)

print("Interactive REPL (type 'exit' or 'quit' to leave)\n")

while True:
    try:
        s = input("> ").strip()
    except (EOFError, KeyboardInterrupt):
        print()
        break
    if not s or s.lower() in {"exit", "quit"}:
        break
    out = mlx_generate(model=model,  tokenizer=tok, prompt=s,  max_tokens=1200)
    #print(repr(out));
    if out.startswith(s):  # strip echo if present
        out = out[len(s):]
    print(out.strip(), "\n")
