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
from pathlib import Path
import csv

RUN_DIR = Path(cfg.run.output_dir)            # e.g., "run"

ARTIFACTS = RUN_DIR / cfg.run.artifacts  # e.g., run/artifacts.csv

def resolve_model_and_adapter():
    if ARTIFACTS.exists():
        with ARTIFACTS.open("r", encoding="utf-8") as f:
            rows = list(csv.DictReader(f))
        # pick the last row that has an adapter_dir
        for row in reversed(rows):
            adapter = (row.get("adapter_dir") or "").strip()
            model_id = (row.get("model_id") or "").strip()
            if adapter and model_id:
                return model_id, adapter  # base + adapter
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
print(f"[repl] base+adapter → model={model_path} adapter={RUN_DIR / "latest_adapter"}")
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
    out = mlx_generate(model=model, tokenizer=tok, prompt=s, min_tokens= 10,  max_tokens=1200)
    print(repr(out));
    if out.startswith(s):  # strip echo if present
        out = out[len(s):]
    print(out.strip(), "\n")
