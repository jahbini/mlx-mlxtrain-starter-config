# scripts/042_sanity.py
# STEP 12 — Regeneration Sanity Checks (artifact + prompt ablation)
# Goal: diagnose empty outputs by trying:
#   1) artifact: quantized vs fused
#   2) prompts: plain / directive / few-shot
# Uses MLX defaults (no sampling kwargs) for broad compatibility.

from __future__ import annotations
import os, sys, json, textwrap, time
from pathlib import Path
from typing import Dict, Any, List, Optional, Tuple
from collections import defaultdict
from mlx_lm import load as mlx_load, generate as mlx_generate

# --- Config loader ---
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config

# --- STEP-AWARE CONFIG ---
CFG       = load_config()
STEP_NAME = os.environ["STEP_NAME"]
STEP_CFG  = CFG[STEP_NAME]
PARAMS    = STEP_CFG

# Resolve paths (params > global cfg)
OUT_DIR   = Path(CFG.data.output_dir)); OUT_DIR.mkdir(exist_ok=True)
EVAL_DIR  = Path(CFG.eval.output_dir)); EVAL_DIR.mkdir(exist_ok=True)
RUN_DIR   = Path(CFG.run.output_dir))

ARTIFACTS = RUN_DIR / CFG.data.artifacts)
CONTRACT  = OUT_DIR / CFG.data.contract)

GEN_JSONL = EVAL_DIR / STEP_CFG.generations + ".jsonl"
GEN_CSV   = EVAL_DIR / STEP_CFG.generations + ".csv"
OUT_SUM   = EVAL_DIR / STEP_CFG.summary + ".csv"
OUT_JSON  = EVAL_DIR / STEP_CFG.analysis + ".json"
ABL_PATH  = EVAL_DIR / STEP_CFG.ablations + ".jsonl"
ABL_YAML  = EVAL_DIR / STEP_CFG.ablations + ".yaml"

# ---- Controls ----
ONLY_MODEL_ID       = ""  # "" = all; or exact id
PROMPTS             = STEP_CFG.prompts)
MAX_NEW_TOKENS_SHORT = 64
MAX_NEW_TOKENS_LONG  = 128
# -------------------

def load_runs() -> List[Dict[str, Any]]:
    reg = json.loads(ARTIFACTS.read_text(encoding="utf-8"))
    runs = reg.get("runs", [])
    if ONLY_MODEL_ID:
        runs = [r for r in runs if r.get("model_id") == ONLY_MODEL_ID]
    if not runs:
        raise SystemExit("No matching runs in artifacts.json.")
    return runs

def pick_artifacts(run_entry: Dict[str, Any]) -> List[Tuple[str, Optional[str], str]]:
    """Return list of (model_path, adapter_path, label) in preference order for ablation."""
    out = []
    if run_entry.get("quantized_dir"):
        out.append((run_entry["quantized_dir"], None, "quantized"))
    if run_entry.get("fused_dir"):
        out.append((run_entry["fused_dir"], None, "fused"))
    # fallback: base + adapter
    out.append((run_entry["model_id"], run_entry["adapter_dir"], "base+adapter"))
    # dedup preserve order
    seen = set(); uniq=[]
    for m,a,label in out:
        key=(m,a or "")
        if key in seen: continue
        seen.add(key); uniq.append((m,a,label))
    return uniq

# --- Prompt variants ---
def pv_plain(prompt: str) -> str:
    return prompt

def pv_directive(prompt: str) -> str:
    return f"{prompt}\n\nAnswer with a single important thought:"

def pv_fewshot(prompt: str) -> str:
    shots = [
        "The moon does not race the tide.",
        "A river carves stone by lingering.",
    ]
    return "Proverbs:\n- " + "\n- ".join(shots) + f"\n\n{prompt}\n- "

PROMPT_VARIANTS = [
    ("plain", pv_plain),
    ("directive", pv_directive),
    ("fewshot", pv_fewshot),
]

def run_generation(model_path: str, adapter_path: Optional[str], prompts: List[str], max_new: int):
    model, tok = mlx_load(model_path, adapter_path=adapter_path or None)
    outs=[]
    for p in prompts:
        txt = mlx_generate(model=model, tokenizer=tok, prompt=p, max_tokens=max_new)
        cont = txt[len(p):] if txt.startswith(p) else txt  # strip echoed prompt
        outs.append(cont.strip())
    meta = {
        "eos_token": getattr(tok, "eos_token", None),
        "eos_token_id": getattr(tok, "eos_token_id", None),
        "pad_token": getattr(tok, "pad_token", None),
        "pad_token_id": getattr(tok, "pad_token_id", None),
    }
    return outs, meta

def preview(text: str, width=120) -> str:
    return textwrap.shorten(text.replace("\n"," ⏎ "), width=width, placeholder="…")

# --- Orchestrate ---
runs = load_runs()
stamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
rows=[]

for run in runs:
    art_list = pick_artifacts(run)

    for model_path, adapter_path, art_label in art_list:
        for pv_label, pv_fn in PROMPT_VARIANTS:
            prompts_v = [pv_fn(p) for p in PROMPTS]

            outs_short, meta = run_generation(model_path, adapter_path, prompts_v, MAX_NEW_TOKENS_SHORT)
            outs_long,  _    = run_generation(model_path, adapter_path, prompts_v, MAX_NEW_TOKENS_LONG)

            print(f"\n=== {run['model_id']} | {art_label} | {pv_label} | max_new={MAX_NEW_TOKENS_SHORT} ===")
            for p, o in zip(PROMPTS, outs_short):
                print(f"- {p}\n→ {preview(o)}")

            print(f"\n=== {run['model_id']} | {art_label} | {pv_label} | max_new={MAX_NEW_TOKENS_LONG} ===")
            for p, o in zip(PROMPTS, outs_long):
                print(f"- {p}\n→ {preview(o)}")

            # record minimal table
            for budget, outs in [("short", outs_short), ("long", outs_long)]:
                for p, o in zip(PROMPTS, outs):
                    rows.append({
                        "timestamp_utc": stamp,
                        "model_id": run["model_id"],
                        "artifact": art_label,
                        "prompt_variant": pv_label,
                        "budget": budget,
                        "model_path": model_path,
                        "adapter_path": adapter_path or "",
                        "eos_token": meta["eos_token"],
                        "eos_token_id": meta["eos_token_id"],
                        "prompt": p,
                        "generation": o,
                        "len_chars": len(o),
                        "len_words": len(o.split()),
                        "is_empty": int(len(o.strip())==0),
                    })

# --- Save quick JSONL ---
with ABL_PATH.open("w", encoding="utf-8") as f:
    for r in rows:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")

print(f"\nSaved detailed ablation outputs to {ABL_PATH}")

# --- Save grouped YAML ---
try:
    import yaml
except ImportError:
    raise RuntimeError("PyYAML is required to write ablations.yaml. Install with: pip install pyyaml")

grouped = defaultdict(list)
for r in rows:
    prompt = (r.get("prompt") or "").strip()
    grouped[prompt].append(r)

with ABL_YAML.open("w", encoding="utf-8") as yf:
    yaml.safe_dump(dict(grouped), yf, allow_unicode=True, sort_keys=False)

print(f"Wrote grouped YAML → {ABL_YAML}")
print("Tip: Look for cases where 'fused' + 'fewshot' fills in while 'quantized' + 'plain' is empty.")
