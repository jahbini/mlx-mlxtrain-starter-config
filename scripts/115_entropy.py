# scripts/115_entropy.py
# STEP 11.5 — Entropy meter for generation (per-token + per-sample)
# Outputs:
#   eval_out/entropy_tokens.jsonl   (one record per generated token)
#   eval_out/entropy_summary.csv    (one record per prompt/result)

"""
Entropy Meter for Language Model Generations
--------------------------------------------
This script measures *per-token uncertainty* during generation.

Each generation step, the model emits log-probabilities over its vocabulary.
We convert those to probabilities and compute Shannon entropy:
    H(p) = -Σ p * log(p)
Low entropy → model is confident; high entropy → model is uncertain.
"""

from __future__ import annotations
import os, sys, json, math, csv, time
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

from mlx_lm import load as mlx_load
from mlx_lm.generate import stream_generate  # yields GenerationResponse objects

# --- Config loader ---
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config

# --- STEP-AWARE CONFIG ---
CFG       = load_config()
STEP_NAME = os.environ["STEP_NAME"]
STEP_CFG  = CFG[STEP_NAME]
PARAMS    = STEP_CFG

EVAL_DIR  = Path( CFG.eval.output_dir); EVAL_DIR.mkdir(exist_ok=True)
RUN_DIR   = Path( CFG.run.output_dir)

ARTIFACTS     = RUN_DIR / CFG.data.artifacts)
POLICY_JSON   = EVAL_DIR / CFG.eval.policy)
GEN_JSONL     = EVAL_DIR / (CFG.eval.generations) + ".jsonl")

TOK_PATH      = EVAL_DIR / "entropy_tokens.jsonl"
SUM_PATH      = EVAL_DIR / "entropy_summary.csv"

MAX_NEW   = int(STEP_CFG.max_new_tokens)
STOP_STRS = getattr(PARAMS, "stop_strings", ["\n\n", "==="])

# --------------------------
# Helpers
# --------------------------
def load_policy() -> Dict[str, Any]:
    if POLICY_JSON.exists():
        return json.loads(POLICY_JSON.read_text(encoding="utf-8"))
    return {"prompt_policy": {"name": "plain"}, "artifact_preference": ["quantized","fused","adapter"]}

def load_prompts_from_generations(path: Path) -> List[str]:
    prompts = []
    if not path.exists():
        raise SystemExit(f"Missing generations.jsonl at {path}")
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            obj = json.loads(line)
            if "prompt" in obj:
                prompts.append(obj["prompt"])
    return prompts

def pick_artifact(artifacts_path: Path, policy: Dict[str, Any]) -> Tuple[str, Optional[str], str]:
    pref = policy.get("artifact_preference", ["quantized","fused","adapter"])
    data = json.loads(artifacts_path.read_text(encoding="utf-8"))
    runs = data.get("runs", [])
    if not runs:
        raise RuntimeError("No runs in artifacts.json")

    cands = []
    for run in reversed(runs):  # newest last
        model_id   = run.get("model_id","").strip()
        adapter    = (run.get("adapter_dir") or "").strip() or None
        fused_dir  = (run.get("fused_dir") or "").strip()
        quant_dir  = (run.get("quantized_dir") or "").strip()
        if quant_dir and Path(quant_dir).exists():
            cands.append(("quantized", quant_dir, None))
        if fused_dir and Path(fused_dir).exists():
            cands.append(("fused", fused_dir, None))
        if adapter and model_id:
            cands.append(("adapter", model_id, adapter))

    for want in pref:
        for lab, mpath, apath in cands:
            if lab == want:
                return mpath, apath, lab
    return cands[0][1], cands[0][2], cands[0][0]

def entropy_from_logprobs(logprobs) -> float:
    vals = list(logprobs)
    m = max(vals)
    exps = [math.exp(v - m) for v in vals]
    Z = sum(exps)
    ps = [e / (Z + 1e-12) for e in exps]
    return -sum(p * math.log(p + 1e-12) for p in ps)

def trim_on_stops(text: str, stops: List[str]) -> str:
    cut = len(text)
    for s in stops:
        i = text.find(s)
        if i != -1:
            cut = min(cut, i)
    return text[:cut]

def median(xs: List[float]) -> float:
    if not xs: return 0.0
    ys = sorted(xs); n = len(ys); h = n//2
    return (ys[h] if n%2 else 0.5*(ys[h-1]+ys[h]))

def apply_prompt_policy(prompt: str, policy: Dict[str,Any]) -> str:
    pp = policy.get("prompt_policy", {"name": "plain"})
    name = pp.get("name", "plain")
    if name == "directive":
        return f"{prompt}{pp.get('directive',{}).get('suffix','')}"
    if name == "fewshot":
        fs = pp.get("fewshot", {})
        shots  = fs.get("shots", [])
        prefix = fs.get("prefix","")
        joiner = fs.get("joiner","\n")
        suffix = fs.get("suffix","\n")
        return f"{prefix}{joiner.join(shots)}{suffix}".replace("{prompt}", prompt)
    return prompt

# --------------------------
# Main
# --------------------------
policy   = load_policy()
prompts  = load_prompts_from_generations(GEN_JSONL)
model_path, adapter_path, artifact_label = pick_artifact(ARTIFACTS, policy)
model, tok = mlx_load(model_path, adapter_path=adapter_path)

toks_f = TOK_PATH.open("w", encoding="utf-8", newline="")
sum_f  = SUM_PATH.open("w", encoding="utf-8", newline="")
sum_writer = csv.writer(sum_f)
sum_writer.writerow(["artifact","prompt_idx","prompt","tokens","mean_entropy","median_entropy","min_entropy","max_entropy"])

for i, user_prompt in enumerate(prompts):
    full_prompt = apply_prompt_policy(user_prompt, policy)
    buf = ""
    entropies: List[float] = []
    token_ids: List[int] = []

    for step in stream_generate(model=model, tokenizer=tok, prompt=full_prompt, max_tokens=MAX_NEW):
        token_id   = step.token
        piece      = step.text
        logprobs   = step.logprobs

        buf += piece
        token_ids.append(token_id)

        if logprobs is not None:
            H = entropy_from_logprobs(logprobs.tolist())
            entropies.append(H)
            rec = {
                "artifact": artifact_label,
                "prompt_idx": i,
                "token_index": len(token_ids)-1,
                "token_id": token_id,
                "token_text": piece,
                "entropy": H,
            }
            toks_f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        if any(s in buf for s in STOP_STRS):
            break

    gen = trim_on_stops(buf, STOP_STRS).strip()
    mean_H = sum(entropies)/len(entropies) if entropies else 0.0
    sum_writer.writerow([artifact_label, i, user_prompt, len(token_ids),
                         f"{mean_H:.4f}", f"{median(entropies):.4f}",
                         f"{min(entropies) if entropies else 0.0:.4f}",
                         f"{max(entropies) if entropies else 0.0:.4f}"])

toks_f.close(); sum_f.close()
print(f"[OK] Wrote per-token → {TOK_PATH}")
print(f"[OK] Wrote per-sample → {SUM_PATH}")
