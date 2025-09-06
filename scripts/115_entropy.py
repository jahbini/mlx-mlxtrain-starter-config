# STEP 11.5 — Entropy meter for generation (per-token + per-sample)
# Outputs:
#   eval_out/entropy_tokens.jsonl   (one record per generated token)
#   eval_out/entropy_summary.csv    (one record per prompt/result)

from __future__ import annotations
import os, json, math, csv, time
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

from mlx_lm import load as mlx_load
from mlx_lm.generate import stream_generate  # yields (token_id, logprobs, from_draft?) per step

# ---------- Inputs ----------
OUT_DIR = Path("eval_out"); OUT_DIR.mkdir(parents=True, exist_ok=True)
POLICY_JSON = Path("generation_policy.json")   # step 2.2 output
PROMPTS_JSON = Path("prompts_eval.json")       # step 2.2 output
ARTIFACTS = Path("runs/artifacts.json")        # your registry; adjust if needed

MAX_NEW = 128
STOP_STRS = ["\n\n","==="]   # post-trim sentinels (no fragile kwargs)

# ---------- Helpers ----------
def load_policy() -> Dict[str,Any]:
    return json.loads(POLICY_JSON.read_text(encoding="utf-8"))

def load_prompts() -> List[str]:
    return json.loads(PROMPTS_JSON.read_text(encoding="utf-8"))

def pick_artifact(artifacts_path: Path) -> Tuple[str, Optional[str], str]:
    """
    Choose model_path + adapter_path according to artifact_preference in policy.
    Prefers: quantized > fused > adapter (base+adapter).
    """
    policy = load_policy()
    pref = policy.get("artifact_preference", ["quantized","fused","adapter"])

    data = json.loads(artifacts_path.read_text(encoding="utf-8"))
    runs = data["runs"] if isinstance(data, dict) and "runs" in data else data
    # Last run is newest in our pipeline
    runs = list(runs)
    if not runs:
        raise RuntimeError("No runs in artifacts.json")

    # Build candidates from newest to oldest
    cands = []
    for run in reversed(runs):
        model_id   = (run.get("model_id") or "").strip()
        adapter    = (run.get("adapter_dir") or "").strip() or None
        fused_dir  = (run.get("fused_dir") or "").strip()
        quant_dir  = (run.get("quantized_dir") or "").strip()
        if quant_dir and Path(quant_dir).exists():
            cands.append(("quantized", quant_dir, None))
        if fused_dir and Path(fused_dir).exists():
            cands.append(("fused", fused_dir, None))
        if adapter and model_id:
            cands.append(("adapter", model_id, adapter))

    # honor preference order
    for want in pref:
        for lab, mpath, apath in cands:
            if lab == want:
                return mpath, apath, lab

    # fallback
    lab, mpath, apath = cands[0]
    return mpath, apath, lab

def entropy_from_logprobs(logprobs: Dict[int, float]) -> float:
    """
    Compute Shannon entropy H(p) = -sum p*log p from a dict of log-probs OR logits slice.
    We normalize in case values are log-probs or raw logits of a top-k subset.
    """
    # Convert (possibly unnormalized) log-weights to probabilities
    vals = list(logprobs.values())
    m = max(vals)
    exps = [math.exp(v - m) for v in vals]  # log-sum-exp trick
    Z = sum(exps)
    ps = [e / (Z + 1e-12) for e in exps]
    # Entropy
    H = -sum(p * math.log(p + 1e-12) for p in ps)
    return H

def trim_on_stops(text: str, stops: List[str]) -> str:
    cut = len(text)
    for s in stops:
        i = text.find(s)
        if i != -1:
            cut = min(cut, i)
    return text[:cut]

def apply_prompt_policy(prompt: str, policy: Dict[str,Any]) -> str:
    pp = policy.get("prompt_policy", {"name": "plain"})
    name = pp.get("name", "plain")
    if name == "directive":
        suf = pp.get("directive", {}).get("suffix", "")
        return f"{prompt}{suf}"
    if name == "fewshot":
        fs = pp.get("fewshot", {})
        shots  = fs.get("shots", [])
        prefix = fs.get("prefix", "")
        joiner = fs.get("joiner", "\n")
        suffix = fs.get("suffix", "\n")
        return f"{prefix}{joiner.join(shots)}{suffix}".replace("{prompt}", prompt)
    return prompt

# ---------- Load model ----------
policy = load_policy()
prompts = load_prompts()
model_path, adapter_path, artifact_label = pick_artifact(ARTIFACTS)
model, tok = mlx_load(model_path, adapter_path=adapter_path)

# ---------- Outputs ----------
tok_path = OUT_DIR / "entropy_tokens.jsonl"
sum_path = OUT_DIR / "entropy_summary.csv"
toks_f = tok_path.open("w", encoding="utf-8", newline="")
sum_f  = sum_path.open("w", encoding="utf-8", newline="")
sum_writer = csv.writer(sum_f)
sum_writer.writerow(["artifact","prompt_idx","prompt","tokens","mean_entropy","median_entropy","min_entropy","max_entropy"])

def median(xs: List[float]) -> float:
    if not xs: return 0.0
    ys = sorted(xs); n = len(ys); h = n//2
    return (ys[h] if n%2 else 0.5*(ys[h-1]+ys[h]))

# ---------- Generate with entropy logging ----------
for i, user_prompt in enumerate(prompts):
    full_prompt = apply_prompt_policy(user_prompt, policy)
    # Build initial text buffer to trim later.
    # We'll stream tokens; for each step, we receive (token_id, logprobs, from_draft)
    buf = ""
    entropies: List[float] = []
    token_ids: List[int] = []

    for token_id, logprobs, _from_draft in stream_generate(
        model=model,
        tokenizer=tok,
        prompt=full_prompt,
        max_tokens=MAX_NEW,
    ):
        piece = tok.decode([token_id])
        buf += piece
        token_ids.append(token_id)

        # logprobs is typically a dict {token_id: logprob or logits_slice}
        if isinstance(logprobs, dict) and logprobs:
            H = entropy_from_logprobs(logprobs)
            entropies.append(H)
            # write per-token record
            rec = {
                "artifact": artifact_label,
                "prompt_idx": i,
                "token_index": len(token_ids)-1,
                "token_id": token_id,
                "token_text": piece,
                "entropy": H,
            }
            toks_f.write(json.dumps(rec, ensure_ascii=False) + "\n")

        # Optional early break on sentinel (keeps loops in check)
        if any(s in buf for s in STOP_STRS):
            break

    # Post-trim for cleanliness
    gen = buf
    gen = trim_on_stops(gen, STOP_STRS).strip()
    # Summaries
    mean_H = sum(entropies)/len(entropies) if entropies else 0.0
    med_H  = median(entropies)
    min_H  = min(entropies) if entropies else 0.0
    max_H  = max(entropies) if entropies else 0.0
    sum_writer.writerow([artifact_label, i, user_prompt, len(token_ids), f"{mean_H:.4f}", f"{med_H:.4f}", f"{min_H:.4f}", f"{max_H:.4f}"])

toks_f.close()
sum_f.close()
print(f"Wrote per-token:   {tok_path}")
print(f"Wrote per-sample:  {sum_path}")
