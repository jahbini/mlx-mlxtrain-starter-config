# scripts/05_eval.py
# STEP 13 — Comparative Report & Policy Lock-in
# Inputs:
#   - eval_out/ablations.jsonl   (from Step 12)
#   - artifacts.json             (for artifact names)
# Outputs:
#   - eval_out/report.md
#   - eval_out/generation_policy.json

from __future__ import annotations
import os, sys, json, textwrap, time
from pathlib import Path
from typing import Dict, Any
import pandas as pd

# --- Config loader ---
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config

# --- STEP-AWARE CONFIG ---
CFG       = load_config()
STEP_NAME = os.environ["STEP_NAME"]
STEP_CFG  = CFG.pipeline.steps[STEP_NAME]
PARAMS    = getattr(STEP_CFG, "params", {})

EVAL_DIR  = Path(getattr(PARAMS, "eval_output_dir", CFG.eval.output_dir)); EVAL_DIR.mkdir(exist_ok=True)
RUN_DIR   = Path(getattr(PARAMS, "run_dir", CFG.run.output_dir))

ARTIFACTS = RUN_DIR / getattr(PARAMS, "artifacts", CFG.data.artifacts)
ABL_JSONL = EVAL_DIR / (getattr(PARAMS, "ablations", CFG.eval.ablations) + ".jsonl")
REPORT_MD = EVAL_DIR / getattr(PARAMS, "report", CFG.eval.report)
POLICY_JS = EVAL_DIR / getattr(PARAMS, "policy", CFG.eval.policy)

if not ABL_JSONL.exists():
    raise SystemExit("Missing eval_out/ablations.jsonl (run Step 12).")

# -----------------------------
# Load JSONL → DataFrame
# -----------------------------
rows = [json.loads(l) for l in ABL_JSONL.read_text(encoding="utf-8").splitlines() if l.strip()]
df = pd.DataFrame(rows)

# -----------------------------
# Metrics per (artifact, prompt_variant)
# -----------------------------
def summarize(g: pd.DataFrame) -> pd.Series:
    n = len(g)
    empty_rate = g["is_empty"].astype(int).sum() / max(1, n)
    sent_end_rate = g["generation"].fillna("").str.strip().str.endswith(tuple(".!?…")).astype(int).sum() / max(1, n)
    avg_len = g["len_words"].mean()
    med_len = g["len_words"].median()
    return pd.Series(dict(
        n=n,
        empty_rate=empty_rate,
        sent_end_rate=sent_end_rate,
        avg_len=round(float(avg_len),3),
        med_len=float(med_len)
    ))

agg = df.groupby(["model_id","artifact","prompt_variant"], as_index=False, group_keys=False).apply(summarize)

# -----------------------------
# Winner / runner-up selection
# -----------------------------
ranked = agg.sort_values(["empty_rate","sent_end_rate","avg_len"], ascending=[True,False,False])
winner    = ranked.iloc[0].to_dict()
runner_up = ranked.iloc[1].to_dict() if len(ranked) > 1 else None

# -----------------------------
# Markdown report
# -----------------------------
def pct(x): return f"{x*100:.1f}%"
table = agg.copy()
table["empty_rate"] = table["empty_rate"].map(pct)
table["sent_end_rate"] = table["sent_end_rate"].map(pct)

ts = time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime())
lines = []
lines += [f"# Learning Ablation Report  \n_{ts}_\n"]
lines += ["## Summary by artifact × prompt_variant"]
lines += ["\n| model | artifact | prompt_variant | n | empty_rate | sent_end_rate | avg_len | med_len |",
          "|-------|----------|----------------|---:|-----------:|--------------:|--------:|--------:|"]
for _, r in table.iterrows():
    lines += [f"| {r['model_id']} | {r['artifact']} | {r['prompt_variant']} | {int(r['n'])} | {r['empty_rate']} | {r['sent_end_rate']} | {r['avg_len']} | {int(r['med_len'])} |"]

lines += ["\n## Chosen policy"]
lines += ["\n### Winner"]
lines += [f"- **artifact**: `{winner['artifact']}`",
          f"- **prompt_variant**: `{winner['prompt_variant']}`",
          "- Rationale: lowest empty rate, then prefer sentence endings and adequate length."]

if runner_up:
    lines += ["\n### Runner-up"]
    lines += [f"- **artifact**: `{runner_up['artifact']}`",
              f"- **prompt_variant**: `{runner_up['prompt_variant']}`"]

# Sample outputs (winner policy, long budget)
mask = (
    (df["artifact"]==winner["artifact"]) &
    (df["prompt_variant"]==winner["prompt_variant"]) &
    (df["budget"]=="long")
)
sample = df[mask].groupby("prompt").head(1)
lines += ["\n## Sample outputs (winner policy)"]
for _, r in sample.iterrows():
    gen = textwrap.shorten(str(r["generation"]).replace("\n"," ⏎ "), width=160, placeholder="…")
    lines += [f"- **{r['prompt']}** → {gen}"]

REPORT_MD.write_text("\n".join(lines), encoding="utf-8")
print(f"[OK] Wrote {REPORT_MD}")

# -----------------------------
# Policy JSON (reusable)
# -----------------------------
POLICY = {
    "created_utc": ts,
    "artifact_preference": [winner["artifact"], "fused", "adapter"],  # fallback order
    "prompt_policy": {
        "name": winner["prompt_variant"],
        "fewshot": {
            "shots": [
                "The moon does not race the tide.",
                "A river carves stone by lingering."
            ],
            "prefix": "Proverbs:\n- ",
            "joiner": "\n- ",
            "suffix": "\n\n{prompt}\n- "
        },
        "directive": {
            "suffix": "\n\nAnswer with a single important thought:"
        }
    }
}
POLICY_JS.write_text(json.dumps(POLICY, indent=2), encoding="utf-8")
print(f"[OK] Wrote {POLICY_JS}")

# -----------------------------
# Console preview
# -----------------------------
print("\n=== WINNER ===")
print(f"model={winner['model_id']} --- artifact={winner['artifact']}  prompt_variant={winner['prompt_variant']}")
if runner_up:
    print("\n=== RUNNER-UP ===")
    print(f"model={runner_up['model_id']} --- artifact={runner_up['artifact']}  prompt_variant={runner_up['prompt_variant']}")
print("\n=== TABLE ===")
print(agg.to_string(index=False))
