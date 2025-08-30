# STEP 13 — Comparative Report & Policy Lock-in
# Inputs:
#   - eval_out/ablation_generations.jsonl   (from Step 12)
#   - artifacts.json                        (for artifact names)
# Outputs:
#   - eval_out/report.md
#   - generation_policy.json  (chosen artifact & prompt template with params)

from __future__ import annotations
import os, sys, json, pandas as pd, textwrap, time
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config
from pathlib import Path

ABL_JSONL = Path("eval_out/ablation_generations.jsonl")
REPORT_MD = Path("eval_out/report.md")
POLICY_JS = Path("generation_policy.json")
cfg = load_config()
#OUT_DIR       = Path(cfg.data.output_dir); OUT_DIR.mkdir(exist_ok=True)
EVAL_DIR      = Path(cfg.eval.output_dir); EVAL_DIR.mkdir(exist_ok=True)
RUN_DIR       = Path(cfg.run.output_dir)
#EXPERIMENTS   = RUN_DIR / cfg.run.experiments
ARTIFACTS     = RUN_DIR / cfg.run.artifacts
#CONTRACT      = OUT_DIR / cfg.paths.contract
#GEN_JSONL     = EVAL_DIR / (cfg.paths.generations + ".jsonl" )
#GEN_CSV       = EVAL_DIR / (cfg.paths.generations + ".csv")
OUT_SUM       = EVAL_DIR / (cfg.paths.summary + ".csv")
OUT_JSON      = EVAL_DIR / (cfg.paths.analysis + ".json")
ABL_JSONL     = EVAL_DIR / cfg.paths.ablations
REPORT_MD     = EVAL_DIR / cfg.eval.report
POLICY_JS     = EVAL_DIR / cfg.eval.policy
if not ABL_JSONL.exists():
    raise SystemExit("Missing eval_out/ablation_generations.jsonl (run Step 12).")

# Load
rows = [json.loads(l) for l in ABL_JSONL.read_text(encoding="utf-8").splitlines() if l.strip()]
df = pd.DataFrame(rows)

# Score per (artifact, prompt_variant)
def summarize(g):
    n = len(g)
    empty_rate = (g["is_empty"].astype(int).sum()) / max(1, n)
    sent_end_rate = (g["generation"].fillna("").str.strip().str.endswith(tuple(".!?…")).astype(int).sum()) / max(1, n)
    avg_len = g["len_words"].mean()
    med_len = g["len_words"].median()
    return pd.Series(dict(
        n=n, empty_rate=empty_rate, sent_end_rate=sent_end_rate,
        avg_len=round(float(avg_len),3), med_len=float(med_len)
    ))

agg = df.groupby(["artifact","prompt_variant"], as_index=False, group_keys=False).apply(summarize)

# Pick winner by heuristic:
# 1) lowest empty_rate, 2) highest sent_end_rate, 3) highest avg_len
winner = (agg.sort_values(["empty_rate","sent_end_rate","avg_len"], ascending=[True,False,False])
            .iloc[0].to_dict())

# Build a human-friendly table
def pct(x): return f"{x*100:.1f}%"
table = agg.copy()
table["empty_rate"] = table["empty_rate"].map(pct)
table["sent_end_rate"] = table["sent_end_rate"].map(pct)

# Draft a short markdown report
ts = time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime())
lines = []
lines += [f"# Learning Ablation Report  \n_{ts}_\n"]
lines += ["## Summary by artifact × prompt_variant"]
lines += ["\n| artifact | prompt_variant | n | empty_rate | sent_end_rate | avg_len | med_len |",
          "|---|---:|---:|---:|---:|---:|---:|"]
for _, r in table.iterrows():
    lines += [f"| {r['artifact']} | {r['prompt_variant']} | {int(r['n'])} | {r['empty_rate']} | {r['sent_end_rate']} | {r['avg_len']} | {int(r['med_len'])} |"]

lines += ["\n## Chosen policy"]
lines += [f"- **artifact**: `{winner['artifact']}`",
          f"- **prompt_variant**: `{winner['prompt_variant']}`",
          "- Rationale: minimize empty outputs, then prefer clean sentence endings and adequate length."]

# Add a tiny sample grid (first row per prompt for the winner)
win_mask = (df["artifact"]==winner["artifact"]) & (df["prompt_variant"]==winner["prompt_variant"]) & (df["budget"]=="long")
sample = df[win_mask].groupby("prompt").head(1)
lines += ["\n## Sample outputs (winner policy)"]
for _, r in sample.iterrows():
    gen = textwrap.shorten(str(r["generation"]).replace("\n"," ⏎ "), width=160, placeholder="…")
    lines += [f"- **{r['prompt']}** → {gen}"]

REPORT_MD.write_text("\n".join(lines), encoding="utf-8")
print(f"Wrote {REPORT_MD}")

# Save a reusable generation policy (Step 10 can read this later)
# Encode few-shot template explicitly so you can tweak the shots later.
POLICY = {
    "created_utc": ts,
    "artifact_preference": [winner["artifact"], "fused", "adapter"],  # fallbacks
    "prompt_policy": {
        "name": winner["prompt_variant"],
        "fewshot": {
            "shots": [
                "The moon does not race the tide.",
                "A river carves stone by lingering."
            ],
            "prefix": "some ideas:\n- ",
            "joiner": "\n- ",
            "suffix": "\n\n{prompt}\n- "
        },
        "directive": {
            "suffix": "\n\nAnswer with a single saying:"
        }
    }
}
POLICY_JS.write_text(json.dumps(POLICY, indent=2), encoding="utf-8")
print(f"Wrote {POLICY_JS}")

# Console preview
print("\n=== WINNER ===")
print(f"artifact={winner['artifact']}  prompt_variant={winner['prompt_variant']}")
print("\n=== TABLE ===")
print(agg.to_string(index=False))
