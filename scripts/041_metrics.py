# STEP 11 — EOS Behavior Probe & Quick Analysis (revised, JSONL-first)
# Reads eval_out/generations.jsonl to avoid NaN coercion, computes the same stats,
# and shows any rows that CSV parsing would have treated as NaN.

from __future__ import annotations
import os, sys, json, re, statistics, pandas as pd
from pathlib import Path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config
cfg = load_config()
OUT_DIR       = Path(cfg.run.output_dir + "/" + cfg.data.output_dir); OUT_DIR.mkdir(exist_ok=True)
EVAL_DIR      = Path(cfg.eval.output_dir); EVAL_DIR.mkdir(exist_ok=True)
RUN_DIR       = Path(cfg.run.output_dir)  # where per-model outputs will go
EXPERIMENTS   = RUN_DIR / cfg.run.experiments
#ARTIFACTS     = RUN_DIR / cfg.run.artifacts
CONTRACT      = OUT_DIR / cfg.paths.contract
GEN_JSONL     = EVAL_DIR / (cfg.paths.generations + ".jsonl" )
GEN_CSV       = EVAL_DIR / (cfg.paths.generations + ".csv")
OUT_SUM       = EVAL_DIR / (cfg.paths.summary + ".csv")
OUT_JSON      = EVAL_DIR / (cfg.paths.analysis + ".json")


if not GEN_JSONL.exists():
    raise SystemExit("Missing eval_out/generations.jsonl (run Step 10).")
if not CONTRACT.exists():
    raise SystemExit("Missing data_contract.json (from Step 2).")

# ---- Load generations from JSONL (authoritative) ----
rows = []
with GEN_JSONL.open("r", encoding="utf-8") as f:
    for line in f:
        if line.strip():
            rows.append(json.loads(line))
df = pd.DataFrame(rows)

# ---- (Optional) CSV diagnostics: which rows would be NaN? ----
csv_missing = pd.DataFrame()
if GEN_CSV.exists():
    df_csv = pd.read_csv(GEN_CSV, keep_default_na=False, na_filter=False)
    # If your earlier CSV had blank cells, reading with default settings would coerce to NaN.
    # keep_default_na=False prevents that. We diff the two to surface any mismatches.
    if len(df_csv) != len(df):
        csv_missing = pd.concat([df, df_csv]).drop_duplicates(keep=False)

# ----- Helpers -----
def word_count(s: str) -> int: return len(s.split())
def ends_with_terminator(s: str) -> bool: return bool(re.search(r"[.!?…]$", s.strip()))
def has_trailing_whitespace(s: str) -> bool: return len(s) > 0 and s[-1].isspace()
def distinct_n(tokens, n=1):
    if len(tokens) < n: return 0.0
    ngrams = set(tuple(tokens[i:i+n]) for i in range(len(tokens)-n+1))
    return len(ngrams) / max(1, (len(tokens)-n+1))

# Load training examples for simple memorization checks
c = json.loads(CONTRACT.read_text(encoding="utf-8"))
train_path = Path(c["filenames"]["train"]["resolved"])
text_field = next((k for k,v in c["schema"]["fields"].items() if str(v).lower()=="string"), "text")
train_texts = []
with train_path.open("r", encoding="utf-8") as f:
    for line in f:
        try:
            obj = json.loads(line); t = obj.get(text_field, "")
            if isinstance(t, str): train_texts.append(t.strip())
        except Exception:
            pass
train_blob = "\n\n".join(train_texts)
train_set = set(train_texts)

# ----- Per-row metrics -----
def row_metrics(r):
    gen = str(r.get("generation", ""))
    toks = gen.split()
    d1 = distinct_n(toks, 1); d2 = distinct_n(toks, 2)
    exact_mem = gen.strip() in train_set
    substr_mem = (not exact_mem) and (len(gen.strip()) >= 20) and (gen.strip() in train_blob)
    return {
        **r,
        "len_chars": len(gen),
        "len_words": word_count(gen),
        "ends_sentence": int(ends_with_terminator(gen)),
        "ends_whitespace": int(has_trailing_whitespace(gen)),
        "distinct1": round(d1, 4),
        "distinct2": round(d2, 4),
        "memorized_exact": int(exact_mem),
        "memorized_substring": int(substr_mem),
    }

m = pd.DataFrame([row_metrics(r) for r in df.to_dict(orient="records")])

# ----- Aggregate by mode -----
agg = (m.groupby("mode")
         .agg(
             n=("generation","count"),
             avg_len_chars=("len_chars","mean"),
             med_len_chars=("len_chars","median"),
             avg_len_words=("len_words","mean"),
             sent_end_rate=("ends_sentence","mean"),
             trailing_ws_rate=("ends_whitespace","mean"),
             distinct1_mean=("distinct1","mean"),
             distinct2_mean=("distinct2","mean"),
             mem_exact_rate=("memorized_exact","mean"),
             mem_sub_rate=("memorized_substring","mean"),
           )
         .reset_index())

for col in ["avg_len_chars","med_len_chars","avg_len_words","sent_end_rate","trailing_ws_rate","distinct1_mean","distinct2_mean","mem_exact_rate","mem_sub_rate"]:
    if col in agg.columns:
        agg[col] = agg[col].map(lambda x: round(float(x), 4))

# ----- Per-prompt sample table -----
def sample_table(df_in: pd.DataFrame, n=1):
    out_rows = []
    for prompt, g in df_in.groupby("prompt"):
        for mode, gg in g.groupby("mode"):
            for _, rr in gg.head(n).iterrows():
                out_rows.append({"prompt": prompt, "mode": mode, "generation": rr["generation"]})
    return pd.DataFrame(out_rows)
preview = sample_table(m, n=1)

# ----- Save outputs -----
OUT_SUM.parent.mkdir(parents=True, exist_ok=True)
agg.to_csv(OUT_SUM, index=False)

analysis = {
    "created_utc": __import__("time").strftime("%Y-%m-%dT%H:%M:%SZ", __import__("time").gmtime()),
    "by_mode": agg.to_dict(orient="records"),
    "notes": [
        "JSONL is used as source of truth to avoid NaN coercion from CSV parsing.",
        "distinct* ~ lexical diversity over whitespace tokens.",
        "memorized_* checks generation against training set (exact / long substring).",
    ],
}
OUT_JSON.write_text(json.dumps(analysis, indent=2), encoding="utf-8")

# ----- Console summary -----
print("=== EOS / OUTPUT ANALYSIS (by mode) [JSONL] ===")
print(agg.to_string(index=False))

print("\n=== SAMPLE OUTPUTS (1 per prompt×mode) ===")
for _, row in preview.iterrows():
    print(f"\n[{row['mode']}] {row['prompt']}\n→ {row['generation']}")

if not csv_missing.empty:
    print("\n[CSV diagnostic] These rows mismatch when parsing CSV with defaults; JSONL kept them:")
    display(csv_missing[["mode","prompt","generation"]].head(6))

print(f"\nWrote: {OUT_SUM} and {OUT_JSON}")
