from __future__ import annotations
from pathlib import Path
import sys, os, json, random, hashlib, csv, time
from datasets import load_from_disk
from typing import List, Optional
import pandas as pd

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config
from mlx_lm import load as mlx_load, generate as mlx_generate

import os, sys
from pathlib import Path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config

CFG = load_config()
STEP_NAME = os.environ["STEP_NAME"]
STEP_CFG  = CFG.pipeline.steps[STEP_NAME]

# --- Config ---
EXPERIMENTS_CSV = Path(cfg.run.output_dir) / cfg.data.experiments_csv
PROMPTS = cfg.snapshot.prompts
SNAPSHOT = Path(cfg.snapshot.output_dir)
MAX_NEW = cfg.snapshot.max_new
SEED = cfg.run.alt_seed
N_SHOTS = cfg.snapshot.n_shots
MIN_WORDS = cfg.snapshot.min_words
RETRIES = cfg.snapshot.retries

OUT_DIR = Path(cfg.data.output_dir)
OUT_DIR.mkdir(exist_ok=True)
EVAL_DIR = Path(cfg.eval.output_dir)
EVAL_DIR.mkdir(exist_ok=True)
CONTRACT = OUT_DIR / cfg.data.contract

JSONL_PATH = EVAL_DIR / (cfg.eval.generations + ".jsonl")
CSV_PATH = EVAL_DIR / (cfg.eval.generations + ".csv")
TOKMETA = OUT_DIR / (cfg.paths.tokenizer + ".json")
CUSTOM_STOP = "\n\n"
MODES = ["default_eos", "no_eos", "custom_stop"]
os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
random.seed(SEED)

# --- Load corpus for few-shot + anti-copy ---
contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
text_field = next((k for k, v in contract["schema"]["fields"].items() if str(v).lower() == "string"), "text")
train_path = Path(contract["filenames"]["train"]["resolved"])

def sha(s: str) -> str: return hashlib.sha256(s.encode("utf-8", "ignore")).hexdigest()
def wc(s: str) -> int: return len(s.split())

train_lines: List[str] = []
with train_path.open("r", encoding="utf-8") as f:
    for line in f:
        try:
            obj = json.loads(line)
            t = obj.get(text_field, "")
            if isinstance(t, str) and t.strip():
                train_lines.append(t.strip())
        except Exception:
            pass

# dedupe + length buckets
seen = set(); unique = []
for t in train_lines:
    h = sha(t)
    if h not in seen:
        seen.add(h); unique.append(t)
short = [t for t in unique if wc(t) <= 4]
medium = [t for t in unique if 5 <= wc(t) <= 12]
longer = [t for t in unique if wc(t) > 12]
train_blob = "\n\n".join(unique)
train_set = set(unique)

def pick_diverse_shots(k: int) -> List[str]:
    pool = []
    if short: pool.append(random.choice(short))
    if medium: pool.append(random.choice(medium))
    if longer: pool.append(random.choice(longer))
    rest = [t for t in unique if t not in pool]
    random.shuffle(rest)
    return (pool + rest)[:k]

def format_fewshot(prompt: str, shots: List[str]) -> str:
    return "Some Proverbs:\n- " + "\n- ".join(shots) + f"\n\n{prompt}\n- "

def trim_on_custom_stop(text: str, stop: str) -> str:
    i = text.find(stop)
    return text if i == -1 else text[:i]

def is_bad(gen: str) -> bool:
    g = gen.strip()
    if wc(g) < MIN_WORDS: return True
    if g in train_set: return True
    if len(g) >= 24 and g in train_blob: return True
    return False

def generate_once(p: str, model, tok) -> tuple[str, str, list[str]]:
    tries = 0
    while True:
        shots = pick_diverse_shots(N_SHOTS)
        fp = format_fewshot(p, shots)
        txt = mlx_generate(model=model, tokenizer=tok, prompt=fp, max_tokens=MAX_NEW)
        gen = txt[len(fp):] if txt.startswith(fp) else txt
        gen = gen.strip()
        if not is_bad(gen) or tries >= RETRIES:
            return fp, gen, shots
        tries += 1

all_rows = []
ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
df = pd.read_csv(EXPERIMENTS_CSV)

for i, row in df.iterrows():
    print("JIM",row)
    base = row.get("model_id", "").strip()
    adapter = row.get("adapter_path", "").strip()
    artifact_label = ""
    model_path = None
    adapter_path = None

    if adapter and Path(adapter).exists():
        model_path = base
        adapter_path = adapter
        artifact_label = "base+adapter"
    else:
        print(f"[WARN] Skipping row {i} — no valid model found")
        continue

    model, tok = mlx_load(model_path, adapter_path=adapter_path)
    TOKMETA.write_text(json.dumps({
        "eos_token": getattr(tok, "eos_token", None),
        "eos_token_id": getattr(tok, "eos_token_id", None),
        "pad_token": getattr(tok, "pad_token", None),
        "pad_token_id": getattr(tok, "pad_token_id", None),
    }, indent=2), encoding="utf-8")

    for p in PROMPTS:
        for mode in MODES:
            fp, gen, shots = generate_once(p, model, tok)
            if mode == "custom_stop":
                gen = trim_on_custom_stop(gen, CUSTOM_STOP).strip()
            all_rows.append({
                "timestamp": ts, "seed": SEED,
                "model_id": base, "artifact": artifact_label,
                "artifact_model_path": model_path,
                "adapter_path": adapter_path or "",
                "prompt_variant": "fewshot-dynamic", "mode": mode,
                "prompt": p, "input_text": fp,
                "output_text": gen, "generation": gen,
                "shots": shots, "max_new_tokens": MAX_NEW,
                "custom_stop": CUSTOM_STOP if mode == "custom_stop" else "",
            })
            print(f"[{mode}] {p} → {gen[:80]}...")

# Write JSONL
with JSONL_PATH.open("w", encoding="utf-8") as f:
    for r in all_rows:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")

# Write CSV
csv_cols = ["timestamp","seed","model_id","artifact","artifact_model_path","adapter_path",
            "prompt_variant","mode","prompt","generation","output_text","shots","max_new_tokens","custom_stop"]
with CSV_PATH.open("w", encoding="utf-8", newline="") as f:
    w = csv.DictWriter(f, fieldnames=csv_cols)
    w.writeheader()
    for r in all_rows:
        rr = r.copy(); rr["shots"] = " | ".join(r["shots"])
        w.writerow({k: rr.get(k, "") for k in csv_cols})

print(f"Rows written: {len(all_rows)} → {JSONL_PATH} and {CSV_PATH}")
