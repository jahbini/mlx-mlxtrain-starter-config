from __future__ import annotations
from pathlib import Path
import sys, os, json, random, hashlib, csv, time
from datasets import load_from_disk
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config
cfg = load_config()
# STEP 10 — Dynamic few-shot + anti-copy retry → WRITE eval_out/generations.{jsonl,csv}
# Adds fields: mode, generation (alias of output_text)
from typing import List, Optional
from mlx_lm import load as mlx_load, generate as mlx_generate

# --- Config ---
SNAPSHOT      = Path(cfg.snapshot.output_dir)
BASE = cfg.snapshot.base
ADAPTER   = cfg.run.output_dir + "/" + "microsoft--Phi-3-mini-4k-instruct" +  "/adapter"
FUSED     = SNAPSHOT / cfg.snapshot.fused         # optional
QUANT     = SNAPSHOT / cfg.snapshot.quant          # optional
MAX_NEW   = cfg.snapshot.max_new
SEED      = cfg.snapshot.seed
N_SHOTS   = cfg.snapshot.n_shots
MIN_WORDS = cfg.snapshot.min_words
RETRIES   = cfg.snapshot.retries
OUT_DIR       = Path(cfg.run.output_dir + "/" + cfg.data.output_dir); OUT_DIR.mkdir(exist_ok=True)
RUN_DIR       = Path(cfg.run.output_dir)  # where per-model outputs will go
EVAL_DIR      = Path(cfg.eval.output_dir); EVAL_DIR.mkdir(exist_ok=True)
EXPERIMENTS   = RUN_DIR / cfg.run.experiments
ARTIFACTS     = RUN_DIR / cfg.run.artifacts
CONTRACT      = OUT_DIR / cfg.paths.contract

print("JIM1",ADAPTER)

SEED  = cfg.run.alt_seed

MODES = ["default_eos", "no_eos", "custom_stop"]
CUSTOM_STOP = "\n\n"  # client-side trim for 'custom_stop'

PROMPTS = cfg.snapshot.prompts
#[
#    "Share a saying about time.",
#    "Offer a short proverb on patience.",
#    "Give a hopeful saying for travelers.",
#]

JSONL_PATH = EVAL_DIR / (cfg.paths.generations+".jsonl")
CSV_PATH   = EVAL_DIR / (cfg.paths.generations+".csv")
TOKMETA    = OUT_DIR / (cfg.paths.tokenizer+".json")

os.environ.setdefault("TOKENIZERS_PARALLELISM", "false")
random.seed(SEED)

# --- Load corpus for few-shot + anti-copy ---
contract = json.loads(CONTRACT.read_text(encoding="utf-8"))
text_field = next((k for k, v in contract["schema"]["fields"].items() if str(v).lower()=="string"), "text")
train_path = Path(contract["filenames"]["train"]["resolved"])

def sha(s: str) -> str: return hashlib.sha256(s.encode("utf-8","ignore")).hexdigest()
def wc(s: str) -> int:  return len(s.split())

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
short  = [t for t in unique if wc(t) <= 4]
medium = [t for t in unique if 5 <= wc(t) <= 12]
longer = [t for t in unique if wc(t) > 12]

def pick_diverse_shots(k: int) -> List[str]:
    pool = []
    if short:  pool.append(random.choice(short))
    if medium: pool.append(random.choice(medium))
    if longer: pool.append(random.choice(longer))
    rest = [t for t in unique if t not in pool]
    random.shuffle(rest)
    return (pool + rest)[:k]

train_blob = "\n\n".join(unique)
train_set  = set(unique)

def format_fewshot(prompt: str, shots: List[str]) -> str:
    return "Some Proverbs:\n- " + "\n- ".join(shots) + f"\n\n{prompt}\n- "

def trim_on_custom_stop(text: str, stop: str) -> str:
    i = text.find(stop)
    return text if i == -1 else text[:i]

def is_bad(gen: str) -> bool:
    g = gen.strip()
    if wc(g) < MIN_WORDS: return True
    if g in train_set:    return True
    if len(g) >= 24 and g in train_blob: return True
    return False

print("JIM",BASE,ADAPTER)
# choose artifact: quantized > fused > adapter
artifact_label: str
model_path: str
adapter_path: Optional[str] = None
if Path(QUANT).exists():
    artifact_label = "quantized"; model_path, adapter_path = QUANT, None
elif Path(FUSED).exists():
    artifact_label = "fused";     model_path, adapter_path = FUSED, None
else:
    artifact_label = "base+adapter"; model_path, adapter_path = BASE, ADAPTER

print("JIM",adapter_path, model_path)
model, tok = mlx_load(model_path, adapter_path=adapter_path)

# Tokenizer meta (optional but helpful)
TOKMETA.write_text(json.dumps({
    "eos_token": getattr(tok, "eos_token", None),
    "eos_token_id": getattr(tok, "eos_token_id", None),
    "pad_token": getattr(tok, "pad_token", None),
    "pad_token_id": getattr(tok, "pad_token_id", None),
}, indent=2), encoding="utf-8")

def generate_once(p: str) -> tuple[str, str, list[str]]:
    """Return (full_prompt, generation, shots) with retry for short/copy."""
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

# Collect rows
ts = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
rows = []

for p in PROMPTS:
    # default_eos
    fp, gen, shots = generate_once(p)
    rows.append({
        "timestamp": ts, "seed": SEED,
        "model_id": BASE, "artifact": artifact_label,
        "artifact_model_path": model_path, "adapter_path": adapter_path or "",
        "prompt_variant": "fewshot-dynamic", "mode": "default_eos",
        "prompt": p, "input_text": fp,
        "output_text": gen, "generation": gen,   # <— add 'generation'
        "shots": shots, "max_new_tokens": MAX_NEW,
    })
    print(f"\n[default_eos] {p}\n→ {gen}")

    # no_eos (same call; labeled separately for grouping)
    fp, gen, shots = generate_once(p)
    rows.append({
        "timestamp": ts, "seed": SEED,
        "model_id": BASE, "artifact": artifact_label,
        "artifact_model_path": model_path, "adapter_path": adapter_path or "",
        "prompt_variant": "fewshot-dynamic", "mode": "no_eos",
        "prompt": p, "input_text": fp,
        "output_text": gen, "generation": gen,   # <— add 'generation'
        "shots": shots, "max_new_tokens": MAX_NEW,
    })
    print(f"\n[no_eos] {p}\n→ {gen}")

    # custom_stop (client-side trim)
    fp, gen, shots = generate_once(p)
    gen_trim = trim_on_custom_stop(gen, CUSTOM_STOP).strip()
    rows.append({
        "timestamp": ts, "seed": SEED,
        "model_id": BASE, "artifact": artifact_label,
        "artifact_model_path": model_path, "adapter_path": adapter_path or "",
        "prompt_variant": "fewshot-dynamic", "mode": "custom_stop",
        "prompt": p, "input_text": fp,
        "output_text": gen_trim, "generation": gen_trim,  # <— add 'generation'
        "shots": shots, "max_new_tokens": MAX_NEW,
        "custom_stop": CUSTOM_STOP,
    })
    print(f"\n[custom_stop] {p}\n→ {gen_trim}")

# Write JSONL
with JSONL_PATH.open("w", encoding="utf-8") as f:
    for r in rows:
        f.write(json.dumps(r, ensure_ascii=False) + "\n")

# Write CSV
csv_cols = ["timestamp","seed","model_id","artifact","artifact_model_path","adapter_path",
            "prompt_variant","mode","prompt","generation","output_text","shots","max_new_tokens"]
with CSV_PATH.open("w", encoding="utf-8", newline="") as f:
    w = csv.DictWriter(f, fieldnames=csv_cols)
    w.writeheader()
    for r in rows:
        rr = r.copy(); rr["shots"] = " | ".join(r["shots"])
        w.writerow({k: rr.get(k, "") for k in csv_cols})

print(f"\n=== GENERATION SUMMARY ===")
print(f"Models evaluated: {BASE}")
print(f"Rows: {len(rows)}  |  JSONL: {JSONL_PATH}  |  CSV: {CSV_PATH}")
print(f"Modes: {MODES}")
print(f"Artifacts: ['{artifact_label}']")
