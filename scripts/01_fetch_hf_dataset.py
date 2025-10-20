# scripts/01_fetch_hf_dataset.py
from pathlib import Path
import sys, os, json, random, hashlib, time
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config
from datasets import load_dataset

# --- STEP-AWARE CONFIG ---
CFG = load_config()
STEP_NAME = os.environ["STEP_NAME"]
STEP_CFG = CFG[STEP_NAME]
PARAMS = getattr(STEP_CFG, "params", {})

# Resolve values (step.params > global.cfg)
HF_DATASET  = STEP_CFG.hf_dataset
SUBSET      = STEP_CFG.subset
MODE        = STEP_CFG.mode
VALID_FRACT = STEP_CFG.valid_fract
MIN_WORDS   = STEP_CFG.min_words
MAX_WORDS   = STEP_CFG.max_words
SEED        = STEP_CFG.seed

DATA_DIR = Path(CFG.run.data_dir); DATA_DIR.mkdir(exist_ok=True)
CONTRACT = DATA_DIR / CFG.run.contract
CATALOG  = DATA_DIR / CFG.run.catalog

print("Dataset:", HF_DATASET)
print("Subset:", SUBSET)
print("Mode:", MODE)
print("Valid fraction:", VALID_FRACT)
print("Seed:", SEED)

random.seed(SEED)

def main():
    print(f"Loading {HF_DATASET} subset={SUBSET} …")
    ds = load_dataset(HF_DATASET, name=SUBSET, split="train")
    print(ds)

    def wc(s): return len(str(s).split())
    def sha(s): return hashlib.sha256(str(s).encode("utf-8","ignore")).hexdigest()

    rows = []
    for r in ds:
        quote  = (r.get("quote") or "").strip()
        author = (r.get("author") or "").strip()
        if not quote:
            continue

        if MODE == "plain":
            text = quote
        else:
            instr = f"Write a short motivational quote in the style of {author}." if author else "Write a short motivational quote."
            text  = f"Instruction:\n{instr}\n\nResponse:\n{quote}"

        if not (MIN_WORDS <= wc(text) <= MAX_WORDS):
            continue
        rows.append(text)

    # dedupe while preserving order
    seen=set(); uniq=[]
    for t in rows:
        h=sha(t)
        if h not in seen:
            seen.add(h); uniq.append(t)

    # split
    random.shuffle(uniq)
    valid_n = max(100, int(len(uniq) * VALID_FRACT))
    valid = uniq[:valid_n]
    train = uniq[valid_n:]

    def write_jsonl(path: Path, texts):
        with path.open("w", encoding="utf-8") as f:
            for t in texts:
                f.write(json.dumps({"text": t}, ensure_ascii=False) + "\n")

    train_path = DATA_DIR / "train.jsonl"
    valid_path = DATA_DIR / "valid.jsonl"
    write_jsonl(train_path, train)
    write_jsonl(valid_path, valid)

    print(f"Wrote {len(train)} train, {len(valid)} valid to {DATA_DIR.resolve()}")

    # --- Write data_contract.json and data_catalog.json ---
    def count_lines_bytes(p: Path):
        n = 0
        with p.open("rb") as f:
            for _ in f: n += 1
        return n, p.stat().st_size

    def sha256_file(p: Path) -> str:
        h = hashlib.sha256()
        with p.open("rb") as f:
            for chunk in iter(lambda: f.read(1024*1024), b""):
                h.update(chunk)
        return h.hexdigest()

    created = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    data_contract = {
        "created_utc": created,
        "data_dir": str(DATA_DIR.resolve()),
        "filenames": {
            "train": {"chosen": train_path.name, "resolved": str(train_path.resolve())},
            "valid": {"chosen": valid_path.name, "resolved": str(valid_path.resolve())},
        },
        "schema": {"format": "jsonl", "fields": {"text": "string"}},
    }
    CONTRACT.write_text(json.dumps(data_contract, indent=2), encoding="utf-8")

    t_lines, t_bytes = count_lines_bytes(train_path)
    v_lines, v_bytes = count_lines_bytes(valid_path)
    t_sha = sha256_file(train_path)
    v_sha = sha256_file(valid_path)

    data_catalog = {
        "created_utc": created,
        "files": {
            "train": {"path": str(train_path.resolve()), "lines": t_lines, "bytes": t_bytes, "sha256": t_sha},
            "valid": {"path": str(valid_path.resolve()), "lines": v_lines, "bytes": v_bytes, "sha256": v_sha},
        },
        "entries": {
            "train": {"path": str(train_path.resolve()), "stats": {
                "num_valid_examples": t_lines, "num_bytes": t_bytes, "sha256": t_sha}},
            "valid": {"path": str(valid_path.resolve()), "stats": {
                "num_valid_examples": v_lines, "num_bytes": v_bytes, "sha256": v_sha}},
        },
    }
    CATALOG.write_text(json.dumps(data_catalog, indent=2), encoding="utf-8")

    print("Wrote data_contract.json and data_catalog.json")

if __name__ == "__main__":
    main()
