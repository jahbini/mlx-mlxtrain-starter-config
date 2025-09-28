#!/usr/bin/env python3
# pip install markdown
import sys, os, re, json, random, hashlib, csv, time
from pathlib import Path
from urllib.parse import urlparse

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config
cfg = load_config()

# ---- CONTEXT: Adapted from crawl_for_voice.py to work with a local .md file instead of HTML crawl ----

SEED = cfg.run.seed
VALID_FRACTION = cfg.web.valid_fraction
MIN_STORY_WORDS = cfg.web.min_story_words

OUT_DIR = Path(cfg.data.output_dir ); OUT_DIR.mkdir(exist_ok=True)
CONTRACT_PATH = OUT_DIR / cfg.data.contract
CATALOG_PATH  = OUT_DIR / cfg.data.catalog
REPORT_PATH   = OUT_DIR / cfg.data.report
TRAIN_JSONL   = OUT_DIR / "train.jsonl"
VALID_JSONL   = OUT_DIR / "valid.jsonl"

# --- Text cleanup ---
def normalize_whitespace(s: str) -> str:
    if not isinstance(s, str):
        return s
    s = re.sub(r"\s*\n\s*", " ", s)
    s = re.sub(r" {2,}", " ", s)
    return s.strip()

def split_into_paragraphs(s: str):
    paras = [p.strip() for p in re.split(r"\n{2,}", s) if p.strip()]
    return paras

def ordinal_suffix(n: int):
    if 10 <= n % 100 <= 20:
        return "th"
    else:
        return {1: "st", 2: "nd", 3: "rd"}.get(n % 10, "th")

def extract_md_stories(md_path: Path):
    stories = []
    current_title = None
    current_body = []

    with md_path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.rstrip()
            if line.startswith("# "):  # new story
                print("JIM",line)
                if current_title and current_body:
                    full_text = "\n".join(current_body).strip()
                    print("JIM",full_text)
                    stories.append((current_title, full_text))
                current_title = line[2:].strip()
                current_body = []
            else:
                if current_title:
                    current_body.append(line)

        # last story
        if current_title and current_body:
            full_text = "\n".join(current_body).strip()
            stories.append((current_title, full_text))

    return stories

def main():
    if len(sys.argv) != 2:
        print("Usage: extract_md_for_voice.py <path/to/file.md>")
        sys.exit(1)

    md_path = Path(sys.argv[1])
    if not md_path.exists():
        print(f"Error: {md_path} does not exist")
        sys.exit(1)

    stories = extract_md_stories(md_path)
    all_examples = []

    for story_id, (title, text) in enumerate(stories):
        if len(text.split()) < MIN_STORY_WORDS:
            continue
        paragraphs = split_into_paragraphs(text)
        for i, para in enumerate(paragraphs):
            n = i + 1
            prompt = (
                #f"This is the {n}{ordinal_suffix(n)} paragraph from the story \"{title}\".\n\n"
                f"{para}\n\n"
                #f"Please summarize it, and note the wording and style of the paragraph."
            )
           # prompt = f"This is the {n}{ordinal_suffix(n)} paragraph from the story \"{title}\". Please summarize it, and note the wording and style of the paragraph."
            all_examples.append({
                "meta": {"doc_id": f"story-{story_id}", "title": title, "paragraph_index": n},
                "prompt": prompt,
                "completion": ""
            })

    random.seed(SEED)
    random.shuffle(all_examples)

    n_valid = max(1, int(len(all_examples) * VALID_FRACTION))
    valid = all_examples[:n_valid]
    train = all_examples[n_valid:]

    with open(TRAIN_JSONL,"w",encoding="utf-8") as f:
        for ex in train:
            f.write(json.dumps(ex, ensure_ascii=False) + "\n")
    with open(VALID_JSONL,"w",encoding="utf-8") as f:
        for ex in valid:
            f.write(json.dumps(ex, ensure_ascii=False) + "\n")

    print(f"Wrote train.jsonl ({len(train)}) and valid.jsonl ({len(valid)}). Summarization-per-paragraph schema.")

if __name__ == "__main__":
    main()

# ==== AUX OUTPUTS FOOTER: contract + catalog (+ report) ======================

def _first_valid(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for ln in f:
            ln = ln.strip()
            if not ln: 
                continue
            try:
                obj = json.loads(ln)
            except Exception:
                continue
            if isinstance(obj, dict):
                return obj
    return {}

probe = _first_valid(Path(TRAIN_JSONL))
mode = None
target_field = None
schema_fields = None

if isinstance(probe, dict) and ("prompt" in probe and "completion" in probe):
    mode = "sft"
    target_field = "completion"
    schema_fields = {"prompt": "string", "completion": "string"}
elif isinstance(probe, dict) and ("text" in probe):
    mode = "plain"
    target_field = "text"
    schema_fields = {"text": "string"}
else:
    print("ERROR: Could not infer dataset schema. Expected either {'text': ...} or {'prompt': ..., 'completion': ...}.", file=sys.stderr)
    sys.exit(2)

def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _count_lines_bytes(path: Path):
    n = 0
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            n += chunk.count(b"\n")
    return n, path.stat().st_size

def _summarize_lengths(path: Path, field: str):
    lens = []
    with path.open("r", encoding="utf-8") as f:
        for ln in f:
            try:
                obj = json.loads(ln)
                s = obj.get(field, "")
                if isinstance(s, str):
                    lens.append(len(s))
            except Exception:
                pass
    if not lens:
        return {"n": 0}
    lens.sort()
    n = len(lens)
    p95 = lens[int(0.95*(n-1))] if n > 1 else lens[-1]
    return {"n": n, "len_min": lens[0], "len_med": lens[n//2], "len_95": p95, "len_max": lens[-1]}

created = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

contract = {
    "created_utc": created,
    "data_dir": str(Path(OUT_DIR).resolve()),
    "filenames": {
        "train": {"chosen": Path(TRAIN_JSONL).name, "resolved": str(Path(TRAIN_JSONL).resolve())},
        "valid": {"chosen": Path(VALID_JSONL).name, "resolved": str(Path(VALID_JSONL).resolve())},
    },
    "schema": {"format": "jsonl", "fields": schema_fields},
    "source": {
        "mode": mode,
        "target_field": target_field,
        "origin": "markdown_file",   # provenance tag
    },
}

t_lines, t_bytes = _count_lines_bytes(Path(TRAIN_JSONL))
v_lines, v_bytes = _count_lines_bytes(Path(VALID_JSONL))

catalog = {
    "created_utc": created,
    "files": {
        "train": {"path": str(Path(TRAIN_JSONL).resolve()), "lines": t_lines, "bytes": t_bytes, "sha256": _sha256_file(Path(TRAIN_JSONL))},
        "valid": {"path": str(Path(VALID_JSONL).resolve()), "lines": v_lines, "bytes": v_bytes, "sha256": _sha256_file(Path(VALID_JSONL))},
    },
    "entries": {
        "train": {"path": str(Path(TRAIN_JSONL).resolve()), "stats": {"num_valid_examples": t_lines, "num_bytes": t_bytes}},
        "valid": {"path": str(Path(VALID_JSONL).resolve()), "stats": {"num_valid_examples": v_lines, "num_bytes": v_bytes}},
    },
}

train_stats = _summarize_lengths(Path(TRAIN_JSONL), target_field)
valid_stats = _summarize_lengths(Path(VALID_JSONL), target_field)
report = {
    "created_utc": created,
    "counts": {"train": t_lines, "valid": v_lines},
    "train_stats": train_stats,
    "valid_stats": valid_stats,
    "target_field": target_field,
    "schema_mode": mode,
}

Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
CONTRACT_PATH.write_text(json.dumps(contract, indent=2), encoding="utf-8")
CATALOG_PATH.write_text(json.dumps(catalog, indent=2), encoding="utf-8")
REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

print("=== AUX FILES WRITTEN ===")
print(f"- {CONTRACT_PATH}")
print(f"- {CATALOG_PATH}")
print(f"- {REPORT_PATH}")
print(f"Schema: mode={mode} target_field={target_field}")
