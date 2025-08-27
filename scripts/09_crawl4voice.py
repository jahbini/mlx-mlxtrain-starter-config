# pip install requests beautifulsoup4
from __future__ import annotations
import os, sys, requests, json, re, time, random, hashlib

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config
cfg = load_config()

from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urldefrag

BASE = cfg.web.base
START_URL = "https://" + BASE + "/"
USER_AGENT = cfg.web.user_agent
REQUEST_TIMEOUT = cfg.web.request_timeout
PAUSE_SEC = cfg.web.pause_sec
SEED = cfg.run.seed
VALID_FRACTION = cfg.web.valid_fraction

from pathlib import Path

OUT_DIR = Path(cfg.run.output_dir + "/" + cfg.data.output_dir )
CONTRACT_PATH = OUT_DIR / cfg.paths.contract
CATALOG_PATH  = OUT_DIR / cfg.paths.catalog
REPORT_PATH   = OUT_DIR / cfg.paths.report
TRAIN_JSONL   = OUT_DIR / "train.jsonl"
VALID_JSONL   = OUT_DIR / "valid.jsonl"

# ---- Continuation windowing (tweak these) ----
MIN_STORY_WORDS        = cfg.web.min_story_words         # skip tiny pages
MIN_PROMPT_WORDS       = cfg.web.min_prompt_words       # prompt lower bound
MAX_PROMPT_WORDS       = cfg.web.max_prompt_words       # prompt upper bound
MIN_COMPLETION_WORDS   = cfg.web.min_completion_words   # completion lower bound
MAX_COMPLETION_WORDS   = cfg.web.max_completion_words   # completion upper bound
MAX_EXAMPLES_PER_STORY = cfg.web.max_examples_per_story # cap examples per story

import re

def normalize_whitespace(s: str) -> str:
    if not isinstance(s, str):
        return s
    # turn any run of newlines (with optional surrounding spaces) into a single space
    s = re.sub(r"\s*\n\s*", " ", s)
    # collapse multiple spaces
    s = re.sub(r" {2,}", " ", s)
    return s.strip()

def get(url: str) -> str:
    r = requests.get(url, timeout=REQUEST_TIMEOUT, headers={"User-Agent": USER_AGENT})
    r.raise_for_status()
    return r.text

def is_local_html(href: str) -> bool:
    href, _ = urldefrag(href)
    if not href or not href.endswith(".html"):
        return False
    full = urljoin(BASE, href)
    u = urlparse(full)
    return (u.netloc == urlparse(BASE).netloc)

def discover_all_html(start_url: str):
    to_visit = [start_url]
    visited = set()
    pages = []
    while to_visit and len(pages) <5000:
        url = to_visit.pop(0)
        if url in visited: 
            continue
        visited.add(url)
        try:
            html = get(url)
        except Exception:
            print("fail",url)
            continue
        pages.append((url, html))
        soup = BeautifulSoup(html, "html.parser")
        for a in soup.find_all("a", href=True):
            if is_local_html(a["href"]):
                nxt = urljoin(START_URL, a["href"])
                if nxt not in visited and nxt not in to_visit:
                    to_visit.append(nxt)
        time.sleep(PAUSE_SEC)
    return pages

# --- Text cleanup: fix common mojibake from Windows-1252/UTF-8 mishaps
MOJIBAKE_MAP = {
    "\u00c2": "",  # Â
    "â": "’",
    "â": "“",
    "â": "”",
    "â": "–",
    "â": "—",
    "â¢": "•",
    "â¦": "…",
    "â": "‘",
    "â¨": " ",
    "âª": "",
    "â«": "",
    "â¬": "",
}
def demojibake(s: str) -> str:
    for k, v in MOJIBAKE_MAP.items():
        s = s.replace(k, v)
    # collapse excessive whitespace
    s = re.sub(r"[ \t]+\n", "\n", s)
    s = re.sub(r"\n{3,}", "\n\n", s)
    return s.strip()

def extract_story_text(html: str) -> tuple[str, str]:
    soup = BeautifulSoup(html, "html.parser")
    h2 = soup.find_all("h2")[1]
    title = h2.get_text(strip=True) if h2 else (soup.title.get_text(strip=True) if soup.title else "Untitled")
    div = soup.find(id="bloviation")
    
    if not div:
        return title, ""
    # Keep paragraphs and simple headings inside #bloviation
    text = div.get_text(strip=True, separator = "\n\n" )
    return title, demojibake(text)

def word_count(s: str) -> int:
    return len(re.findall(r"\w+", s))

def split_into_paragraphs(s: str):
    paras = [p.strip() for p in re.split(r"\n{2,}", s) if p.strip()]
    return paras

def clip_by_words(s: str, max_words: int) -> str:
    words = s.split()
    if len(words) <= max_words:
        return s
    return " ".join(words[:max_words])

def build_continuations(doc_id: str, title: str, text: str, url: str):
    """Yield multiple (prompt, completion) pairs from one story."""
    paras = split_into_paragraphs(text)
    if len(paras) < 2:
        return []

    # Greedy sliding window over paragraphs: take k paras as prompt, next m paras as completion
    # Keep within word budgets.
    exs = []
    i = 0
    while i < len(paras) - 1 and len(exs) < MAX_EXAMPLES_PER_STORY:
        # grow prompt until near MAX_PROMPT_WORDS
        prompt_parts, w = [], 0
        j = i
        while j < len(paras) - 1 and w < MAX_PROMPT_WORDS:
            w += word_count(paras[j])
            prompt_parts.append(paras[j])
            j += 1
            if w >= MIN_PROMPT_WORDS:  # acceptable prompt size
                break
        if not prompt_parts:
            break

        # completion = next paragraph(s)
        comp_parts, cw = [], 0
        k = j
        while k < len(paras) and cw < MIN_COMPLETION_WORDS:
            cw += word_count(paras[k])
            comp_parts.append(paras[k])
            k += 1
        if not comp_parts:
            break

        prompt_parts = normalize_whitespace(prompt_parts)
        comp_parts = normalize_whitespace(comp_parts)
        prompt = f"Title: {title}\n\n" + "\n\n".join(prompt_parts)
        completion = "\n\n".join(comp_parts)
        # hard caps to avoid very long sequences
        prompt = clip_by_words(prompt, MAX_PROMPT_WORDS + 40)
        completion = clip_by_words(completion, MAX_COMPLETION_WORDS)

        # Simple guard: ensure the completion doesn’t appear verbatim in prompt
        if completion and completion not in prompt:
            exs.append({
                "meta": {"doc_id": doc_id, "title": title, "url": url},
                "prompt": prompt,
                "completion": completion
            })

        # advance window: start later so pairs don’t overlap too much
        i = j  # move to just after the prompt block
    return exs

def main():
    print("Crawling site-local .html ", START_URL)
    pages = discover_all_html(START_URL)
    print(f"Fetched {len(pages)} pages; extracting #bloviation …")

    all_examples = []
    for url, html in pages:
        try:
            title, story = extract_story_text(html)
            if word_count(story) < MIN_STORY_WORDS:
                continue
            slug = urlparse(url).path.rsplit("/",1)[-1].replace(".html","")
            exs = build_continuations(slug, title, story, url)
            all_examples.extend(exs)
        except Exception:
            continue
    #print("EXAMPLES",all_examples)
    # Dedup by (doc_id, prompt) to be safe
    dedup = {}
    for ex in all_examples:
        key = (ex["meta"]["doc_id"], ex["prompt"][:2000])
        dedup[key] = ex
    examples = list(dedup.values())

    random.seed(SEED)
    random.shuffle(examples)

    n_valid = max(1, int(len(examples) * VALID_FRACTION))
    valid = examples[:n_valid]
    train = examples[n_valid:]

    with open(TRAIN_JSONL,"w",encoding="utf-8") as f:
        for ex in train:
            f.write(json.dumps(ex, ensure_ascii=False) + "\n")
    with open(VALID_JSONL,"w",encoding="utf-8") as f:
        for ex in valid:
            f.write(json.dumps(ex, ensure_ascii=False) + "\n")

    print(f"Wrote train.jsonl ({len(train)}) and valid.jsonl ({len(valid)}). Voice-continuation schema.")

if __name__ == "__main__":
    main()
    
# ==== AUX OUTPUTS FOOTER: contract + catalog (+ report) ======================


# --- infer schema from first valid example
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

# --- helpers
def _sha256_file(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()

def _count_lines_bytes(path: Path):
    # fast line count
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

# --- build contract
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
        "origin": "web_crawl",   # simple provenance tag; adjust if you like
    },
}

# --- build catalog (+ legacy 'entries' shim for older steps)
t_lines, t_bytes = _count_lines_bytes(Path(TRAIN_JSONL))
v_lines, v_bytes = _count_lines_bytes(Path(VALID_JSONL))

catalog = {
    "created_utc": created,
    "files": {
        "train": {"path": str(Path(TRAIN_JSONL).resolve()), "lines": t_lines, "bytes": t_bytes, "sha256": _sha256_file(Path(TRAIN_JSONL))},
        "valid": {"path": str(Path(VALID_JSONL).resolve()), "lines": v_lines, "bytes": v_bytes, "sha256": _sha256_file(Path(VALID_JSONL))},
    },
    # legacy fields used by some downstream scripts
    "entries": {
        "train": {"path": str(Path(TRAIN_JSONL).resolve()), "stats": {"num_valid_examples": t_lines, "num_bytes": t_bytes}},
        "valid": {"path": str(Path(VALID_JSONL).resolve()), "stats": {"num_valid_examples": v_lines, "num_bytes": v_bytes}},
    },
}

# --- optional compact report (length stats)
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

# --- write files
Path(OUT_DIR).mkdir(parents=True, exist_ok=True)
CONTRACT_PATH.write_text(json.dumps(contract, indent=2), encoding="utf-8")
CATALOG_PATH.write_text(json.dumps(catalog, indent=2), encoding="utf-8")
REPORT_PATH.write_text(json.dumps(report, indent=2), encoding="utf-8")

print("=== AUX FILES WRITTEN ===")
print(f"- {CONTRACT_PATH}")
print(f"- {CATALOG_PATH}")
print(f"- {REPORT_PATH}")
print(f"Schema: mode={mode} target_field={target_field}")

