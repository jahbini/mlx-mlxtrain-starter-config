# scripts/02_prepare_data.py
from __future__ import annotations
import sys, os, json, re, unicodedata, statistics, hashlib
from pathlib import Path
from typing import Dict, Any, List, Tuple

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config

# --- STEP-AWARE CONFIG ---
CFG = load_config()
STEP_NAME = os.environ["STEP_NAME"]
STEP_CFG  = CFG.pipeline.steps[STEP_NAME]
PARAMS    = getattr(STEP_CFG, "params", {})

OUT_DIR = Path(getattr(PARAMS, "output_dir", CFG.data.output_dir)); OUT_DIR.mkdir(exist_ok=True)
CONTRACT = OUT_DIR / getattr(PARAMS, "contract", CFG.data.contract)
REPORT   = OUT_DIR / getattr(PARAMS, "report", CFG.data.report)

# Heuristics: potential stop/EOS markers to scan for
EOS_MARKERS = [
    "</s>",         # common HF eos
    "###",          # section break in some templates
    "\n\n",         # blank-line stop
    "<|eot_id|>",   # chat-style separators
    "<|endoftext|>" # GPT-like
]

def load_contract(path: Path) -> Tuple[str, Dict[str, str], str]:
    c = json.loads(path.read_text(encoding="utf-8"))
    data_dir = c["data_dir"]
    # discover the text field (first string-type field in schema)
    fields = c.get("schema", {}).get("fields", {})
    text_field = None
    for k, v in fields.items():
        if str(v).lower() == "string":
            text_field = k; break
    if not text_field:
        text_field = "text"  # fallback
    files = {split: info["resolved"] for split, info in c["filenames"].items() if info.get("resolved")}
    return text_field, files, data_dir

def hash_text(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8", "ignore")).hexdigest()

def char_classes(s: str) -> Dict[str, int]:
    ctrl = sum(1 for ch in s if unicodedata.category(ch) in ("Cc","Cf"))
    ws   = sum(1 for ch in s if ch.isspace())
    nonascii = sum(1 for ch in s if ord(ch) > 127)
    return {"control": ctrl, "whitespace": ws, "non_ascii": nonascii}

def percentiles(values: List[int], q=(5, 25, 50, 75, 95)) -> Dict[str, int]:
    if not values: return {f"p{p}": 0 for p in q}
    vals = sorted(values)
    out = {}
    for p in q:
        k = max(0, min(len(vals)-1, int(round((p/100)* (len(vals)-1)))))
        out[f"p{p}"] = int(vals[k])
    return out

def scan_file(path: Path, field: str) -> Dict[str, Any]:
    n_lines = bad_json = missing_field = non_str = 0
    empty = whitespace_only = leading_ws = trailing_ws = ctrl_lines = 0
    lengths, hashes = [], []
    eos_hits = {m: 0 for m in EOS_MARKERS}
    samples_good: List[str] = []
    samples_bad: List[str]  = []

    with path.open("r", encoding="utf-8", errors="replace") as f:
        for line in f:
            n_lines += 1
            line = line.rstrip("\n")
            try:
                obj = json.loads(line)
            except Exception:
                bad_json += 1
                if len(samples_bad) < 3: samples_bad.append(f"[bad_json] {line[:160]}")
                continue

            if field not in obj:
                missing_field += 1
                if len(samples_bad) < 3: samples_bad.append(f"[missing_field] {line[:160]}")
                continue
            val = obj[field]
            if not isinstance(val, str):
                non_str += 1
                if len(samples_bad) < 3: samples_bad.append(f"[non_string] {str(val)[:160]}")
                continue

            if val == "":
                empty += 1
            if val.strip() == "":
                whitespace_only += 1
            if val and val[0].isspace():
                leading_ws += 1
            if val and val[-1].isspace():
                trailing_ws += 1

            cc = char_classes(val)
            if cc["control"] > 0:
                ctrl_lines += 1

            L = len(val)
            lengths.append(L)
            hashes.append(hash_text(val))
            for m in EOS_MARKERS:
                if m in val:
                    eos_hits[m] += 1

            if len(samples_good) < 3:
                samples_good.append(val)

    # duplicates
    from collections import Counter
    dup_count, dup_examples = 0, []
    for h, cnt in Counter(hashes).items():
        if cnt > 1:
            dup_count += cnt - 1
            if len(dup_examples) < 3:
                dup_examples.append(h)

    # length stats
    length_stats = {
        "count": len(lengths),
        "min": int(min(lengths)) if lengths else 0,
        "max": int(max(lengths)) if lengths else 0,
        "mean": float(statistics.mean(lengths)) if lengths else 0.0,
        "median": float(statistics.median(lengths)) if lengths else 0.0,
        "percentiles": percentiles(lengths),
    }

    return {
        "path": str(path),
        "lines": n_lines,
        "valid_examples": len(lengths),
        "errors": {
            "bad_json": bad_json,
            "missing_field": missing_field,
            "non_string_field": non_str,
        },
        "empties": {
            "empty_exact": empty,
            "whitespace_only": whitespace_only,
            "leading_whitespace": leading_ws,
            "trailing_whitespace": trailing_ws,
        },
        "control_char_lines": ctrl_lines,
        "duplicates": {
            "duplicate_example_count": dup_count,
            "sha256_examples": dup_examples,
        },
        "length_chars": length_stats,
        "eos_markers_hits": eos_hits,
        "samples": {
            "good_first3": samples_good,
            "bad_first3": samples_bad,
        },
    }

# --- MAIN EXECUTION ---
text_field, files, data_dir = load_contract(CONTRACT)
report: Dict[str, Any] = {
    "created_utc": __import__("time").strftime("%Y-%m-%dT%H:%M:%SZ", __import__("time").gmtime()),
    "data_dir": data_dir,
    "text_field": text_field,
    "splits": {},
}

for split, p in files.items():
    rep = scan_file(Path(p), text_field)
    report["splits"][split] = rep

REPORT.write_text(json.dumps(report, indent=2), encoding="utf-8")

# Console summary
print("=== DATA VALIDATION SUMMARY ===")
for split, rep in report["splits"].items():
    errs = rep["errors"]; empt = rep["empties"]; lens = rep["length_chars"]
    eos = rep["eos_markers_hits"]; dup = rep["duplicates"]["duplicate_example_count"]
    print(f"- {split}: lines={rep['lines']} valid={rep['valid_examples']} "
          f"errors(bad/miss/nonstr)={errs['bad_json']}/{errs['missing_field']}/{errs['non_string_field']} "
          f"empties(exact/ws/lead/trail)={empt['empty_exact']}/{empt['whitespace_only']}/{empt['leading_whitespace']}/{empt['trailing_whitespace']} "
          f"dupes={dup} len[min/med/95/max]={lens['min']}/{int(lens['median'])}/{lens['percentiles']['p95']}/{lens['max']} "
          f"eos_hits={{" + ", ".join(f'{k}:{v}' for k,v in eos.items() if v) + "}}")
print("Wrote:", REPORT)
