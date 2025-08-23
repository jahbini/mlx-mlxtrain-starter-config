#!/usr/bin/env python3
"""
build_catalog_contract.py
Create data_contract.json and data_catalog.json for the current directory.

- Inputs (in current dir):  train.jsonl, valid.jsonl
- Outputs (in current dir): data_contract.json, data_catalog.json

Usage:
  python build_catalog_contract.py
  python build_catalog_contract.py --force   # overwrite existing outputs
  python build_catalog_contract.py --train other_train.jsonl --valid other_valid.jsonl
"""

from __future__ import annotations
import argparse, hashlib, json, time
from pathlib import Path
from typing import Dict, Tuple

def count_lines_bytes_sha(p: Path) -> Tuple[int, int, str]:
    """Fast count of newline-terminated records, byte size, and sha256."""
    n = 0
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            n += chunk.count(b"\n")
            h.update(chunk)
    return n, p.stat().st_size, h.hexdigest()

def sniff_schema(jsonl: Path) -> Dict:
    """
    Return {"format":"jsonl","fields":{<key>:"string"}}.
    Priority for text field: 'text' → 'completion' → common alternates → first non-empty string.
    """
    preferred = ("text", "completion", "output", "response", "content", "message", "answer")
    try:
        with jsonl.open("r", encoding="utf-8") as f:
            for _ in range(200):  # sample up to 200 lines
                line = f.readline()
                if not line:
                    break
                line = line.strip()
                if not line:
                    continue
                try:
                    obj = json.loads(line)
                except Exception:
                    continue
                # strict priorities first
                for k in preferred:
                    v = obj.get(k)
                    if isinstance(v, str) and v.strip():
                        return {"format": "jsonl", "fields": {k: "string"}}
                # fallback: first non-empty string field
                for k, v in obj.items():
                    if isinstance(v, str) and v.strip():
                        return {"format": "jsonl", "fields": {k: "string"}}
    except FileNotFoundError:
        pass
    # final fallback
    return {"format": "jsonl", "fields": {"text": "string"}}

def main():
    ap = argparse.ArgumentParser(description="Build data_contract.json and data_catalog.json for local JSONL files.")
    ap.add_argument("--train", default="train.jsonl", help="Train JSONL filename (default: train.jsonl)")
    ap.add_argument("--valid", default="valid.jsonl", help="Valid JSONL filename (default: valid.jsonl)")
    ap.add_argument("--contract", default="data_contract.json", help="Output contract filename")
    ap.add_argument("--catalog",  default="data_catalog.json",  help="Output catalog filename")
    ap.add_argument("--force", action="store_true", help="Overwrite existing outputs")
    args = ap.parse_args()

    train = Path(args.train).resolve()
    valid = Path(args.valid).resolve()
    if not train.exists() or not valid.exists():
        raise SystemExit(f"Expected {train.name} and {valid.name} in {train.parent}.")

    created = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # Detect schema from TRAIN (VALID assumed same)
    schema = sniff_schema(train)

    # Build contract
    contract = {
        "created_utc": created,
        "data_dir": str(train.parent),
        "filenames": {
            "train": {"chosen": train.name, "resolved": str(train)},
            "valid": {"chosen": valid.name, "resolved": str(valid)},
        },
        "schema": schema,
    }

    # Build catalog (both simple and legacy views)
    t_lines, t_bytes, t_sha = count_lines_bytes_sha(train)
    v_lines, v_bytes, v_sha = count_lines_bytes_sha(valid)
    catalog = {
        "created_utc": created,
        "files": {
            "train": {"path": str(train), "lines": t_lines, "bytes": t_bytes, "sha256": t_sha},
            "valid": {"path": str(valid), "lines": v_lines, "bytes": v_bytes, "sha256": v_sha},
        },
        "entries": {
            "train": {"path": str(train), "stats": {
                "num_valid_examples": t_lines, "num_bytes": t_bytes, "sha256": t_sha}},
            "valid": {"path": str(valid), "stats": {
                "num_valid_examples": v_lines, "num_bytes": v_bytes, "sha256": v_sha}},
        },
    }

    contract_path = Path(args.contract)
    catalog_path  = Path(args.catalog)

    if contract_path.exists() and not args.force:
        print(f"[skip] {contract_path} exists (use --force to overwrite)")
    else:
        contract_path.write_text(json.dumps(contract, indent=2), encoding="utf-8")
        print(f"[ok] wrote {contract_path}")

    if catalog_path.exists() and not args.force:
        print(f"[skip] {catalog_path} exists (use --force to overwrite)")
    else:
        catalog_path.write_text(json.dumps(catalog, indent=2), encoding="utf-8")
        print(f"[ok] wrote {catalog_path}")

    print(f"[info] train: lines={t_lines} bytes={t_bytes:,} sha={t_sha[:12]}…")
    print(f"[info] valid: lines={v_lines} bytes={v_bytes:,} sha={v_sha[:12]}…")
    print("[done] data_contract.json and data_catalog.json are ready.")

if __name__ == "__main__":
    main()
