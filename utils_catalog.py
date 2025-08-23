# === Catalog/Contract builder (drop-in) ======================================
# Add to crawl4voice.py (after your JSONL writing), or import as a helper.

from __future__ import annotations
import json, hashlib, time, argparse
from pathlib import Path
from typing import Dict, Tuple

def _count_lines_bytes_sha(p: Path) -> Tuple[int, int, str]:
    n = 0
    h = hashlib.sha256()
    with p.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            n += chunk.count(b"\n")
            h.update(chunk)
    return n, p.stat().st_size, h.hexdigest()

def _sniff_schema(jsonl: Path) -> Dict:
    """
    Return {"format":"jsonl","fields":{<key>:"string"}}.
    Priority: 'text' → 'completion' → common alt keys → first string field.
    """
    preferred_order = ("text", "completion", "output", "response", "content", "message", "answer")
    with jsonl.open("r", encoding="utf-8") as f:
        for _ in range(200):
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
            for k in preferred_order:
                v = obj.get(k)
                if isinstance(v, str) and v.strip():
                    return {"format": "jsonl", "fields": {k: "string"}}
            # fallback: first non-empty string field
            for k, v in obj.items():
                if isinstance(v, str) and v.strip():
                    return {"format": "jsonl", "fields": {k: "string"}}
    return {"format": "jsonl", "fields": {"text": "string"}}

def finalize_data_dir(
    data_dir: Path | str = "data",
    train_name: str = "train.jsonl",
    valid_name: str = "valid.jsonl",
    out_contract: str = "data_contract.json",
    out_catalog: str = "data_catalog.json",
    force: bool = False,
) -> None:
    """
    Build data_contract.json and data_catalog.json for the given JSONL files.
    Writes BOTH the 'files' (simple) and 'entries' (legacy) views.
    """
    data_dir = Path(data_dir)
    train = data_dir / train_name
    valid = data_dir / valid_name
    if not train.exists() or not valid.exists():
        raise FileNotFoundError(f"Expected {train} and {valid} to exist")

    created = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())

    # Detect schema from TRAIN (valid assumed same schema)
    schema = _sniff_schema(train)

    # Contract
    contract = {
        "created_utc": created,
        "data_dir": str(data_dir.resolve()),
        "filenames": {
            "train": {"chosen": train.name, "resolved": str(train.resolve())},
            "valid": {"chosen": valid.name, "resolved": str(valid.resolve())},
        },
        "schema": schema,
    }

    # Catalog
    t_lines, t_bytes, t_sha = _count_lines_bytes_sha(train)
    v_lines, v_bytes, v_sha = _count_lines_bytes_sha(valid)
    catalog = {
        "created_utc": created,
        "files": {
            "train": {"path": str(train.resolve()), "lines": t_lines, "bytes": t_bytes, "sha256": t_sha},
            "valid": {"path": str(valid.resolve()), "lines": v_lines, "bytes": v_bytes, "sha256": v_sha},
        },
        # Legacy view consumed by older Step 6:
        "entries": {
            "train": {"path": str(train.resolve()), "stats": {
                "num_valid_examples": t_lines, "num_bytes": t_bytes, "sha256": t_sha}},
            "valid": {"path": str(valid.resolve()), "stats": {
                "num_valid_examples": v_lines, "num_bytes": v_bytes, "sha256": v_sha}},
        },
    }

    # Write (safe unless force=True)
    contract_path = Path(out_contract)
    catalog_path  = Path(out_catalog)
    if contract_path.exists() and not force:
        print(f"[finalize] {contract_path} exists; use force=True to overwrite")
    else:
        contract_path.write_text(json.dumps(contract, indent=2), encoding="utf-8")
        print(f"[finalize] wrote {contract_path}")

    if catalog_path.exists() and not force:
        print(f"[finalize] {catalog_path} exists; use force=True to overwrite")
    else:
        catalog_path.write_text(json.dumps(catalog, indent=2), encoding="utf-8")
        print(f"[finalize] wrote {catalog_path}")

    print(f"[finalize] train: lines={t_lines} bytes={t_bytes:,} sha={t_sha[:12]}…")
    print(f"[finalize] valid: lines={v_lines} bytes={v_bytes:,} sha={v_sha[:12]}…")

# --- Optional: CLI integration (add to your existing argparse) ----------------
if __name__ == "__main__":
    parser = argparse.ArgumentParser(add_help=True)
    parser.add_argument("--finalize-data", action="store_true",
                        help="Write data_contract.json and data_catalog.json for the data dir")
    parser.add_argument("--data-dir", default="data", help="Directory with train.jsonl and valid.jsonl")
    parser.add_argument("--force", action="store_true", help="Overwrite existing contract/catalog")
    args, _unknown = parser.parse_known_args()

    # … your crawler logic here …
    # after you've written data/train.jsonl and data/valid.jsonl:

    if args.finalize_data:
        finalize_data_dir(args.data_dir, force=args.force)
