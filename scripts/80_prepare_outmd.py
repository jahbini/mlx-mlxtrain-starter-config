# data/prepare_outmd.py
"""
STEP — Prepare Markdown Stories for Instruction Tuning
Converts your.md into Alpaca-style JSONL for fine-tuning.

Inputs:
    your.md (markdown with story sections, # headers separate stories)
Outputs:
    run/data/out_instruct.jsonl
"""

from __future__ import annotations
import os, sys, json
from pathlib import Path

# --- Config loader (optional, keeps it pipeline-ready) ---
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
try:
    from config_loader import load_config
    CFG = load_config()
    DATA_DIR = Path(CFG.data.output_dir)
    INPUT_MD = Path(CFG.data.stories) if hasattr(CFG.data, "stories") else Path("your/your.md")
    OUTPUT_JSONL = DATA_DIR / "out_instruct.jsonl"
except Exception:
    # fallback for standalone runs
    INPUT_MD = Path("../your/your.md")
    OUTPUT_JSONL = Path("./run/data/out_instruct.jsonl")
    OUTPUT_JSONL.parent.mkdir(parents=True, exist_ok=True)

PROMPT_TEMPLATE = (
    "You are St. John's Jim, a myth-weaving, bar-stool Buddha of the Pacific Northwest.\n"
    "Tell a new short story in your usual voice. Base it on this seed:\n"
)

# --- Helpers ---
def extract_snippets(md_text: str):
    """Naive chunking: split by headers starting with '# '"""
    return [chunk.strip() for chunk in md_text.split("# ") if chunk.strip()]

def format_as_alpaca(chunk: str):
    return {
        "instruction": PROMPT_TEMPLATE + chunk[:200] + "...",
        "input": "",
        "output": chunk.strip(),
    }

# --- Main ---
def main():
    if not INPUT_MD.exists():
        raise FileNotFoundError(f"Missing input markdown: {INPUT_MD}")

    md_text = INPUT_MD.read_text(encoding="utf-8")
    chunks = extract_snippets(md_text)
    entries = [format_as_alpaca(c) for c in chunks]

    with OUTPUT_JSONL.open("w", encoding="utf-8") as f:
        for entry in entries:
            f.write(json.dumps(entry, ensure_ascii=False) + "\n")

    print(f"[OK] Wrote {len(entries)} entries to {OUTPUT_JSONL}")

if __name__ == "__main__":
    main()
