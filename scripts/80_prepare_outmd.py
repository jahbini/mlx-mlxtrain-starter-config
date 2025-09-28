# stjohns-jim-llm: Local fine-tuning stack for story-style LLMs on Mac

# 🗂️ Folder Structure:
# .
# ├── data/
# │   └── prepare_outmd.py           # Convert Markdown to instruction-tuning format
# ├── scripts/
# │   ├── train_sft.py               # LoRA fine-tuning on local machine (Phi-3 ready)
# │   ├── train_rm.py                # Reward model training (optional)
# │   └── run_dpo.py                 # Direct Preference Optimization (optional)
# ├── serve/
# │   └── serve_ollama.py            # Serve model via Ollama or llama.cpp
# └── eval/
#     └── eval_story_mimic.py        # Embedding-based story match scoring

# -----------------------------
# data/prepare_outmd.py
# -----------------------------
import json
import os
from pathlib import Path

INPUT_MD = "../jim_stories/jim.md"
OUTPUT_JSONL = "./run/data/out_instruct.jsonl"

PROMPT_TEMPLATE = """
You are St. John's Jim, a myth-weaving, bar-stool Buddha of the Pacific Northwest.
Tell a new short story in your usual voice. Base it on this seed:
"""

def extract_snippets(md_text):
    # Naive chunking: split by headers
    chunks = [chunk.strip() for chunk in md_text.split("# ") if chunk.strip()]
    return chunks

def format_as_alpaca(chunk):
    return {
        "instruction": PROMPT_TEMPLATE + chunk[:200] + "...",
        "input": "",
        "output": chunk.strip()
    }

def main():
    with open(INPUT_MD, 'r') as f:
        md_text = f.read()

    chunks = extract_snippets(md_text)
    entries = [format_as_alpaca(c) for c in chunks]

    with open(OUTPUT_JSONL, 'w') as f:
        for entry in entries:
            f.write(json.dumps(entry) + "\n")

    print(f"Wrote {len(entries)} entries to {OUTPUT_JSONL}")

if __name__ == "__main__":
    main()
