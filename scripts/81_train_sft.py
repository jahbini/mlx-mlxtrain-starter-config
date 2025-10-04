# stjohns-jim-llm: Local fine-tuning stack for story-style LLMs on Mac

# 🗂️ Folder Structure:
# .
# ├── data/
# │   └── prepare_outmd.py           # Convert Markdown to instruction-tuning format
# ├── scripts/
# │   ├── mlx_train.py               # Fine-tuning with mlx-lm (Phi-3 on Apple Silicon)
# │   ├── train_rm.py                # Reward model training (optional)
# │   └── run_dpo.py                 # Direct Preference Optimization (optional)
# ├── serve/
# │   └── serve_ollama.py            # Serve model via Ollama or llama.cpp
# └── eval/
#     └── eval_story_mimic.py        # Embedding-based story match scoring
# scripts/mlx_train.py
"""
STEP — Local Fine-Tuning (Phi-3 on Apple Silicon)
Fine-tunes story-style data using mlx-lm on Mac (Metal).

Inputs:
    run/data/out_instruct.jsonl   (Alpaca-style JSONL)
Outputs:
    run/phi3-jim-mlx/ (fine-tuned model + tokenizer)

Notes:
- Uses mlx-lm for Apple-native training (Metal backend).
- Compatible with Phi-3-mini-4k-instruct.
- Config-aware, but also works standalone.
"""

from __future__ import annotations
import os, sys, json
from pathlib import Path
from tqdm import tqdm

import mlx.core as mx
import mlx.nn as nn
from mlx_lm import load
from mlx_lm.utils import save_model

# --- Config-aware defaults ---
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
try:
    from config_loader import load_config
    CFG = load_config()
    MODEL_NAME  = getattr(CFG.model, "base", "microsoft/Phi-3-mini-4k-instruct")
    DATA_PATH   = Path(CFG.data.output_dir) / "out_instruct.jsonl"
    OUTPUT_DIR  = Path(CFG.run.output_dir) / "phi3-jim-mlx"
    EPOCHS      = getattr(CFG.train, "epochs", 5)
    MAX_LEN     = getattr(CFG.train, "max_seq_length", 5120)
    BATCH_SIZE  = getattr(CFG.train, "batch_size", 1)
except Exception:
    MODEL_NAME = "microsoft/Phi-3-mini-4k-instruct"
    DATA_PATH  = Path("./run/data/out_instruct.jsonl")
    OUTPUT_DIR = Path("./phi3-jim-mlx")
    EPOCHS     = 5
    MAX_LEN    = 512
    BATCH_SIZE = 1

PROMPT_KEY   = "instruction"  # modern format
RESPONSE_KEY = "output"

# ------------------------
# Utility: load training samples
# ------------------------
def load_dataset(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            try:
                item = json.loads(line)
                prompt = item.get(PROMPT_KEY, "")
                resp   = item.get(RESPONSE_KEY, "")
                if prompt and resp:
                    yield f"{prompt.strip()}\n{resp.strip()}"
            except Exception:
                continue

# ------------------------
# Training Loop
# ------------------------
def train():
    model, tokenizer = load(MODEL_NAME)
    model.train()

    samples = list(load_dataset(DATA_PATH))
    print(f"Loaded {len(samples)} samples from {DATA_PATH}")

    # Simple AdamW optimizer (functional style preferred now in MLX)
    opt = nn.optimizers.AdamW(learning_rate=5e-5)

    for epoch in range(EPOCHS):
        print(f"\nEpoch {epoch+1}/{EPOCHS}")
        total_loss = 0.0

        for text in tqdm(samples):
            tokens = tokenizer.encode(text, max_length=MAX_LEN, truncation=True)
            input_ids = mx.array(tokens).reshape(1, -1)

            logits = model(input_ids)
            loss = nn.losses.cross_entropy(logits[:, :-1, :], input_ids[:, 1:])

            # Backward + optimizer step
            loss.backward()
            opt.step(model.parameters())
            opt.zero_grad(model.parameters())

            total_loss += float(loss.item())

        print(f"Epoch {epoch+1} avg loss: {total_loss/len(samples):.4f}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    save_model(OUTPUT_DIR, model)
    tokenizer.save_pretrained(OUTPUT_DIR)
    print(f"\n✅ Saved fine-tuned model to: {OUTPUT_DIR}")

if __name__ == "__main__":
    train()
