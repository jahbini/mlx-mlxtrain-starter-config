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

# -----------------------------
# scripts/mlx_train.py
# -----------------------------
import json
import mlx.core as mx
import mlx.nn as nn
import mlx.optimizers as optim
from pathlib import Path
from tqdm import tqdm
from mlx_lm import load
from mlx_lm import generate

MODEL_NAME = "microsoft/Phi-3-mini-4k-instruct"
DATA_PATH = "./run/data/out_instruct.jsonl"
OUTPUT_DIR = "phi3-jim-mlx"
EPOCHS = 5
MAX_LEN = 5120
BATCH_SIZE = 1

PROMPT_KEY = "prompt"
RESPONSE_KEY = "response"

# ------------------------
# Utility: load training samples
# ------------------------
def load_dataset(path):
    with open(path, 'r') as f:
        for line in f:
            item = json.loads(line)
            if PROMPT_KEY in item and RESPONSE_KEY in item:
                yield f"{item[PROMPT_KEY].strip()}\n{item[RESPONSE_KEY].strip()}"

# ------------------------
# Training Loop
# ------------------------
from mlx_lm.utils import save_model
def train():
    model, tokenizer = load(MODEL_NAME)
    model.train()

    samples = list(load_dataset(DATA_PATH))
    opt = optim.Adam(5e-5, model.parameters())

    for epoch in range(EPOCHS):
        print(f"\nEpoch {epoch+1}/{EPOCHS}")
        for text in tqdm(samples):
            tokens = tokenizer.encode(text, max_length=MAX_LEN, truncation=True)
            input_ids = mx.array(tokens).reshape(1, -1)

            logits = model(input_ids)
            loss = nn.losses.cross_entropy(logits[:, :-1, :], input_ids[:, 1:])

            loss.backward()
            opt.step()
            opt.zero_grad()


    save_model(OUTPUT_DIR, model)
    tokenizer.save_pretrained(OUTPUT_DIR)
    print(f"\n✅ Saved fine-tuned model to: {OUTPUT_DIR}")

if __name__ == "__main__":
    train()

# -----------------------------
# Notes:
# - Uses mlx-lm for Apple-native training
# - Compatible with Phi-3-mini (4K) instruct model
# - Automatically saves to ./phi3-jim-mlx
# - Prompts/Responses come from out_instruct.jsonl

# You're now fine-tuning in Metal, not CUDA. Just like '83 — but this time, the motherboard is in your head.
