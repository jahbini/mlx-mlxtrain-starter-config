
# 🗂️ Folder Structure:
# .
# ├── data/
# │   └── prepare_outmd.py           # Convert Markdown to instruction-tuning format
# ├── scripts/
# │   ├── mlx_train.py               # Fine-tuning with mlx-lm (Phi-3 on Apple Silicon)
# │   ├── chat.py                # Query your fine-tuned Phi-3 model
# │   ├── train_rm.py                # Reward model training (optional)
# │   └── run_dpo.py                 # Direct Preference Optimization (optional)
# ├── serve/
# │   └── serve_ollama.py            # Serve model via Ollama or llama.cpp
# └── eval/
#     └── eval_story_mimic.py        # Embedding-based story match scoring

# -----------------------------
# scripts/chat.py
# -----------------------------
from mlx_lm import load, generate
from mlx_lm.utils import load_model
import readline

from pathlib import Path
MODEL_NAME = "microsoft/Phi-3-mini-4k-instruct"
WEIGHTS_PATH = Path( "phi3-mlx/model.safetensors" )

PROMPT_TEMPLATE = """
You are St. John's Jim, a myth-weaving, bar-stool Buddha of the Pacific Northwest.
Tell a new short story in your usual voice. Base it on this seed:
"""

model, tokenizer = load(MODEL_NAME)

print("\n🌀 Chatting with the Jim-tuned model. Type your story seed. Ctrl+C to exit.\n")

while True:
    try:
        user_input = input("Seed > ").strip()
        if not user_input:
            continue

        full_prompt = PROMPT_TEMPLATE + user_input + "\n"
        response = generate(model, tokenizer, prompt=full_prompt, verbose=False, max_tokens=512)
        print("\n📘 Jim says:\n" + response + "\n")

    except (KeyboardInterrupt, EOFError):
        print("\n👋 Goodbye!")
        break

# -----------------------------
# Usage:
# $ python scripts/chat.py
# Type a story seed, get a response in Jim's voice.
# -----------------------------
