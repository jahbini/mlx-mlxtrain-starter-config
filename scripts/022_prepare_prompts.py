# scripts/022_prepare_prompts.py
from __future__ import annotations
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config
cfg = load_config()
# STEP 4 — Formatting Policy (Prompt Template)
# Goal:
#   - Pick a formatting policy for how prompts/responses should look during training & generation.
#   - DOES NOT MODIFY your existing JSONL files.
#   - Writes `format_policy.json` describing the chosen template + parameters.
#
# Inputs:
#   - data_contract.json  (from Step 2)
# Outputs:
#   - format_policy.json  (template choice & settings)
# Console:
#   - Before/After preview for a few examples

import json, textwrap
from pathlib import Path
from typing import Dict, Any, List

out_dir = Path(cfg.data.output_dir); out_dir.mkdir(exist_ok=True)
CONTRACT    = out_dir / cfg.paths.contract
POLICY      = out_dir / cfg.paths.policy

# ------------------------
# 1) Choose a template
# ------------------------
# Options:
#   "plain_text_passthrough" : use the JSONL "text" as-is (good when your data already contains instruction/response)
#   "icl_minimal"            : a simple Q/A style wrapper (no special chat tokens)
#   "llama3_style"           : a friendly chat-like wrapper (ASCII tags only)
TEMPLATE_NAME = "plain_text_passthrough"

# Optional stop strings you intend to use during generation probes later.
# (These are just recorded here; not enforced yet.)
STOP_STRINGS = ["\n\n"]   # common “blank line” stop
USE_EOS_TOKEN = True      # whether to set eos_token_id in “default” runs later

# ------------------------
# 2) Load contract & sample a few rows for preview
# ------------------------
def load_contract(path: Path):
    c = json.loads(path.read_text(encoding="utf-8"))
    data_dir = Path(c["data_dir"])
    files = {k: v["resolved"] for k, v in c["filenames"].items() if v.get("resolved")}
    # detect the text field name (from schema) with fallback
    fields = c.get("schema", {}).get("fields", {})
    text_field = next((k for k,v in fields.items() if str(v).lower()=="string"), "text")
    return data_dir, files, text_field

data_dir, files, TEXT_FIELD = load_contract(CONTRACT)
train_path = Path(files["train"])

def read_first_n_texts(p: Path, n: int = 3, field: str = "text") -> List[str]:
    out = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            if len(out) >= n: break
            try:
                obj = json.loads(line)
            except Exception:
                continue
            val = obj.get(field)
            if isinstance(val, str):
                out.append(val)
    return out

SAMPLES = read_first_n_texts(train_path, n=3, field=TEXT_FIELD)

# ------------------------
# 3) Define template functions (no mutation)
# ------------------------
def fmt_plain(text: str) -> str:
    # return exactly as stored
    return text

def fmt_icl_minimal(text: str) -> str:
    # Wrap the existing content as a single instruction→response block.
    # If your JSONL already contains both, this is nearly a no-op with a header.
    return (
        "### Instruction\n"
        "Share an important thought.\n\n"
        "### Response\n" + text.strip()
    )

def fmt_llama3_style(text: str) -> str:
    # A neutral chat-ish style using plain ASCII delimiters.
    # (We avoid special tokens here; real chat templates can be added later if desired.)
    return (
        "<s>[INSTRUCTION]\n"
        "Share an .\n"
        "[/INSTRUCTION]\n"
        "[RESPONSE]\n" + text.strip() + "\n[/RESPONSE]</s>"
    )

FORMATTERS = {
    "plain_text_passthrough": fmt_plain,
    "icl_minimal": fmt_icl_minimal,
    "llama3_style": fmt_llama3_style,
}

if TEMPLATE_NAME not in FORMATTERS:
    raise SystemExit(f"Unknown TEMPLATE_NAME: {TEMPLATE_NAME}")

formatter = FORMATTERS[TEMPLATE_NAME]

# ------------------------
# 4) Preview: before/after for a few rows
# ------------------------
print("=== FORMAT PREVIEW ===")
print(f"Template: {TEMPLATE_NAME}")
for i, txt in enumerate(SAMPLES, 1):
    print(f"\n--- Example {i}: BEFORE ---")
    print(textwrap.shorten(txt.replace("\n"," \\n "), width=220, placeholder="…"))
    print("--- Example {i}: AFTER  ---")
    print(textwrap.shorten(formatter(txt).replace("\n"," \\n "), width=220, placeholder="…"))

# ------------------------
# 5) Persist policy (for downstream steps)
# ------------------------
policy: Dict[str, Any] = {
    "template_name": TEMPLATE_NAME,
    "text_field": TEXT_FIELD,
    "stop_strings": STOP_STRINGS,
    "use_eos_token": USE_EOS_TOKEN,
    "notes": [
        "This policy describes how to *format* examples when generating or when materializing new data.",
        "Your current JSONL will not be changed by this step.",
        "Downstream steps can choose to apply this formatter or keep passthrough depending on the experiment."
    ],
}

# Keep a tiny deterministic sample of BEFORE/AFTER in the policy for traceability
policy["preview"] = [
    {"before": SAMPLES[i], "after": formatter(SAMPLES[i])} for i in range(min(2, len(SAMPLES)))
]

POLICY.write_text(json.dumps(policy, indent=2), encoding="utf-8")
print(f"\nWrote {POLICY}")
