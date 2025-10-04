# scripts/022_prepare_prompts.py
from __future__ import annotations
import sys, os, json, textwrap
from pathlib import Path
from typing import Dict, Any, List

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config

# --- STEP-AWARE CONFIG ---
CFG = load_config()
STEP_NAME = os.environ["STEP_NAME"]
STEP_CFG  = CFG.pipeline.steps[STEP_NAME]
PARAMS    = getattr(STEP_CFG, "params", {})

# Resolve output locations from params > global config
OUT_DIR = Path(getattr(PARAMS, "output_dir", CFG.data.output_dir)); OUT_DIR.mkdir(exist_ok=True)
CONTRACT = OUT_DIR / getattr(PARAMS, "contract", CFG.data.contract)
POLICY   = OUT_DIR / getattr(PARAMS, "policy", CFG.data.policy)

# ------------------------
# 1) Choose a template
# ------------------------
TEMPLATE_NAME = PARAMS.get("template_name", "plain_text_passthrough")
STOP_STRINGS  = PARAMS.get("stop_strings", ["\n\n"])
USE_EOS_TOKEN = PARAMS.get("use_eos_token", True)

# ------------------------
# 2) Load contract & sample a few rows
# ------------------------
def load_contract(path: Path):
    c = json.loads(path.read_text(encoding="utf-8"))
    data_dir = Path(c["data_dir"])
    files = {k: v["resolved"] for k, v in c["filenames"].items() if v.get("resolved")}
    fields = c.get("schema", {}).get("fields", {})
    text_field = next((k for k, v in fields.items() if str(v).lower() == "string"), "text")
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
# 3) Define template functions
# ------------------------
def fmt_plain(text: str) -> str:
    return text

def fmt_icl_minimal(text: str) -> str:
    return (
        "### Instruction\n"
        "Share an important thought.\n\n"
        "### Response\n" + text.strip()
    )

def fmt_llama3_style(text: str) -> str:
    return (
        "<s>[INSTRUCTION]\n"
        "Share an important thought.\n"
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
# 4) Preview
# ------------------------
print("=== FORMAT PREVIEW ===")
print(f"Template: {TEMPLATE_NAME}")
for i, txt in enumerate(SAMPLES, 1):
    print(f"\n--- Example {i}: BEFORE ---")
    print(textwrap.shorten(txt.replace("\n"," \\n "), width=220, placeholder="…"))
    print(f"--- Example {i}: AFTER  ---")
    print(textwrap.shorten(formatter(txt).replace("\n"," \\n "), width=220, placeholder="…"))

# ------------------------
# 5) Persist policy
# ------------------------
policy: Dict[str, Any] = {
    "template_name": TEMPLATE_NAME,
    "text_field": TEXT_FIELD,
    "stop_strings": STOP_STRINGS,
    "use_eos_token": USE_EOS_TOKEN,
    "notes": [
        "This policy describes how to *format* examples when generating or materializing new data.",
        "Your current JSONL will not be changed by this step.",
        "Downstream steps can choose to apply this formatter or keep passthrough depending on the experiment."
    ],
    "preview": [
        {"before": SAMPLES[i], "after": formatter(SAMPLES[i])} for i in range(min(2, len(SAMPLES)))
    ]
}

POLICY.write_text(json.dumps(policy, indent=2), encoding="utf-8")
print(f"\nWrote {POLICY}")
