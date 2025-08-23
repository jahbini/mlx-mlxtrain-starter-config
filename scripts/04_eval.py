# scripts/04_eval.py
from pathlib import Path
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config
cfg = load_config()
from mlxtrain.utils import load_config, parse_args, apply_overrides, set_logger, dump_json

def main():
    args = parse_args()
    log = set_logger()
    # Load model/checkpoint, run eval; here we stub:
    results = {"bleu": 0.0, "rougeL": 0.0}
    out = Path(cfg["eval"]["output_dir"]); out.mkdir(parents=True, exist_ok=True)
    dump_json(results, out / "report.json")
    log.info(f"Eval report at {out/'report.json'}")

if __name__ == "__main__":
    main()
