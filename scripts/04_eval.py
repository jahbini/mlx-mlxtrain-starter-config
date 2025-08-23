# scripts/04_eval.py
from pathlib import Path
from mlxtrain.utils import load_config, parse_args, apply_overrides, set_logger, dump_json

def main():
    args = parse_args()
    cfg = apply_overrides(load_config(args.config), args.override)
    log = set_logger()
    # Load model/checkpoint, run eval; here we stub:
    results = {"bleu": 0.0, "rougeL": 0.0}
    out = Path(cfg["eval"]["output_dir"]); out.mkdir(parents=True, exist_ok=True)
    dump_json(results, out / "report.json")
    log.info(f"Eval report at {out/'report.json'}")

if __name__ == "__main__":
    main()
