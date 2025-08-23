# scripts/01_fetch_hf_dataset.py
from pathlib import Path
from mlxtrain.utils import load_config, parse_args, apply_overrides, set_logger
from datasets import load_dataset

def main():
    args = parse_args()
    cfg = apply_overrides(load_config(args.config), args.override)
    log = set_logger()
    name   = cfg["data"]["hf_dataset"]
    subset = cfg["data"]["subset"]
    outdir = Path(cfg["data"]["output_dir"])
    outdir.mkdir(parents=True, exist_ok=True)
    log.info(f"Loading HF dataset: {name}/{subset}")
    ds = load_dataset(name, subset)
    # save to disk (arrow), or jsonl for transparency
    arrow_dir = outdir / "hf_cache"
    ds.save_to_disk(str(arrow_dir))
    log.info(f"Saved dataset to {arrow_dir.resolve()} with splits: {list(ds.keys())}")

if __name__ == "__main__":
    main()
