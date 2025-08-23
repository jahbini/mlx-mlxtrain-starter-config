# scripts/02_prepare_data.py
from pathlib import Path
from mlxtrain.utils import load_config, parse_args, apply_overrides, set_logger
from datasets import load_from_disk

def main():
    args = parse_args()
    cfg = apply_overrides(load_config(args.config), args.override)
    log = set_logger()

    data = cfg["data"]
    outdir = Path(data["output_dir"])
    ds = load_from_disk(str(outdir / "hf_cache"))

    min_w, max_w = data["min_words"], data["max_words"]
    def keep(rec): 
        n = len(str(rec.get("text", "")).split())
        return (n >= min_w) and (n <= max_w)

    ds = ds.filter(keep)
    ds = ds["train"].train_test_split(test_size=data["valid_fraction"], seed=42)
    processed_dir = outdir / "processed"
    ds.save_to_disk(str(processed_dir))
    log.info(f"Processed dataset saved to {processed_dir.resolve()} "
             f"(train={len(ds['train'])}, valid={len(ds['test'])})")

if __name__ == "__main__":
    main()
