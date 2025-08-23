# scripts/00_setup_data_dir.py
from pathlib import Path
from mlxtrain.utils import load_config, parse_args, apply_overrides, set_logger

def main():
    args = parse_args()
    cfg = apply_overrides(load_config(args.config), args.override)
    log = set_logger()
    data_root = Path(cfg["data"]["output_dir"])
    data_root.mkdir(parents=True, exist_ok=True)
    # your existing scraping logic here…
    # e.g., write to data_root / "raw/"
    log.info(f"Initialized data directory at {data_root.resolve()}")

if __name__ == "__main__":
    main()
