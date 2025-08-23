# scripts/00_setup_data_dir.py
from pathlib import Path
import sys, os
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from mlxtrain.utils import  parse_args, apply_overrides, set_logger
from config_loader import load_config
cfg = load_config()

def main():
    args = parse_args()
    log = set_logger()
    data_root = Path(cfg["data"]["output_dir"])
    data_root.mkdir(parents=True, exist_ok=True)
    # your existing scraping logic here…
    # e.g., write to data_root / "raw/"
    log.info(f"Initialized data directory at {data_root.resolve()}")

if __name__ == "__main__":
    main()
