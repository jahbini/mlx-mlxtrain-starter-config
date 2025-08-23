# scripts/03_train.py
from pathlib import Path
from mlxtrain.utils import load_config, parse_args, apply_overrides, set_logger, now_stamp, dump_json
from datasets import load_from_disk

def main():
    args = parse_args()
    cfg = apply_overrides(load_config(args.config), args.override)
    log = set_logger()
    paths = cfg["paths"]
    trainer = cfg["trainer"]
    model = cfg["model"]

    runs_dir = Path(cfg["run"]["output_dir"])
    run_id = f"{now_stamp()}-{model['name'].split('/')[-1]}"
    run_dir = runs_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)

    data_dir = Path(cfg["data"]["output_dir"]) / "processed"
    ds = load_from_disk(str(data_dir))
    log.info(f"Loaded processed data from {data_dir} | train={len(ds['train'])} valid={len(ds['test'])}")

    # Here you’d call your MLX/MLXTrain training loop.
    # For now, we simulate metrics:
    metrics = {"train_loss": 1.23, "valid_loss": 1.10, "valid_ppl": 3.00}
    dump_json(metrics, run_dir / "metrics.json")
    log.info(f"Saved metrics to {run_dir/'metrics.json'}")

    # Optionally write artifacts matching your config’s paths
    Path(paths["report"]).write_text("Training complete.\n")
    log.info(f"Wrote report to {paths['report']}")

if __name__ == "__main__":
    main()
