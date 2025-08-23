# mlxtrain/utils.py
from pathlib import Path
import argparse, json, logging, sys, time, yaml

def load_config(path: str):
    with open(path, "r") as f:
        return yaml.safe_load(f)

def dump_json(obj, path: str):
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w") as f:
        json.dump(obj, f, indent=2)

def set_logger(name="app", level="INFO"):
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        fmt = logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s")
        h.setFormatter(fmt)
        logger.addHandler(h)
    logger.setLevel(level)
    return logger

def parse_args():
    p = argparse.ArgumentParser()
    p.add_argument("--config", default="configs/default.yaml")
    # allow ad‑hoc overrides without changing YAML (simple K=V pairs)
    p.add_argument("--override", nargs="*", default=[], help="key=val pairs")
    return p.parse_args()

def apply_overrides(cfg: dict, pairs: list[str]):
    # very simple dotted.key=value overrides
    for pair in pairs:
        k, v = pair.split("=", 1)
        node = cfg
        keys = k.split(".")
        for kk in keys[:-1]:
            node = node.setdefault(kk, {})
        # try int/float/bool, else str
        vv = v
        for caster in (int, float):
            try:
                vv = caster(v)
                break
            except ValueError:
                pass
        if v.lower() in ("true", "false"):
            vv = v.lower() == "true"
        node[keys[-1]] = vv
    return cfg

def now_stamp():
    return time.strftime("%Y%m%d-%H%M%S")
