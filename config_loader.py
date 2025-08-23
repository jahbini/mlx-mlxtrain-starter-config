from __future__ import annotations
import os, json, argparse
from copy import deepcopy

try:
    from dotenv import load_dotenv
    load_dotenv(override=False)
except Exception:
    pass

try:
    import yaml
except ImportError as e:
    raise SystemExit("Please `pip install pyyaml python-dotenv`") from e


# --- Config object wrapper ---
class Config:
    """
    Recursive wrapper so you can use dot access:
        cfg.model.name
    Also has .as_dict() to get the underlying dict back.
    """
    def __init__(self, data: dict):
        for k, v in data.items():
            if isinstance(v, dict):
                v = Config(v)
            elif isinstance(v, list):
                v = [Config(x) if isinstance(x, dict) else x for x in v]
            setattr(self, k, v)

    def __getitem__(self, key):
        return getattr(self, key)

    def as_dict(self) -> dict:
        """Convert back to plain dict recursively."""
        out = {}
        for k, v in self.__dict__.items():
            if isinstance(v, Config):
                v = v.as_dict()
            elif isinstance(v, list):
                v = [x.as_dict() if isinstance(x, Config) else x for x in v]
            out[k] = v
        return out

    def __repr__(self):
        return f"Config({self.__dict__})"


# --- Helpers ---
def _deep_update(dst: dict, src: dict) -> dict:
    for k, v in src.items():
        if isinstance(v, dict) and isinstance(dst.get(k), dict):
            _deep_update(dst[k], v)
        else:
            dst[k] = v
    return dst

def _load_yaml(path: str) -> dict:
    if not os.path.exists(path):
        return {}
    with open(path, "r") as f:
        return yaml.safe_load(f) or {}

def _env_overrides(prefix: str = "CFG_") -> dict:
    out = {}
    for k, v in os.environ.items():
        if not k.startswith(prefix):
            continue
        path = k[len(prefix):].split("__")
        try:
            val = json.loads(v)
        except Exception:
            val = v
        node = out
        for part in path[:-1]:
            node = node.setdefault(part, {})
        node[path[-1]] = val
    return out


# --- Main loader ---
def load_config(
    default_path: str = "config/default.yaml",
    local_path: str = "config/local.yaml",
    cli_overrides: dict | None = None,
    env_prefix: str = "CFG_",
) -> Config:
    """
    Precedence (lowest -> highest):
      default.yaml < local.yaml < environment (CFG_*) < cli_overrides
    Returns a Config object with dot-access.
    """
    cfg = _load_yaml(default_path)
    _deep_update(cfg, _load_yaml(local_path))
    _deep_update(cfg, _env_overrides(env_prefix))
    if cli_overrides:
        _deep_update(cfg, cli_overrides)

    return Config(cfg)


# --- CLI helpers (unchanged) ---
def config_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(add_help=False)
    p.add_argument("--config", default="config/default.yaml")
    p.add_argument("--local", default="config/local.yaml")
    p.add_argument("--set", action="append", default=[], help="Override key=JSON (nested via dots).")
    return p

def parse_set_overrides(pairs: list[str]) -> dict:
    def set_nested(d: dict, dotted: str, value):
        parts = dotted.split(".")
        node = d
        for part in parts[:-1]:
            node = node.setdefault(part, {})
        node[parts[-1]] = value

    out = {}
    for pair in pairs:
        if "=" not in pair:
            raise ValueError(f"--set must be key=VALUE, got: {pair}")
        k, v = pair.split("=", 1)
        try:
            v_parsed = json.loads(v)
        except Exception:
            v_parsed = v
        set_nested(out, k, v_parsed)
    return out
