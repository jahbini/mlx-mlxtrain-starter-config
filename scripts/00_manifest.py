# STEP 1 — Run Manifest & Environment (Apple Silicon / MLX)
# - Captures exact runtime info (OS, chip, Python, key libs)
# - Locks dependencies via `pip freeze` -> requirements.lock
# - Sets deterministic seeds (random, numpy; PYTHONHASHSEED)
# - Writes manifest to run_manifest.yaml (falls back to JSON if PyYAML missing)

import os, sys, platform, subprocess, json, time, hashlib, shlex
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from pathlib import Path
sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from config_loader import load_config

# ---------- Configuration (edit as needed) ----------
CFG = load_config()  # pulls default.yaml, then local.yaml, then CFG_* env, then (optionally) pass a dict of overrides
print("JIM config",CFG)
OUT_DIR = Path(CFG.run.output_dir)                  # where to write outputs
LOCKFILE = OUT_DIR / "requirements.lock"
MANIFEST_YAML = OUT_DIR / "run_manifest.yaml"
MANIFEST_JSON = OUT_DIR / "run_manifest.json"
SEED = CFG.run.seed
ARTIFACTS = CFG.data.artifacts
# ----------------------------------------------------

# 1) Set seeds for determinism (Python & NumPy)
import random
random.seed(SEED)
os.environ["PYTHONHASHSEED"] = str(SEED)
try:
    import numpy as np
    np.random.seed(SEED)
    numpy_ver = np.__version__
except Exception:
    numpy_ver = None

# 2) Collect environment info
def _safe_import_version(pkg_name):
    try:
        import importlib.metadata as md
        return md.version(pkg_name)
    except Exception:
        return None

def _which(cmd):
    try:
        r = subprocess.run(["which", cmd], capture_output=True, text=True)
        return r.stdout.strip() or None
    except Exception:
        return None

def _run(cmd):
    try:
        r = subprocess.run(cmd, shell=True, capture_output=True, text=True)
        return (r.returncode, r.stdout.strip(), r.stderr.strip())
    except Exception as e:
        return (1, "", str(e))

py_ver = sys.version.split()[0]
platform_info = {
    "system": platform.system(),
    "release": platform.release(),
    "version": platform.version(),
    "machine": platform.machine(),
    "processor": platform.processor(),
    "python": py_ver,
}

# Apple chip details (best-effort)
chip_brand = None
if platform.system() == "Darwin":
    code, out, _ = _run("sysctl -n machdep.cpu.brand_string")
    chip_brand = out if code == 0 else None
    platform_info["mac_ver"] = platform.mac_ver()[0]
platform_info["chip_brand"] = chip_brand

# 3) Key package versions (MLX-focused)
mlx_lm_ver   = _safe_import_version("mlx-lm")
datasets_ver = _safe_import_version("datasets")
pandas_ver   = _safe_import_version("pandas")
tqdm_ver     = _safe_import_version("tqdm")

# 4) Lock dependencies with pip freeze
LOCKFILE.parent.mkdir(parents=True, exist_ok=True)
code, out, err = _run(f"{shlex.quote(sys.executable)} -m pip freeze")
if code == 0:
    LOCKFILE.write_text(out + "\n", encoding="utf-8")
else:
    print("[warn] pip freeze failed:", err)

# Hash the lock for quick integrity checks
lock_hash = None
if LOCKFILE.exists():
    lock_hash = hashlib.sha256(LOCKFILE.read_bytes()).hexdigest()

# 5) Build manifest object
manifest = {
    "timestamp_utc": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    "seed": SEED,
    "platform": platform_info,
    "packages": {
        "mlx-lm": mlx_lm_ver,
        "datasets": datasets_ver,
        "pandas": pandas_ver,
        "tqdm": tqdm_ver,
        "numpy": numpy_ver,
    },
    "executables": {
        "python": sys.executable,
        "python_which": _which("python"),
        "pip_which": _which("pip"),
    },
    "artifacts": {
        "requirements_lock": str(LOCKFILE.resolve()) if LOCKFILE.exists() else None,
        "requirements_lock_sha256": lock_hash,
    },
    "notes": [
        "This manifest anchors the run. Keep it with any training outputs.",
        "If you change env/deps, regenerate this step to create a new lock."
    ],
}

# 6) Write manifest to YAML (fallback to JSON if PyYAML not installed)
def write_manifest_yaml(obj, path_yaml, path_json_fallback):
    try:
        import yaml  # type: ignore
        with open(path_yaml, "w", encoding="utf-8") as f:
            yaml.safe_dump(obj, f, sort_keys=False)
        return str(path_yaml)
    except Exception as e:
        # Fallback JSON
        with open(path_json_fallback, "w", encoding="utf-8") as f:
            json.dump(obj, f, indent=2)
        return f"{path_yaml} (PyYAML missing) -> wrote JSON: {path_json_fallback}"

out_path = write_manifest_yaml(manifest, MANIFEST_YAML, MANIFEST_JSON)

# 7) Print a compact summary
print("\n=== RUN MANIFEST SUMMARY ===")
print(f"Python:        {py_ver}")
print(f"OS/Chip:       {platform_info['system']} {platform_info.get('mac_ver') or platform_info['release']} | {platform_info.get('chip_brand') or platform_info['machine']}")
print(f"mlx-lm:        {mlx_lm_ver}")
print(f"datasets:      {datasets_ver}")
print(f"pandas:        {pandas_ver}")
print(f"tqdm:          {tqdm_ver}")
print(f"numpy:         {numpy_ver}")
print(f"Seed:          {SEED}")
print(f"Lockfile:      {LOCKFILE}  sha256={lock_hash[:12]+'…' if lock_hash else None}")
print(f"Manifest path: {out_path}")
print("============================\n")
