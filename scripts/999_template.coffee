### 
Template Script for Pipeline Integration
----------------------------------------

Use this template when creating a new script for the pipeline.  
All parameters and paths must come from config (default + override).  
No command-line arguments are allowed.

Inputs:
  - Defined in config (cfg.data.*, cfg.run.*, etc.)
Outputs:
  - Defined in config and written into PWD (never EXEC)
Logs:
  - Written into PWD/logs/

Behavior:
  - Deterministic (same input + config → same output)
  - Fail fast on missing inputs or invalid config
###

fs   = require 'fs'
path = require 'path'

# --- 1) Load Config ---
{load_config} = require '../config_loader'
cfg = load_config()

# Directories
outDir  = path.resolve cfg.data.output_dir
logDir  = path.join outDir, 'logs'
fs.mkdirSync outDir, {recursive: true}
fs.mkdirSync logDir, {recursive: true}

# Example input/output files from config
INPUT_FILE  = path.join outDir, cfg.data.contract     # replace with correct key
OUTPUT_FILE = path.join outDir, 'example_output.json' # replace with correct key

# --- 2) Logging Helper ---
logPath = path.join logDir, 'template.log'
log = (msg) ->
  fs.appendFileSync logPath, msg + '\n', 'utf8'
  console.log msg

# --- 3) Validate Inputs ---
unless fs.existsSync INPUT_FILE
  log "[FATAL] Missing required input file: #{INPUT_FILE}"
  process.exit 1

log "[INFO] Starting template script"
log "[INFO] Using config keys from: #{JSON.stringify cfg}"

# --- 4) Core Work (replace with real logic) ---
processContract = (p) ->
  try
    raw = fs.readFileSync p, 'utf8'
    data = JSON.parse raw
    result =
      summary: "Contract contains #{Object.keys(data.filenames or {}).length} files"
      git_commit: cfg.run?.git_commit or 'unknown'
    return result
  catch err
    log "[FATAL] Error processing contract: #{err}"
    process.exit 1

result = processContract INPUT_FILE

# --- 5) Save Outputs ---
fs.writeFileSync OUTPUT_FILE, JSON.stringify(result, null, 2), 'utf8'
log "[INFO] Wrote output: #{OUTPUT_FILE}"

# --- 6) Exit Cleanly ---
log "[INFO] Completed successfully"
process.exit 0
