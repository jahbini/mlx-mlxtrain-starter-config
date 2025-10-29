#!/usr/bin/env coffee
###
999_template.coffee — Pipeline-Compliant Step Template (2025 Edition)
---------------------------------------------------------------------

Use this template for any new CoffeeScript pipeline step.

Principles:
  • All paths and parameters come from config (default + override)
  • Deterministic: same input + config → same output
  • No CLI args
  • Fails fast on bad inputs
  • Logs go under <output>/logs/
  • Designed for integration with the Memo system
###

fs   = require 'fs'
path = require 'path'

# -------------------------------------------------------------------
# 1) Load Config
# -------------------------------------------------------------------
{ load_config } = require '../config_loader'

CFG       = load_config()
STEP_NAME = process.env.STEP_NAME or '999_template'
STEP_CFG  = CFG.pipeline?.steps?[STEP_NAME] or {}
PARAMS    = STEP_CFG.params or {}

# -------------------------------------------------------------------
# 2) Directories
# -------------------------------------------------------------------
ROOT     = path.resolve process.env.EXEC or path.dirname(__dirname)
OUT_DIR  = path.resolve PARAMS.output_dir or CFG.data.output_dir
LOG_DIR  = path.join OUT_DIR, 'logs'
fs.mkdirSync OUT_DIR, {recursive: true}
fs.mkdirSync LOG_DIR, {recursive: true}

INPUT_FILE  = path.resolve PARAMS.input or path.join(OUT_DIR, CFG.data.contract)
OUTPUT_FILE = path.resolve PARAMS.output or path.join(OUT_DIR, "#{STEP_NAME}_output.json")

# -------------------------------------------------------------------
# 3) Logging utilities
# -------------------------------------------------------------------
LOG_PATH = path.join LOG_DIR, "#{STEP_NAME}.log"

log = (msg) ->
  stamp = new Date().toISOString().replace('T',' ').replace(/\..+$/,'')
  line  = "[#{stamp}] #{msg}"
  fs.appendFileSync LOG_PATH, line + '\n', 'utf8'
  console.log line

write_json = (fpath, obj) ->
  try
    fs.writeFileSync fpath, JSON.stringify(obj, null, 2), 'utf8'
    log "[OK] Wrote #{fpath}"
  catch err
    log "[FATAL] Could not write #{fpath}: #{err}"
    process.exit 2

# -------------------------------------------------------------------
# 4) Input validation
# -------------------------------------------------------------------
unless fs.existsSync INPUT_FILE
  log "[FATAL] Missing required input file: #{INPUT_FILE}"
  process.exit 1

log "[INFO] Starting step '#{STEP_NAME}'"
log "[INFO] Output directory: #{OUT_DIR}"
log "[INFO] Step parameters: #{JSON.stringify PARAMS, null, 2}"

# -------------------------------------------------------------------
# 5) Core Logic (replace this section for new steps)
# -------------------------------------------------------------------
processContract = (p) ->
  try
    raw  = fs.readFileSync p, 'utf8'
    data = JSON.parse raw
    result =
      summary: "Contract includes #{Object.keys(data.filenames or {}).length} items"
      timestamp: new Date().toISOString()
      git_commit: CFG.run?.git_commit or 'unknown'
    return result
  catch err
    log "[FATAL] Error processing contract: #{err}"
    process.exit 1

# -------------------------------------------------------------------
# 6) Execution
# -------------------------------------------------------------------
t0 = Date.now()
result = processContract INPUT_FILE
write_json OUTPUT_FILE, result

# -------------------------------------------------------------------
# 7) Memo / bookkeeping
# -------------------------------------------------------------------
try
  if global.M? and typeof global.M.saveThis is 'function'
    global.M.saveThis "#{STEP_NAME}:output", OUTPUT_FILE
    log "[INFO] Memo recorded output file."
catch err
  log "[WARN] Memo integration failed: #{err}"

# -------------------------------------------------------------------
# 8) Clean Exit
# -------------------------------------------------------------------
elapsed = ((Date.now() - t0)/1000).toFixed(2)
log "[INFO] Step runtime: #{elapsed}s"
log "[INFO] Completed step '#{STEP_NAME}' successfully"
process.exit 0