#!/usr/bin/env coffee
###
999_template.coffee — Pipeline-Compliant Step Template
------------------------------------------------------

Use this template when creating a new CoffeeScript pipeline step.

Rules:
  • All parameters and paths come from config (default + override).
  • No CLI args.
  • Deterministic: same input + config → same output.
  • Fail fast on missing inputs or bad config.
  • Logs written under <output>/logs/.
###

fs   = require 'fs'
path = require 'path'

# --- 1) Load Config -------------------------------------------------
{ load_config } = require '../config_loader'

CFG       = load_config()
STEP_NAME = process.env.STEP_NAME or '999_template'
STEP_CFG  = CFG.pipeline.steps[STEP_NAME]
PARAMS    = STEP_CFG?.params or {}

# --- 2) Resolve Directories ----------------------------------------
ROOT     = path.resolve process.env.EXEC or path.dirname(__dirname)
OUT_DIR  = path.resolve PARAMS.output_dir or CFG.data.output_dir
LOG_DIR  = path.join OUT_DIR, 'logs'
fs.mkdirSync OUT_DIR, {recursive: true}
fs.mkdirSync LOG_DIR, {recursive: true}

INPUT_FILE  = path.resolve PARAMS.input or path.join(OUT_DIR, CFG.data.contract)
OUTPUT_FILE = path.resolve PARAMS.output or path.join(OUT_DIR, "#{STEP_NAME}_output.json")

# --- 3) Logging -----------------------------------------------------
LOG_PATH = path.join LOG_DIR, "#{STEP_NAME}.log"
log = (msg) ->
  stamp = new Date().toISOString().replace('T',' ').replace(/\..+$/,'')
  line  = "[#{stamp}] #{msg}"
  fs.appendFileSync LOG_PATH, line + '\n', 'utf8'
  console.log line

# --- 4) Validate Inputs --------------------------------------------
unless fs.existsSync INPUT_FILE
  log "[FATAL] Missing required input file: #{INPUT_FILE}"
  process.exit 1

log "[INFO] Starting step '#{STEP_NAME}'"
log "[INFO] Output directory: #{OUT_DIR}"
log "[INFO] Step parameters: #{JSON.stringify PARAMS, null, 2}"

# --- 5) Core Logic (replace this section) --------------------------
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

result = processContract INPUT_FILE

# --- 6) Save Outputs -----------------------------------------------
fs.writeFileSync OUTPUT_FILE, JSON.stringify(result, null, 2), 'utf8'
log "[INFO] Wrote output: #{OUTPUT_FILE}"

# --- 7) Clean Exit --------------------------------------------------
log "[INFO] Completed step '#{STEP_NAME}' successfully"
process.exit 0