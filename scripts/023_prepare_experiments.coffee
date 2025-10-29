###
  022_prepare_experiment.coffee
  -----------------------------
  Direct CoffeeScript analog of 022_prepare_experiment.py
  ✅ Ready for declarative integration into the pipeline (e.g., "prepare_experiment" step).

  Function:
    - Loads the data_contract and prompt_policy from prior steps
    - Fuses them with global run configuration
    - Builds a normalized "experiment_manifest.json"
    - Prints summary table of contents
###

fs   = require 'fs'
path = require 'path'
yaml = require 'js-yaml'

# --- STEP-AWARE CONFIG ---
CFG_PATH  = process.env.CFG_OVERRIDE or path.join(process.cwd(), 'experiment.yaml')
STEP_NAME = process.env.STEP_NAME or 'prepare_experiment'
cfgFull   = yaml.load(fs.readFileSync(CFG_PATH, 'utf8'))
STEP_CFG  = cfgFull[STEP_NAME] or {}
RUN_CFG   = cfgFull['run'] or {}

RUN_DIR   = path.resolve(RUN_CFG.data_dir or 'data')
fs.mkdirSync(RUN_DIR, {recursive:true})
CONTRACT  = path.join(RUN_DIR, RUN_CFG.contract or 'data_contract.json')
POLICY    = path.join(RUN_DIR, RUN_CFG.policy or 'prompt_policy.json')
OUT_PATH  = path.join(RUN_DIR, RUN_CFG.experiment_manifest or 'experiment_manifest.json')

# --- Helpers ---
readJSON = (p) ->
  try
    JSON.parse(fs.readFileSync(p, 'utf8'))
  catch e
    console.error "Failed to read JSON:", p, e.message
    {}

# --- Load components ---
contract = readJSON(CONTRACT)
policy   = readJSON(POLICY)

# --- Compose manifest ---
manifest =
  created_utc: new Date().toISOString().replace(/\.\d+Z$/, 'Z')
  run:
    output_dir: RUN_CFG.output_dir
    data_dir: RUN_CFG.data_dir
    eval_dir: RUN_CFG.eval_dir
    snapshot_dir: RUN_CFG.snapshot_dir
    experiments_csv: RUN_CFG.experiments_csv
  contract:
    path: CONTRACT
    schema: contract.schema
    data_dir: contract.data_dir
    files: contract.filenames
  prompt_policy:
    template_name: policy.template_name
    stop_strings: policy.stop_strings
    use_eos_token: policy.use_eos_token
    text_field: policy.text_field
  notes: [
    "This manifest consolidates data + prompt configuration into a single reference point."
    "All downstream steps (train, snapshot, eval) can read this file for consistency."
  ]

# --- Write manifest ---
fs.writeFileSync(OUT_PATH, JSON.stringify(manifest, null, 2), 'utf8')
console.log "Wrote #{OUT_PATH}"

# --- Console summary ---
console.log "\n=== EXPERIMENT MANIFEST ==="
console.log "Data dir:   #{manifest.contract.data_dir}"
console.log "Template:   #{manifest.prompt_policy.template_name}"
console.log "Stop tokens:", (manifest.prompt_policy.stop_strings or []).join(', ') or '(none)'
console.log "EOS token:  #{manifest.prompt_policy.use_eos_token}"
console.log "Schema keys:", Object.keys(manifest.contract.schema?.fields or {}).join(', ') or '(none)'
console.log "Files:", Object.keys(manifest.contract.files or {}).join(', ') or '(none)'

# Optional memo save
try
  if global.M? and typeof global.M.saveThis is 'function'
    global.M.saveThis 'prepare_experiment:manifest', manifest
catch e
  console.warn "(memo skip)", e.message