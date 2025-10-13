#!/usr/bin/env coffee
###
042_prepare_outmd_kag.coffee
----------------------------------------
STEP — Prepare Markdown Stories for KAG Fine-Tuning

Converts jim.md (Markdown stories) into KAG-style JSONL:
Each entry has:
    { "prompt": "...", "response": "..." }

Inputs:
    jim.md   – Markdown with "# " headers separating stories
Outputs:
    run/data/out_kag.jsonl
###

fs   = require 'fs'
path = require 'path'
os   = require 'os'
process.env.NODE_NO_WARNINGS = 1

# --- 1) Config loader ---
{load_config} = require '../config_loader'
CFG       = load_config()
STEP_NAME = process.env.STEP_NAME or 'prepare_outmd_kag'
STEP_CFG  = CFG.pipeline.steps?[STEP_NAME] or {}
PARAMS    = STEP_CFG.params or {}

# --- 2) Paths ---
DATA_DIR = path.resolve PARAMS.output_dir or CFG.data.output_dir
LOG_DIR  = path.join DATA_DIR, 'logs'
INPUT_MD = PARAMS.input_md or CFG.data.stories or 'jim_stories/jim.md'
OUTPUT_JSONL = path.join DATA_DIR, (PARAMS.output_jsonl or 'out_kag.jsonl')

fs.mkdirSync DATA_DIR, {recursive: true}
fs.mkdirSync LOG_DIR,  {recursive: true}

# --- 3) Logging helper ---
logPath = path.join LOG_DIR, "#{STEP_NAME}.log"
log = (msg) ->
  stamp = new Date().toISOString().replace('T',' ').replace('Z','')
  line  = "[#{stamp}] #{msg}"
  fs.appendFileSync logPath, line + os.EOL, 'utf8'
  console.log line

# --- 4) Parameters ---
PROMPT_TEMPLATE = [
  "You are St. John's Jim — a myth-weaving, bar-stool Buddha of the Pacific Northwest.",
  "Tell a new short story in your own voice, using this idea as inspiration:\n"
].join '\n'

# --- 5) Helpers ---
extract_snippets = (mdText) ->
  # Split by '# ' headers
  chunks = mdText.split('# ')
  chunks = (c.trim() for c in chunks when c.trim().length > 0)
  return chunks

make_entry = (chunk) ->
  prompt = PROMPT_TEMPLATE + chunk.slice(0, 200) + '...'
  response = chunk.trim()
  return {prompt, response}

# --- 6) Main logic ---
main = ->
  log "Starting step: #{STEP_NAME}"
  log "Input markdown: #{INPUT_MD}"
  log "Output JSONL:   #{OUTPUT_JSONL}"

  unless fs.existsSync INPUT_MD
    log "[FATAL] Missing input markdown: #{INPUT_MD}"
    process.exit 1

  mdText = fs.readFileSync(INPUT_MD, 'utf8')
  chunks = extract_snippets mdText
  entries = (make_entry c for c in chunks)

  out = fs.createWriteStream OUTPUT_JSONL, encoding: 'utf8'
  for entry in entries
    out.write JSON.stringify(entry) + '\n'
  out.end()

  log "[OK] Wrote #{entries.length} entries to #{OUTPUT_JSONL}"
  log "Completed successfully."
  process.exit 0

# --- 7) Run ---
main()
