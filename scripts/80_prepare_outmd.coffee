#!/usr/bin/env coffee
###
080_prepare_outmd.coffee
----------------------------------------
STEP — Prepare Markdown Stories for Instruction Tuning

Converts a markdown file (# headers split sections)
into Alpaca-style JSONL for fine-tuning.

Inputs:
  your.md (from experiment.yaml or CFG)
Outputs:
  run/data/out_instruct.jsonl

Config block example:
  prepare_outmd:
    run: scripts/080_prepare_outmd.coffee
    input_md: your/your.md
    output_jsonl: out_instruct.jsonl
    output_dir: run/data
###

fs   = require 'fs'
path = require 'path'
os   = require 'os'
yaml = require 'js-yaml'

process.env.NODE_NO_WARNINGS = 1

# --- Config loader ----------------------------------------------------
STEP_NAME = process.env.STEP_NAME or 'prepare_outmd'
CFG_PATH  = process.env.CFG_OVERRIDE or path.join process.cwd(), 'experiment.yaml'

try
  CFG_FULL = yaml.load fs.readFileSync(CFG_PATH, 'utf8')
catch err
  console.error "❌ Could not load #{CFG_PATH}: #{err.message}"
  CFG_FULL = {}

RUN_CFG  = CFG_FULL?.run or {}
STEP_CFG = CFG_FULL?[STEP_NAME] or {}

DATA_DIR = path.resolve STEP_CFG.output_dir or RUN_CFG.data_dir or 'run/data'
LOG_DIR  = path.resolve STEP_CFG.log_dir or path.join(DATA_DIR, 'logs')
INPUT_MD = path.resolve STEP_CFG.input_md or RUN_CFG.stories or 'your/your.md'
OUTPUT_JSONL = path.resolve DATA_DIR, STEP_CFG.output_jsonl or 'out_instruct.jsonl'

fs.mkdirSync DATA_DIR, {recursive:true}
fs.mkdirSync LOG_DIR, {recursive:true}

# --- Logging -----------------------------------------------------------
LOG_PATH = path.join LOG_DIR, "#{STEP_NAME}.log"
log = (msg) ->
  stamp = new Date().toISOString().replace('T',' ').replace('Z','')
  line  = "[#{stamp}] #{msg}"
  try fs.appendFileSync LOG_PATH, line + os.EOL, 'utf8' catch e then null
  console.log line

# --- Template & Helpers ------------------------------------------------
PROMPT_TEMPLATE = """
You are St. John's Jim, a myth-weaving, bar-stool Buddha of the Pacific Northwest.
Tell a new short story in your usual voice. Base it on this seed:
"""

extract_snippets = (md_text) ->
  md_text.split('# ').map((chunk) -> chunk.trim()).filter (c) -> c.length > 0

format_as_alpaca = (chunk) ->
  instruction = PROMPT_TEMPLATE + chunk.slice(0, 200) + '...'
  {
    instruction: instruction
    input: ''
    output: chunk.trim()
  }

# --- Main --------------------------------------------------------------
main = ->
  log "Starting step: #{STEP_NAME}"
  log "Input: #{INPUT_MD}"
  log "Output: #{OUTPUT_JSONL}"

  unless fs.existsSync INPUT_MD
    log "[FATAL] Missing markdown: #{INPUT_MD}"
    process.exit 1

  md_text = fs.readFileSync(INPUT_MD, 'utf8')
  chunks  = extract_snippets md_text
  entries = (format_as_alpaca(c) for c in chunks)

  out = fs.createWriteStream OUTPUT_JSONL, encoding:'utf8'
  count = 0
  for e in entries
    out.write JSON.stringify(e) + '\n'
    count += 1
  out.end()

  log "[OK] Wrote #{count} entries to #{OUTPUT_JSONL}"

  # Optional Memo signal
  try
    if global.M? and typeof global.M.saveThis is 'function'
      global.M.saveThis "done:#{STEP_NAME}", true
      global.M.saveThis "#{STEP_NAME}:stats", {count, output: OUTPUT_JSONL}
  catch e
    log "(memo skip) #{e.message}"

  process.exit 0

main()