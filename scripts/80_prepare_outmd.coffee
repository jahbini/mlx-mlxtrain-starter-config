#!/usr/bin/env coffee
###
080_prepare_outmd.coffee — strict memo-aware (2025)
----------------------------------------------------
STEP — Prepare Markdown Stories for Instruction Tuning

Reads:
  Markdown file with "# " headers separating stories
Writes:
  <run.data_dir>/<output_jsonl>

Config (experiment.yaml):
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

# --- STEP-AWARE CONFIG -----------------------------------------------
STEP_NAME = process.env.STEP_NAME or 'prepare_outmd'
CFG_PATH  = process.env.CFG_OVERRIDE or path.join(process.cwd(), 'experiment.yaml')

cfgFull = yaml.load(fs.readFileSync(CFG_PATH, 'utf8'))
unless cfgFull?
  throw new Error "❌ Failed to load #{CFG_PATH}"

stepCfg = cfgFull[STEP_NAME]
throw new Error "Missing step config for '#{STEP_NAME}' in experiment.yaml" unless stepCfg?

runCfg  = cfgFull['run']
throw new Error "Missing global 'run' section in experiment.yaml" unless runCfg?

# --- Required parameters (NO defaults) -------------------------------
for key in ['output_dir','input_md','output_jsonl']
  unless stepCfg[key]? and String(stepCfg[key]).length
    throw new Error "Missing required key: #{STEP_NAME}.#{key} in experiment.yaml"

DATA_DIR = path.resolve(stepCfg.output_dir)
INPUT_MD = path.resolve(stepCfg.input_md)
OUTPUT_JSONL = path.resolve(DATA_DIR, stepCfg.output_jsonl)
LOG_DIR  = path.join(DATA_DIR, 'logs')

fs.mkdirSync(DATA_DIR, {recursive:true})
fs.mkdirSync(LOG_DIR, {recursive:true})

# --- Logging ----------------------------------------------------------
LOG_PATH = path.join(LOG_DIR, "#{STEP_NAME}.log")
log = (msg) ->
  stamp = new Date().toISOString().replace('T',' ').replace(/\.\d+Z$/,'')
  line  = "[#{stamp}] #{msg}"
  try fs.appendFileSync(LOG_PATH, line + os.EOL, 'utf8') catch e then null
  console.log line

# --- Template & Helpers ----------------------------------------------
PROMPT_TEMPLATE = """
You are St. John's Jim — a myth-weaving, bar-stool Buddha of the Pacific Northwest.
Tell a new short story in your usual voice. Base it on this seed:
"""

extract_snippets = (mdText) ->
  mdText.split('# ').map((chunk)-> chunk.trim()).filter (c)-> c.length > 0

format_as_alpaca = (chunk) ->
  {
    instruction: PROMPT_TEMPLATE + chunk.slice(0,200) + '...'
    input: ''
    output: chunk.trim()
  }

# --- Main -------------------------------------------------------------
main = ->
  log "Starting step: #{STEP_NAME}"
  log "Input: #{INPUT_MD}"
  log "Output: #{OUTPUT_JSONL}"

  unless fs.existsSync(INPUT_MD)
    throw new Error "Missing markdown source: #{INPUT_MD}"

  mdText = fs.readFileSync(INPUT_MD, 'utf8')
  chunks = extract_snippets(mdText)
  entries = (format_as_alpaca(c) for c in chunks)

  out = fs.createWriteStream(OUTPUT_JSONL, encoding:'utf8')
  count = 0
  for e in entries
    out.write(JSON.stringify(e) + '\n')
    count += 1
  out.end()

  log "[OK] Wrote #{count} entries → #{OUTPUT_JSONL}"

  # --- Memo save -----------------------------------------------------
  try
    if global.M? and typeof global.M.saveThis is 'function'
      global.M.saveThis "done:#{STEP_NAME}", true
      global.M.saveThis "#{STEP_NAME}:stats", {count, output: OUTPUT_JSONL}
  catch e
    log "(memo skip) #{e.message}"

  return count

main()