#!/usr/bin/env coffee
###
043_extract_keywords_kag.coffee
----------------------------------------
STEP — Extract semantic keywords for KAG fine-tuning

Reads a JSONL of {prompt, response} pairs and appends auto-generated
tags for Emotion, Location, and Character.

Outputs:
  <run.data_dir>/<output_jsonl>  (default: out_kag_keywords.jsonl)

Config (flat step entry in experiment.yaml):
  extract_keywords_kag:
    run: scripts/043_extract_keywords_kag.coffee
    depends_on: [prepare_kag]          # or your previous step name
    input_jsonl: out_kag.jsonl
    output_jsonl: out_kag_keywords.jsonl
    output_dir: ""                     # defaults to run.data_dir if empty
    log_dir: ""                        # defaults to run.data_dir/logs if empty
###

fs   = require 'fs'
path = require 'path'
os   = require 'os'
yaml = require 'js-yaml'

process.env.NODE_NO_WARNINGS = 1

# ----------------------------
# Config Loader (flat model)
# ----------------------------
CFG_PATH  = process.env.CFG_OVERRIDE or path.join(process.cwd(), 'experiment.yaml')
STEP_NAME = process.env.STEP_NAME or 'extract_keywords_kag'

try
  CFG_FULL = yaml.load(fs.readFileSync(CFG_PATH, 'utf8')) or {}
catch e
  console.error "❌ Failed to read config at #{CFG_PATH}: #{e.message}"
  process.exit 1

RUN_CFG  = CFG_FULL['run'] or {}
STEP_CFG = CFG_FULL[STEP_NAME] or {}

DATA_DIR = path.resolve(STEP_CFG.output_dir or RUN_CFG.data_dir or 'run/data')
LOG_DIR  = path.resolve(STEP_CFG.log_dir    or path.join(DATA_DIR, 'logs'))

INPUT_JSONL  = path.resolve DATA_DIR, (STEP_CFG.input_jsonl  or 'out_kag.jsonl')
OUTPUT_JSONL = path.resolve DATA_DIR, (STEP_CFG.output_jsonl or 'out_kag_keywords.jsonl')

# Ensure dirs
for d in [DATA_DIR, LOG_DIR]
  try
    fs.mkdirSync d, {recursive:true}
  catch e
    console.error "❌ Could not create directory #{d}: #{e.message}"
    process.exit 1

# ----------------------------
# Logging
# ----------------------------
LOG_PATH = path.join LOG_DIR, "#{STEP_NAME}.log"
log = (msg) ->
  stamp = new Date().toISOString().replace('T',' ').replace('Z','')
  line  = "[#{stamp}] #{msg}"
  try fs.appendFileSync LOG_PATH, line + os.EOL, 'utf8' catch e then null
  console.log line

# ----------------------------
# Keyword Tables (regex)
# ----------------------------
EMOTION_WORDS =
  joy:       /\b(happy|joy|delight|smile|laugh|hope|love|bliss)\b/i
  sorrow:    /\b(sad|grief|lonely|weep|cry|loss|mourning)\b/i
  anger:     /\b(angry|rage|fury|mad|hate|irritate)\b/i
  fear:      /\b(fear|terror|afraid|panic|horror|scared)\b/i
  wonder:    /\b(awe|wonder|mystery|curious|dream|magic|miracle)\b/i

LOCATION_WORDS =
  sea:       /\b(sea|ocean|bay|shore|beach|wave|tide|harbor)\b/i
  forest:    /\b(forest|woods|tree|grove|pine|oak|maple|fern)\b/i
  mountain:  /\b(mountain|hill|peak|ridge|valley|cliff)\b/i
  city:      /\b(city|street|alley|building|market|cafe|bar)\b/i
  sky:       /\b(sky|cloud|sun|moon|star|wind|rain)\b/i

CHARACTER_WORDS =
  your:      /\b(your)\b/i
  friend:    /\b(friend|buddy|pal|companion|stranger)\b/i
  woman:     /\b(woman|lady|girl|mother|daughter|queen)\b/i
  man:       /\b(man|boy|father|son|king)\b/i
  spirit:    /\b(spirit|ghost|angel|soul|god|goddess)\b/i

# ----------------------------
# Helpers
# ----------------------------
titleTag = (k) -> "##{k[0].toUpperCase()}#{k.slice(1)}".replace(/^##/,'#')

detect_tags = (text) ->
  tags = []
  for [k,re] in Object.entries EMOTION_WORDS when re.test text
    tags.push titleTag k
  for [k,re] in Object.entries LOCATION_WORDS when re.test text
    tags.push titleTag k
  for [k,re] in Object.entries CHARACTER_WORDS when re.test text
    tags.push titleTag k
  # de-dup while preserving order
  seen = new Set()
  uniq = []
  for t in tags
    continue if seen.has t
    seen.add t
    uniq.push t
  uniq

readLines = (p) ->
  try
    fs.readFileSync(p, 'utf8').split(/\r?\n/).filter (l)-> l.trim().length>0
  catch e
    log "[FATAL] Cannot read #{p}: #{e.message}"
    process.exit 1

safeJSON = (l) ->
  try
    JSON.parse l
  catch err
    null

# ----------------------------
# Main
# ----------------------------
main = ->
  log "Starting step: #{STEP_NAME}"
  log "CFG: #{CFG_PATH}"
  log "DATA_DIR: #{DATA_DIR}"
  log "INPUT: #{INPUT_JSONL}"
  log "OUTPUT: #{OUTPUT_JSONL}"

  unless fs.existsSync INPUT_JSONL
    log "[FATAL] Missing input file: #{INPUT_JSONL}"
    process.exit 1

  lines = readLines INPUT_JSONL
  out   = fs.createWriteStream OUTPUT_JSONL, encoding:'utf8'
  total = 0
  bad   = 0

  for line, idx in lines
    obj = safeJSON line
    unless obj?
      bad += 1
      if bad <= 5 then log "[WARN] JSON parse failed on line #{idx+1}"
      continue

    text = "#{obj.prompt or ''}\n#{obj.response or ''}"
    tags = detect_tags text
    obj.tags = tags
    try
      out.write JSON.stringify(obj) + '\n'
      total += 1
    catch e
      log "[WARN] Failed to write output at line #{idx+1}: #{e.message}"

  out.end()

  log "[OK] Wrote #{total} tagged entries to #{OUTPUT_JSONL} (#{bad} bad input lines skipped)"
  # Optional memo signal for the runner / dashboards
  try
    if global.M? and typeof global.M.saveThis is 'function'
      global.M.saveThis "done:#{STEP_NAME}", true
      global.M.saveThis "#{STEP_NAME}:stats", {total, bad, output: OUTPUT_JSONL}
  catch e
    log "(memo skip) #{e.message}"

  process.exit 0

main()