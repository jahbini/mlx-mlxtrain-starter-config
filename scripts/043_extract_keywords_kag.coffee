#!/usr/bin/env coffee
###
043_extract_keywords_kag.coffee
----------------------------------------
STEP — Extract semantic keywords for KAG fine-tuning

Reads out_kag.jsonl (prompt/response pairs)
and adds auto-generated tags for Emotion, Location, and Character.

Outputs:
  run/data/out_kag_keywords.jsonl

Each entry:
  { "prompt": "...", "response": "...", "tags": ["#Joy", "#Forest", "#Jim"] }
###

fs   = require 'fs'
path = require 'path'
os   = require 'os'
process.env.NODE_NO_WARNINGS = 1

# --- Config loader ---
{load_config} = require './config_loader'
CFG       = load_config()
STEP_NAME = process.env.STEP_NAME or 'extract_keywords_kag'
STEP_CFG  = CFG.pipeline.steps?[STEP_NAME] or {}
PARAMS    = STEP_CFG.params or {}

# --- Paths ---
DATA_DIR  = path.resolve PARAMS.output_dir or CFG.data.output_dir
LOG_DIR   = path.join DATA_DIR, 'logs'
INPUT_JSONL  = path.join DATA_DIR, PARAMS.input_jsonl or 'out_kag.jsonl'
OUTPUT_JSONL = path.join DATA_DIR, PARAMS.output_jsonl or 'out_kag_keywords.jsonl'

fs.mkdirSync DATA_DIR, {recursive: true}
fs.mkdirSync LOG_DIR,  {recursive: true}

logPath = path.join LOG_DIR, "#{STEP_NAME}.log"
log = (msg) ->
  stamp = new Date().toISOString().replace('T',' ').replace('Z','')
  fs.appendFileSync logPath, "[#{stamp}] #{msg}" + os.EOL, 'utf8'
  console.log "[#{stamp}] #{msg}"

# --- Simple Keyword Tables ---
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
  your:       /\b(your|st.? john.?s your)\b/i
  friend:    /\b(friend|buddy|pal|companion|stranger)\b/i
  woman:     /\b(woman|lady|girl|mother|daughter|queen)\b/i
  man:       /\b(man|boy|father|son|king)\b/i
  spirit:    /\b(spirit|ghost|angel|soul|god|goddess)\b/i

# --- Helper to match tags ---
detect_tags = (text) ->
  tags = []
  for [k,re] in Object.entries EMOTION_WORDS when re.test text
    tags.push "#" + k[0].toUpperCase() + k.slice(1)
  for [k,re] in Object.entries LOCATION_WORDS when re.test text
    tags.push "#" + k[0].toUpperCase() + k.slice(1)
  for [k,re] in Object.entries CHARACTER_WORDS when re.test text
    tags.push "#" + k[0].toUpperCase() + k.slice(1)
  tags

# --- Main ---
main = ->
  log "Starting step: #{STEP_NAME}"
  unless fs.existsSync INPUT_JSONL
    log "[FATAL] Missing input file: #{INPUT_JSONL}"
    process.exit 1

  lines = fs.readFileSync(INPUT_JSONL, 'utf8').split('\n').filter (l) -> l.trim().length > 0
  out   = fs.createWriteStream OUTPUT_JSONL, encoding:'utf8'

  total = 0
  for line in lines
    try
      obj = JSON.parse line
      text = (obj.prompt or '') + '\n' + (obj.response or '')
      tags = detect_tags text
      obj.tags = tags
      out.write JSON.stringify(obj) + '\n'
      total += 1
    catch err
      log "[WARN] JSON parse failed on line #{total}: #{err.message}"

  out.end()
  log "[OK] Wrote #{total} entries to #{OUTPUT_JSONL}"
  log "Completed successfully."
  process.exit 0

main()
