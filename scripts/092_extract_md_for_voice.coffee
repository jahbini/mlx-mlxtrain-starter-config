#!/usr/bin/env coffee
###
092_extract_md_for_voice.coffee
----------------------------------------
STEP — Extract Markdown Stories for Voice Fine-Tuning

Reads a Markdown file (headers "# " separate stories),
splits each story into paragraphs, and writes train/valid JSONL.

Outputs:
  - train.jsonl / valid.jsonl
  - contract.json / report.json / catalog.json
###

fs      = require 'fs'
path    = require 'path'
os      = require 'os'
crypto  = require 'crypto'
yaml    = require 'js-yaml'
seedrandom = require 'seedrandom'

process.env.NODE_NO_WARNINGS = 1

# -------------------------------------------------------------------
# 1) Load Config
# -------------------------------------------------------------------
STEP_NAME = process.env.STEP_NAME or 'extract_md_for_voice'
CFG_PATH  = process.env.CFG_OVERRIDE or path.join process.cwd(), 'experiment.yaml'

try
  CFG_FULL = yaml.load fs.readFileSync(CFG_PATH, 'utf8')
catch err
  console.error "⚠️ Could not load #{CFG_PATH}: #{err.message}"
  CFG_FULL = {}

RUN_CFG   = CFG_FULL?.run or {}
STEP_CFG  = CFG_FULL?[STEP_NAME] or {}
PARAMS    = STEP_CFG?.params or {}

# -------------------------------------------------------------------
# 2) Paths
# -------------------------------------------------------------------
RUN_DIR   = path.resolve PARAMS.run_dir or RUN_CFG.output_dir or 'run'
OUT_DIR   = path.resolve PARAMS.output_dir or path.join(RUN_DIR, 'data')
LOG_DIR   = path.join OUT_DIR, 'logs'
TRAIN_JSONL = path.join OUT_DIR, 'train.jsonl'
VALID_JSONL = path.join OUT_DIR, 'valid.jsonl'
CONTRACT_PATH = path.join OUT_DIR, 'contract.json'
REPORT_PATH   = path.join OUT_DIR, 'report.json'
CATALOG_PATH  = path.join OUT_DIR, 'catalog.json'

fs.mkdirSync OUT_DIR, {recursive:true}
fs.mkdirSync LOG_DIR, {recursive:true}

# -------------------------------------------------------------------
# 3) Logging
# -------------------------------------------------------------------
LOG_PATH = path.join LOG_DIR, "#{STEP_NAME}.log"
log = (msg) ->
  stamp = new Date().toISOString().replace('T',' ').replace('Z','')
  line  = "[#{stamp}] #{msg}"
  try fs.appendFileSync LOG_PATH, line + os.EOL, 'utf8' catch e then null
  console.log line

# -------------------------------------------------------------------
# 4) Parameters
# -------------------------------------------------------------------
INPUT_MD        = path.resolve PARAMS.input_md or 'your.md'
VALID_FRAC      = parseFloat PARAMS.valid_fraction or 0.1
MIN_STORY_WORDS = parseInt PARAMS.min_story_words or 50
SEED            = parseInt RUN_CFG.seed or 42

# -------------------------------------------------------------------
# 5) Helpers
# -------------------------------------------------------------------
normalize_ws = (s) ->
  s.replace(/\s*\n\s*/g,' ').replace(/ {2,}/g,' ').trim()

split_paragraphs = (s) ->
  (p.trim() for p in s.split(/\n{2,}/) when p.trim().length > 0)

ordinal_suffix = (n) ->
  if 10 <= n % 100 <= 20 then 'th' else {1:'st',2:'nd',3:'rd'}[n % 10] or 'th'

sha256_file = (p) ->
  crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex')

count_lines_bytes = (p) ->
  data = fs.readFileSync p
  lines = data.toString().split('\n').length - 1
  bytes = data.length
  [lines, bytes]

summarize_lengths = (p, field) ->
  lens = []
  for ln in fs.readFileSync(p,'utf8').split('\n')
    continue unless ln.trim()
    try
      obj = JSON.parse ln
      s = obj[field]
      lens.push s.length if typeof s is 'string'
    catch err then continue
  return {n:0} unless lens.length
  lens.sort (a,b)->a-b
  n = lens.length
  p95 = lens[Math.floor(0.95*(n-1))] or lens[n-1]
  {n, len_min:lens[0], len_med:lens[Math.floor(n/2)], len_95:p95, len_max:lens[n-1]}

extract_md_stories = (mdPath) ->
  stories = []
  currentTitle = null
  currentBody  = []
  for line in fs.readFileSync(mdPath,'utf8').split(/\r?\n/)
    line = line.trimEnd()
    if line.startsWith '# '
      if currentTitle and currentBody.length
        stories.push [currentTitle, currentBody.join('\n').trim()]
      currentTitle = line.slice(2).trim()
      currentBody = []
    else if currentTitle
      currentBody.push line
  if currentTitle and currentBody.length
    stories.push [currentTitle, currentBody.join('\n').trim()]
  stories

# -------------------------------------------------------------------
# 6) Main
# -------------------------------------------------------------------
unless fs.existsSync INPUT_MD
  log "[FATAL] Markdown input not found: #{INPUT_MD}"
  process.exit 1

log "[INFO] Step #{STEP_NAME} starting"
log "[INFO] Input Markdown: #{INPUT_MD}"
log "[INFO] Output dir: #{OUT_DIR}"
log "[INFO] seed=#{SEED} valid_fraction=#{VALID_FRAC} min_story_words=#{MIN_STORY_WORDS}"

stories = extract_md_stories INPUT_MD
examples = []

for idx,story of stories
  [title,text] = story
  continue unless text
  continue if text.split(/\s+/).length < MIN_STORY_WORDS
  paragraphs = split_paragraphs text
  for i,para of paragraphs
    n = i + 1
    examples.push
      meta:
        doc_id: "story-#{idx}"
        title: title
        paragraph_index: n
      prompt: para + "\n\n"
      completion: ""

log "[INFO] Extracted #{examples.length} examples"

rng = seedrandom(SEED)
examples.sort -> rng() - 0.5

n_valid = Math.max 1, Math.floor(examples.length * VALID_FRAC)
valid = examples.slice 0, n_valid
train = examples.slice n_valid

write_jsonl = (fn, arr) ->
  out = fs.createWriteStream fn, encoding:'utf8'
  for ex in arr
    out.write JSON.stringify(ex) + '\n'
  out.end()

write_jsonl TRAIN_JSONL, train
write_jsonl VALID_JSONL, valid
log "[OK] Wrote #{TRAIN_JSONL} (#{train.length})"
log "[OK] Wrote #{VALID_JSONL} (#{valid.length})"

# -------------------------------------------------------------------
# 7) Contract / Report / Catalog
# -------------------------------------------------------------------
probeLine = fs.readFileSync(TRAIN_JSONL,'utf8').split('\n').find((l)->l.trim()) or '{}'
probe = JSON.parse probeLine

mode = if 'prompt' of probe and 'completion' of probe then 'sft' else 'plain'
target_field = if mode is 'sft' then 'completion' else 'text'
schema_fields = if mode is 'sft' then {prompt:'string',completion:'string'} else {text:'string'}

created = new Date().toISOString().replace('T',' ').replace('Z','')
[t_lines, t_bytes] = count_lines_bytes TRAIN_JSONL
[v_lines, v_bytes] = count_lines_bytes VALID_JSONL

contract =
  created_utc: created
  data_dir: OUT_DIR
  filenames:
    train: {chosen:path.basename(TRAIN_JSONL), resolved:TRAIN_JSONL}
    valid: {chosen:path.basename(VALID_JSONL), resolved:VALID_JSONL}
  schema: {format:'jsonl', fields:schema_fields}
  source: {mode, target_field, origin:'markdown_file'}

fs.writeFileSync CONTRACT_PATH, JSON.stringify(contract,null,2)

report =
  created_utc: created
  counts: {train:t_lines, valid:v_lines}
  train_stats: summarize_lengths TRAIN_JSONL, target_field
  valid_stats: summarize_lengths VALID_JSONL, target_field
  target_field: target_field
  schema_mode: mode

fs.writeFileSync REPORT_PATH, JSON.stringify(report,null,2)

catalog =
  created_utc: created
  data_dir: OUT_DIR
  mode: mode
  target_field: target_field
  schema: schema_fields
  total_examples: {train:t_lines, valid:v_lines}
  files:
    train:
      path: TRAIN_JSONL
      lines: t_lines
      bytes: t_bytes
      sha256: sha256_file TRAIN_JSONL
    valid:
      path: VALID_JSONL
      lines: v_lines
      bytes: v_bytes
      sha256: sha256_file VALID_JSONL
  checksums:
    contract: sha256_file CONTRACT_PATH
    report:   sha256_file REPORT_PATH

fs.writeFileSync CATALOG_PATH, JSON.stringify(catalog,null,2)
log "[OK] Wrote contract/catalog/report"

try
  if global.M? and typeof global.M.saveThis is 'function'
    global.M.saveThis "done:#{STEP_NAME}", true
    global.M.saveThis "#{STEP_NAME}:counts", {train:t_lines, valid:v_lines}
catch e
  log "(memo skip) #{e.message}"

log "[INFO] Completed step #{STEP_NAME} successfully"
process.exit 0