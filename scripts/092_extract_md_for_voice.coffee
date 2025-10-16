#!/usr/bin/env coffee
###
092_extract_md_for_voice.coffee
----------------------------------------
STEP — Extract Markdown Stories for Voice Fine-Tuning

Reads a Markdown file with "# " headers dividing stories,
splits each story into paragraphs, and writes train/valid JSONL.

All paths and parameters come from config (default + override).
Deterministic, reproducible, and pipeline-friendly.
###

fs   = require 'fs'
path = require 'path'
os   = require 'os'
crypto = require 'crypto'
process.env.NODE_NO_WARNINGS = 1

# --- 1) Config Loader ---
{load_config} = require './config_loader'
CFG       = load_config()
STEP_NAME = process.env.STEP_NAME or 'extract_md_for_voice'
STEP_CFG  = CFG.pipeline.steps?[STEP_NAME] or {}
PARAMS    = STEP_CFG.params or {}

# --- 2) Directories ---
ROOT    = path.resolve process.env.EXEC or path.dirname(__filename)
OUT_DIR = path.resolve PARAMS.output_dir or CFG.data.output_dir
LOG_DIR = path.join OUT_DIR, 'logs'
fs.mkdirSync OUT_DIR, {recursive: true}
fs.mkdirSync LOG_DIR, {recursive: true}

logPath = path.join LOG_DIR, "#{STEP_NAME}.log"
log = (msg) ->
  stamp = new Date().toISOString().replace('T',' ').replace('Z','')
  line  = "[#{stamp}] #{msg}"
  fs.appendFileSync logPath, line + os.EOL, 'utf8'
  console.log line

# --- 3) Parameters ---
INPUT_MD        = path.resolve PARAMS.input_md or 'your.md'
SEED            = parseInt PARAMS.seed or CFG.run?.seed or 42
VALID_FRAC      = parseFloat PARAMS.valid_fraction or CFG.web?.valid_fraction or 0.1
MIN_STORY_WORDS = parseInt PARAMS.min_story_words or CFG.web?.min_story_words or 50

CONTRACT_PATH = path.join OUT_DIR, CFG.data?.contract or 'data_contract.json'
CATALOG_PATH  = path.join OUT_DIR, CFG.data?.catalog  or 'data_catalog.json'
REPORT_PATH   = path.join OUT_DIR, CFG.data?.report   or 'data_report.json'
TRAIN_JSONL   = path.join OUT_DIR, 'train.jsonl'
VALID_JSONL   = path.join OUT_DIR, 'valid.jsonl'

# --- 4) Validation ---
unless fs.existsSync INPUT_MD
  log "[FATAL] Markdown input not found: #{INPUT_MD}"
  process.exit 1

log "[INFO] Step #{STEP_NAME} starting"
log "[INFO] Input Markdown: #{INPUT_MD}"
log "[INFO] Output dir: #{OUT_DIR}"
log "[INFO] seed=#{SEED} valid_fraction=#{VALID_FRAC} min_story_words=#{MIN_STORY_WORDS}"

# --- 5) Helpers ---
normalize_ws = (s) ->
  s.replace(/\s*\n\s*/g,' ').replace(/ {2,}/g,' ').trim()

split_paragraphs = (s) ->
  (p.trim() for p in s.split(/\n{2,}/) when p.trim().length > 0)

ordinal_suffix = (n) ->
  if 10 <= n % 100 <= 20 then 'th' else {1:'st',2:'nd',3:'rd'}[n % 10] or 'th'

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

sha256_file = (p) ->
  data = fs.readFileSync p
  crypto.createHash('sha256').update(data).digest('hex')

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

# --- 6) Main Dataset Logic ---
stories = extract_md_stories INPUT_MD
examples = []

for story_id, [title,text] of stories.entries()
  continue if text.split(/\s+/).length < MIN_STORY_WORDS
  paragraphs = split_paragraphs text
  for i,para of paragraphs
    n = i + 1
    prompt = "#{para}\n\n"
    examples.push
      meta:
        doc_id: "story-#{story_id}"
        title: title
        paragraph_index: n
      prompt: prompt
      completion: ""

log "[INFO] Extracted #{examples.length} examples"

# Shuffle (deterministic)
rng = require('seedrandom')(SEED)
examples.sort -> rng() - 0.5

n_valid = Math.max 1, Math.floor(examples.length * VALID_FRAC)
valid = examples.slice 0, n_valid
train = examples.slice n_valid

write_jsonl = (filename, arr) ->
  out = fs.createWriteStream filename, encoding:'utf8'
  for ex in arr
    out.write JSON.stringify(ex) + '\n'
  out.end()

write_jsonl TRAIN_JSONL, train
write_jsonl VALID_JSONL, valid
log "[INFO] Wrote #{TRAIN_JSONL} (#{train.length}), #{VALID_JSONL} (#{valid.length})"

# --- 7) Metadata Files ---
probe = JSON.parse fs.readFileSync(TRAIN_JSONL,'utf8').split('\n').find((l)->l.trim()) or '{}'
if 'prompt' of probe and 'completion' of probe
  mode = 'sft'
  target_field = 'completion'
  schema_fields = prompt:'string', completion:'string'
else if 'text' of probe
  mode = 'plain'
  target_field = 'text'
  schema_fields = text:'string'
else
  log "[FATAL] Could not infer dataset schema."
  process.exit 2

created = new Date().toISOString().replace('T',' ').replace('Z','')
[t_lines, t_bytes] = count_lines_bytes TRAIN_JSONL
[v_lines, v_bytes] = count_lines_bytes VALID_JSONL

contract =
  created_utc: created
  data_dir: OUT_DIR
  filenames:
    train: chosen:path.basename(TRAIN_JSONL), resolved:TRAIN_JSONL
    valid: chosen:path.basename(VALID_JSONL), resolved:VALID_JSONL
  schema:
    format: 'jsonl'
    fields: schema_fields
  source:
    mode: mode
    target_field: target_field
    origin: 'markdown_file'

catalog =
  created_utc: created
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

report =
  created_utc: created
  counts: train:t_lines, valid:v_lines
  train_stats: summarize_lengths TRAIN_JSONL, target_field
  valid_stats: summarize_lengths VALID_JSONL, target_field
  target_field: target_field
  schema_mode: mode

fs.writeFileSync CONTRACT_PATH, JSON.stringify(contract,null,2)
fs.writeFileSync CATALOG_PATH,  JSON.stringify(catalog,null,2)
fs.writeFileSync REPORT_PATH,   JSON.stringify(report,null,2)

log "[INFO] Wrote contract/catalog/report to #{OUT_DIR}"
log "[INFO] Completed step #{STEP_NAME} successfully"
process.exit 0
