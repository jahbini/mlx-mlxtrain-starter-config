###
  01_fetch_hf_dataset.coffee
  --------------------------
  Direct CoffeeScript port of 01_fetch_hf_dataset.py
  ✅ Could later be replaced by a declarative YAML "fetch_dataset" step.

  Function:
    - Loads a HuggingFace dataset (train split)
    - Filters by word count, dedupes, splits train/valid
    - Writes train.jsonl / valid.jsonl
    - Generates data_contract.json and data_catalog.json
###

fs        = require 'fs'
path      = require 'path'
yaml      = require 'js-yaml'
crypto    = require 'crypto'
os        = require 'os'
child     = require 'child_process'

# --- Config ---
CFG_PATH  = process.env.CFG_OVERRIDE or path.join(process.cwd(), 'experiment.yaml')
STEP_NAME = process.env.STEP_NAME or 'fetch_dataset'
cfgFull   = yaml.load(fs.readFileSync(CFG_PATH, 'utf8'))
STEP_CFG  = cfgFull[STEP_NAME] or {}
RUN_CFG   = cfgFull['run'] or {}

DATA_DIR  = path.resolve(RUN_CFG.data_dir or 'data')
CONTRACT  = path.join(DATA_DIR, RUN_CFG.contract or 'data_contract.json')
CATALOG   = path.join(DATA_DIR, RUN_CFG.catalog  or 'data_catalog.json')

HF_DATASET  = STEP_CFG.hf_dataset or 'unknown'
SUBSET      = STEP_CFG.subset or null
MODE        = STEP_CFG.mode or 'plain'
VALID_FRACT = STEP_CFG.valid_fract or 0.1
MIN_WORDS   = STEP_CFG.min_words or 1
MAX_WORDS   = STEP_CFG.max_words or 1000
SEED        = STEP_CFG.seed or 1234

fs.mkdirSync(DATA_DIR, {recursive:true})
console.log "Dataset:", HF_DATASET
console.log "Subset:", SUBSET
console.log "Mode:", MODE
console.log "Valid fraction:", VALID_FRACT
console.log "Seed:", SEED

# --- Helpers ---
rand = require 'seedrandom'
rng = rand(SEED)
Math.random = rng

wc = (s) -> String(s).split(/\s+/).length
sha = (s) -> crypto.createHash('sha256').update(String(s)).digest('hex')

timestampUTC = -> new Date().toISOString().replace(/\.\d+Z$/, 'Z')

writeJSONL = (file, arr) ->
  fs.writeFileSync(file, '', 'utf8')
  fout = fs.createWriteStream(file, {flags:'a', encoding:'utf8'})
  for t in arr
    fout.write JSON.stringify({text:t}) + "\n"
  fout.close()

countLinesBytes = (p) ->
  data = fs.readFileSync(p)
  n = (data.toString('utf8').match(/\n/g) or []).length
  [n, data.length]

sha256File = (p) ->
  crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex')

# --- Dataset fetch via HF CLI ---
main = ->
  console.log "📦 Loading #{HF_DATASET} subset=#{SUBSET or 'none'} …"

  # we spawn a python inline call for HF datasets since MLX not needed here
  script = """
from datasets import load_dataset
import json, sys
HF_DATASET = #{JSON.stringify(HF_DATASET)}
SUBSET     = #{JSON.stringify(SUBSET)}
ds = load_dataset(HF_DATASET, name=SUBSET, split='train')
for r in ds:
  print(json.dumps(r))
"""
  res = child.spawnSync('python', ['-u', '-c', script], {encoding:'utf8'})
  if res.error? or res.status isnt 0
    console.error "❌ datasets.load_dataset failed"
    console.error res.stderr
    process.exit(1)

  lines = res.stdout.trim().split(/\r?\n/)
  rawRows = []
  for l in lines when l.trim().length
    try rawRows.push JSON.parse(l)
    catch e then console.warn "⚠️ bad JSON row", e.message

  console.log "Fetched #{rawRows.length} records"

  rows = []
  for r in rawRows
    quote  = (r.quote or '').trim()
    author = (r.author or '').trim()
    continue unless quote.length
    text = if MODE is 'plain'
      quote
    else
      instr = if author then "Write a short motivational quote in the style of #{author}." else "Write a short motivational quote."
      "Instruction:\n#{instr}\n\nResponse:\n#{quote}"
    continue unless MIN_WORDS <= wc(text) <= MAX_WORDS
    rows.push text

  # Deduplicate preserving order
  seen = new Set()
  uniq = []
  for t in rows
    h = sha(t)
    unless seen.has(h)
      seen.add(h)
      uniq.push(t)

  # Shuffle + split
  uniq.sort -> rng() - 0.5
  valid_n = Math.max(100, Math.floor(uniq.length * VALID_FRACT))
  valid = uniq.slice(0, valid_n)
  train = uniq.slice(valid_n)

  trainPath = path.join(DATA_DIR, 'train.jsonl')
  validPath = path.join(DATA_DIR, 'valid.jsonl')

  writeJSONL(trainPath, train)
  writeJSONL(validPath, valid)
  console.log "✅ Wrote #{train.length} train, #{valid.length} valid to #{DATA_DIR}"

  created = timestampUTC()

  data_contract =
    created_utc: created
    data_dir: DATA_DIR
    filenames:
      train: { chosen: path.basename(trainPath), resolved: path.resolve(trainPath) }
      valid: { chosen: path.basename(validPath), resolved: path.resolve(validPath) }
    schema:
      format: 'jsonl'
      fields: { text: 'string' }

  fs.writeFileSync(CONTRACT, JSON.stringify(data_contract, null, 2), 'utf8')

  [t_lines, t_bytes] = countLinesBytes(trainPath)
  [v_lines, v_bytes] = countLinesBytes(validPath)
  t_sha = sha256File(trainPath)
  v_sha = sha256File(validPath)

  data_catalog =
    created_utc: created
    files:
      train: { path: path.resolve(trainPath), lines: t_lines, bytes: t_bytes, sha256: t_sha }
      valid: { path: path.resolve(validPath), lines: v_lines, bytes: v_bytes, sha256: v_sha }
    entries:
      train:
        path: path.resolve(trainPath)
        stats:
          num_valid_examples: t_lines
          num_bytes: t_bytes
          sha256: t_sha
      valid:
        path: path.resolve(validPath)
        stats:
          num_valid_examples: v_lines
          num_bytes: v_bytes
          sha256: v_sha

  fs.writeFileSync(CATALOG, JSON.stringify(data_catalog, null, 2), 'utf8')

  console.log "📗 Wrote data_contract.json and data_catalog.json"

  # Optional memo save
  try
    if global.M? and typeof global.M.saveThis is 'function'
      global.M.saveThis 'data_contract.json', data_contract
      global.M.saveThis 'data_catalog.json', data_catalog
  catch e
    console.warn "(memo skip)", e.message

main()