###
  02_prepare_data.coffee
  -----------------------
  Direct CoffeeScript port of 02_prepare_data.py
  ✅ Could later be replaced by a declarative YAML "prepare_data" step.

  Function:
    - Reads data_contract.json from data_dir
    - Validates JSONL files (train/valid)
    - Computes character class, length, EOS marker stats
    - Detects duplicates
    - Writes data_report.json
###

fs        = require 'fs'
path      = require 'path'
yaml      = require 'js-yaml'
crypto    = require 'crypto'
os        = require 'os'

# --- Config ---
CFG_PATH  = process.env.CFG_OVERRIDE or path.join(process.cwd(), 'experiment.yaml')
STEP_NAME = process.env.STEP_NAME or 'prepare_data'
cfgFull   = yaml.load(fs.readFileSync(CFG_PATH, 'utf8'))
STEP_CFG  = cfgFull[STEP_NAME] or {}
RUN_CFG   = cfgFull['run'] or {}

DATA_DIR  = path.resolve(RUN_CFG.data_dir or 'data')
fs.mkdirSync(DATA_DIR, {recursive:true})

CONTRACT  = path.join(DATA_DIR, RUN_CFG.contract or 'data_contract.json')
REPORT    = path.join(DATA_DIR, RUN_CFG.report   or 'data_report.json')

# --- Constants ---
EOS_MARKERS = [
  '</s>', '###', '\n\n', '<|eot_id|>', '<|endoftext|>'
]

# --- Helpers ---
readJSON = (p) ->
  JSON.parse(fs.readFileSync(p, 'utf8'))

hashText = (s) ->
  crypto.createHash('sha256').update(String(s), 'utf8').digest('hex')

charClasses = (s) ->
  ctrl = 0; ws = 0; nonascii = 0
  for ch in s
    code = ch.charCodeAt(0)
    if code <= 31 or code is 127 then ctrl++
    if /\s/.test(ch) then ws++
    if code > 127 then nonascii++
  {control: ctrl, whitespace: ws, non_ascii: nonascii}

percentiles = (values, q=[5,25,50,75,95]) ->
  return (("p#{p}": 0) for p in q) unless values?.length
  vals = values.slice().sort((a,b)->a-b)
  out = {}
  for p in q
    k = Math.max(0, Math.min(vals.length-1, Math.round((p/100)*(vals.length-1))))
    out["p#{p}"] = vals[k]
  out

loadContract = (contractPath) ->
  c = readJSON(contractPath)
  data_dir = c.data_dir
  fields = c?.schema?.fields or {}
  text_field = null
  for k,v of fields
    if String(v).toLowerCase() is 'string'
      text_field = k
      break
  text_field ?= 'text'
  files = {}
  for split, info of c.filenames when info?.resolved?
    files[split] = info.resolved
  [text_field, files, data_dir]

# --- Scan a file ---
scanFile = (filePath, field) ->
  n_lines = bad_json = missing_field = non_str = 0
  empty = whitespace_only = leading_ws = trailing_ws = ctrl_lines = 0
  lengths = []; hashes = []
  eos_hits = {}; eos_hits[m] = 0 for m in EOS_MARKERS
  samples_good = []; samples_bad = []

  data = fs.readFileSync(filePath, 'utf8').split(/\r?\n/)
  for line in data when line.length
    n_lines++
    line = line.replace(/\r$/, '')
    obj = null
    try
      obj = JSON.parse(line)
    catch e
      bad_json++
      if samples_bad.length < 3
        samples_bad.push "[bad_json] #{line.slice(0,160)}"
      continue

    unless field of obj
      missing_field++
      if samples_bad.length < 3
        samples_bad.push "[missing_field] #{line.slice(0,160)}"
      continue

    val = obj[field]
    unless typeof val is 'string'
      non_str++
      if samples_bad.length < 3
        samples_bad.push "[non_string] #{String(val).slice(0,160)}"
      continue

    if val is '' then empty++
    if val.trim() is '' then whitespace_only++
    if val[0]?.match(/\s/) then leading_ws++
    if val[val.length-1]?.match(/\s/) then trailing_ws++

    cc = charClasses(val)
    if cc.control > 0 then ctrl_lines++

    L = val.length
    lengths.push L
    hashes.push hashText(val)
    for m in EOS_MARKERS
      if val.includes(m) then eos_hits[m] += 1

    if samples_good.length < 3
      samples_good.push val

  # duplicates
  counts = {}
  dup_count = 0; dup_examples = []
  for h in hashes
    counts[h] ?= 0
    counts[h] += 1
  for h, cnt of counts when cnt > 1
    dup_count += cnt - 1
    if dup_examples.length < 3
      dup_examples.push h

  length_stats =
    count: lengths.length
    min: if lengths.length then Math.min.apply(null, lengths) else 0
    max: if lengths.length then Math.max.apply(null, lengths) else 0
    mean: if lengths.length then lengths.reduce((a,b)->a+b)/lengths.length else 0
    median: if lengths.length then lengths.slice().sort((a,b)->a-b)[Math.floor(lengths.length/2)] else 0
    percentiles: percentiles(lengths)

  {
    path: filePath
    lines: n_lines
    valid_examples: lengths.length
    errors:
      bad_json: bad_json
      missing_field: missing_field
      non_string_field: non_str
    empties:
      empty_exact: empty
      whitespace_only: whitespace_only
      leading_whitespace: leading_ws
      trailing_whitespace: trailing_ws
    control_char_lines: ctrl_lines
    duplicates:
      duplicate_example_count: dup_count
      sha256_examples: dup_examples
    length_chars: length_stats
    eos_markers_hits: eos_hits
    samples:
      good_first3: samples_good
      bad_first3: samples_bad
  }

# --- Main ---
[text_field, files, data_dir] = loadContract(CONTRACT)
created = new Date().toISOString().replace(/\.\d+Z$/, 'Z')
report =
  created_utc: created
  data_dir: data_dir
  text_field: text_field
  splits: {}

for split, filePath of files
  rep = scanFile(filePath, text_field)
  report.splits[split] = rep

fs.writeFileSync(REPORT, JSON.stringify(report, null, 2), 'utf8')

# --- Console summary ---
console.log "=== DATA VALIDATION SUMMARY ==="
for split, rep of report.splits
  errs = rep.errors; empt = rep.empties; lens = rep.length_chars
  eos = rep.eos_markers_hits; dup = rep.duplicates.duplicate_example_count
  eos_summary = (k+":"+v for k,v of eos when v).join(", ")
  console.log "- #{split}: lines=#{rep.lines} valid=#{rep.valid_examples} " +
    "errors(bad/miss/nonstr)=#{errs.bad_json}/#{errs.missing_field}/#{errs.non_string_field} " +
    "empties(exact/ws/lead/trail)=#{empt.empty_exact}/#{empt.whitespace_only}/#{empt.leading_whitespace}/#{empt.trailing_whitespace} " +
    "dupes=#{dup} len[min/med/95/max]=#{lens.min}/#{Math.floor(lens.median)}/#{lens.percentiles.p95}/#{lens.max} " +
    "eos_hits={#{eos_summary}}"

console.log "Wrote:", REPORT

# Optional memo save
try
  if global.M? and typeof global.M.saveThis is 'function'
    global.M.saveThis 'data_report.json', report
catch e
  console.warn "(memo skip)", e.message