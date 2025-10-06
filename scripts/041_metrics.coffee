###
  scripts/11_eos_analysis.coffee
  STEP 11 — EOS Behavior Probe & Quick Analysis (JSONL-first)
  Direct CoffeeScript port of the Python version.
###

fs   = require 'fs'
path = require 'path'
yaml = require 'js-yaml'
d3   = require 'd3-dsv'    # for CSV parsing/writing

# --- Config loader ---
{ load_config } = require '../config_loader'

# --- STEP-AWARE CONFIG ---
CFG       = load_config()
STEP_NAME = process.env.STEP_NAME
STEP_CFG  = CFG.pipeline.steps[STEP_NAME]
PARAMS    = STEP_CFG?.params or {}

# Resolve paths (params > global cfg)
OUT_DIR   = path.resolve PARAMS.output_dir   or CFG.data.output_dir
EVAL_DIR  = path.resolve PARAMS.eval_output_dir or CFG.eval.output_dir
RUN_DIR   = path.resolve PARAMS.run_dir      or CFG.run.output_dir

CONTRACT  = path.join OUT_DIR,   PARAMS.contract    or CFG.data.contract
GEN_JSONL = path.join EVAL_DIR, (PARAMS.generations or CFG.eval.generations) + ".jsonl"
GEN_CSV   = path.join EVAL_DIR, (PARAMS.generations or CFG.eval.generations) + ".csv"
OUT_SUM   = path.join EVAL_DIR, (PARAMS.summary     or CFG.eval.summary)     + ".csv"
OUT_JSON  = path.join EVAL_DIR, (PARAMS.analysis    or CFG.eval.analysis)    + ".json"

# --- Safety checks ---
unless fs.existsSync GEN_JSONL
  console.error "Missing eval_out/generations.jsonl (run Step 10)."
  process.exit 1

unless fs.existsSync CONTRACT
  console.error "Missing data_contract.json (from Step 2)."
  process.exit 1

# ---- Load generations from JSONL (authoritative) ----
rows = []
for line in fs.readFileSync(GEN_JSONL, 'utf8').split /\r?\n/
  continue unless line.trim()
  rows.push JSON.parse line

# ---- (Optional) CSV diagnostics ----
csv_missing = []
if fs.existsSync GEN_CSV
  csv_text = fs.readFileSync GEN_CSV, 'utf8'
  df_csv   = d3.csvParse csv_text
  if df_csv.length isnt rows.length
    # crude symmetric diff by JSON stringify
    seen = new Set(rows.map (r) -> JSON.stringify r)
    for r in df_csv
      s = JSON.stringify r
      unless seen.has s
        csv_missing.push r

# ----- Helpers -----
word_count = (s) -> s.trim().split(/\s+/).length
ends_with_terminator = (s) -> /[.!?…]$/.test s.trim()
has_trailing_whitespace = (s) -> s.length > 0 and /\s$/.test s
distinct_n = (tokens, n=1) ->
  return 0.0 if tokens.length < n
  ngrams = new Set()
  for i in [0..tokens.length-n]
    ngrams.add tokens.slice(i, i+n).join(" ")
  ngrams.size / Math.max 1, (tokens.length - n + 1)

# ----- Load training examples for memorization checks -----
c          = JSON.parse fs.readFileSync(CONTRACT, 'utf8')
train_path = c.filenames.train.resolved
text_field = Object.keys(c.schema.fields).find((k) ->
  String(c.schema.fields[k]).toLowerCase() is "string"
) or "text"

train_texts = []
for line in fs.readFileSync(train_path, 'utf8').split /\r?\n/
  continue unless line.trim()
  try
    obj = JSON.parse line
    t   = obj[text_field] or ""
    if typeof t is 'string'
      train_texts.push t.trim()
  catch e
    continue

train_blob = train_texts.join "\n\n"
train_set  = new Set train_texts

# ----- Per-row metrics -----
row_metrics = (r) ->
  gen = String(r.generation or "")
  toks = gen.split /\s+/
  d1 = distinct_n toks, 1
  d2 = distinct_n toks, 2
  exact_mem  = train_set.has gen.trim()
  substr_mem = (not exact_mem) and gen.trim().length >= 20 and train_blob.includes gen.trim()
  Object.assign {}, r,
    len_chars: gen.length
    len_words: word_count gen
    ends_sentence: if ends_with_terminator(gen) then 1 else 0
    ends_whitespace: if has_trailing_whitespace(gen) then 1 else 0
    distinct1: Number(d1.toFixed 4)
    distinct2: Number(d2.toFixed 4)
    memorized_exact: if exact_mem then 1 else 0
    memorized_substring: if substr_mem then 1 else 0

metrics = rows.map row_metrics

# ----- Aggregate by mode -----
groupBy = (arr, key) ->
  out = {}
  for r in arr
    k = r[key]
    out[k] ?= []
    out[k].push r
  out

agg = []
for mode, arr of groupBy(metrics, "mode")
  n = arr.length
  avg_len_chars   = arr.reduce(((a,b)->a+b.len_chars),0)/n
  med_len_chars   = arr.map((r)->r.len_chars).sort((a,b)->a-b)[Math.floor(n/2)]
  avg_len_words   = arr.reduce(((a,b)->a+b.len_words),0)/n
  sent_end_rate   = arr.reduce(((a,b)->a+b.ends_sentence),0)/n
  trailing_ws_rate= arr.reduce(((a,b)->a+b.ends_whitespace),0)/n
  distinct1_mean  = arr.reduce(((a,b)->a+b.distinct1),0)/n
  distinct2_mean  = arr.reduce(((a,b)->a+b.distinct2),0)/n
  mem_exact_rate  = arr.reduce(((a,b)->a+b.memorized_exact),0)/n
  mem_sub_rate    = arr.reduce(((a,b)->a+b.memorized_substring),0)/n
  agg.push
    mode: mode
    n: n
    avg_len_chars: Number(avg_len_chars.toFixed 4)
    med_len_chars: med_len_chars
    avg_len_words: Number(avg_len_words.toFixed 4)
    sent_end_rate: Number(sent_end_rate.toFixed 4)
    trailing_ws_rate: Number(trailing_ws_rate.toFixed 4)
    distinct1_mean: Number(distinct1_mean.toFixed 4)
    distinct2_mean: Number(distinct2_mean.toFixed 4)
    mem_exact_rate: Number(mem_exact_rate.toFixed 4)
    mem_sub_rate: Number(mem_sub_rate.toFixed 4)

# ----- Per-prompt sample table -----
sample_table = (arr, n=1) ->
  out = []
  grouped = groupBy arr, "prompt"
  for prompt, subset of grouped
    byMode = groupBy subset, "mode"
    for mode, ss of byMode
      for rr in ss.slice 0,n
        out.push prompt: prompt, mode: mode, generation: rr.generation
  out

preview = sample_table(metrics, 1)

# ----- Save outputs -----
fs.mkdirSync path.dirname(OUT_SUM), {recursive:true}
fs.writeFileSync OUT_SUM, d3.csvFormat agg
analysis =
  created_utc: new Date().toISOString()
  by_mode: agg
  notes: [
    "JSONL is source of truth to avoid NaN coercion from CSV parsing."
    "distinct* ~ lexical diversity over whitespace tokens."
    "memorized_* checks against training set (exact / substring)."
  ]
fs.writeFileSync OUT_JSON, JSON.stringify(analysis,null,2)

# ----- Console summary -----
console.log "=== EOS / OUTPUT ANALYSIS (by mode) [JSONL] ==="
console.table agg

console.log "\n=== SAMPLE OUTPUTS (1 per prompt×mode) ==="
for r in preview
  console.log "\n[#{r.mode}] #{r.prompt}\n→ #{r.generation}"

if csv_missing.length > 0
  console.log "\n[CSV diagnostic] Rows mismatched between JSONL and CSV parsing:"
  console.log csv_missing.slice(0,6)

console.log "\nWrote:", OUT_SUM, "and", OUT_JSON
