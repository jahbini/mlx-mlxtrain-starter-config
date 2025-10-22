#!/usr/bin/env coffee

###
STEP 12 — Sanity / Ablation Evaluation (CoffeeScript)
Evaluation-side version (no MLX generation)

Reads:
  eval_out/ablations.jsonl     ← produced by 042_examination.py
Writes:
  eval_out/ablations_summary.json
  eval_out/ablations_summary.csv

Checks for:
  - empty generations
  - sentence endings
  - average and median word counts
  - per-artifact × prompt_variant grouping
###

fs   = require 'fs'
path = require 'path'
yaml = require 'js-yaml'
d3   = require 'd3-dsv'
_    = require 'lodash'

# --- Local Config Loader ---
loadLocalConfig = ->
  expPath = path.join(process.cwd(), 'evaluate.yaml')
  unless fs.existsSync(expPath)
    console.error "❌ Missing evaluate.yaml in #{process.cwd()}"
    process.exit(1)
  yaml.load fs.readFileSync(expPath, 'utf8')

CFG = loadLocalConfig()

EVAL_DIR = path.resolve CFG.run.eval_dir
fs.mkdirSync EVAL_DIR, { recursive: true }

ABL_JSONL = path.join EVAL_DIR, "#{CFG.sanity.ablations}.jsonl"
SUM_JSON  = path.join EVAL_DIR, "#{CFG.sanity.ablations}_summary.json"
SUM_CSV   = path.join EVAL_DIR, "#{CFG.sanity.ablations}_summary.csv"
unless fs.existsSync ABL_JSONL
  console.error "❌ Missing #{ABL_JSONL}. Run 042_examination.py first."
  process.exit(1)

# --- Load data ---
rows = []
for line in fs.readFileSync(ABL_JSONL, 'utf8').split /\r?\n/
  continue unless line.trim()
  try
    rows.push JSON.parse line
  catch err
    console.warn "⚠️ Skipped malformed line:", err.message

if rows.length is 0
  console.error "No valid rows found in #{ABL_JSONL}"
  process.exit(1)

# --- Helpers ---
pct = (x) -> "#{(x * 100).toFixed(1)}%"
median = (xs) ->
  ys = _.sortBy xs
  mid = Math.floor ys.length / 2
  if ys.length % 2 then ys[mid] else (ys[mid-1] + ys[mid]) / 2
endsSentence = (s) -> /[.!?…]$/.test s.trim()

# --- Group and summarize ---
groups = _.groupBy rows, (r) -> [r.model_id, r.artifact, r.prompt_variant].join('|')
agg = []

for key, g of groups
  n = g.length

  empty = _.sumBy g, (x) ->
    val = if x.is_empty then 1 else 0
    return val

  sentEnd = _.sumBy g, (x) ->
    txt = String(x.generation or "")
    val = if endsSentence(txt) then 1 else 0
    return val

  lens_list = []
  for x in g
    lw = x.len_words
    lens_list.push lw

  avgLen = _.mean(lens_list) or 0
  medLen = median(lens_list) or 0

  parts = key.split('|')
  model_id = parts[0]
  artifact = parts[1]
  prompt_variant = parts[2]

  rec =
    model_id: model_id
    artifact: artifact
    prompt_variant: prompt_variant
    n: n
    empty_rate: empty / n
    sent_end_rate: sentEnd / n
    avg_len: avgLen
    med_len: medLen

  agg.push rec

# --- Save outputs ---
ts = new Date().toISOString().replace(/\.\d+Z$/,'Z')
summary =
  created_utc: ts
  total_rows: rows.length
  groups: agg

fs.writeFileSync SUM_JSON, JSON.stringify(summary, null, 2), 'utf8'

csv_text = d3.csvFormat(agg)
fs.writeFileSync SUM_CSV, csv_text, 'utf8'

# --- Console output ---
console.log "=== Sanity / Ablation Summary ==="
for r in agg
  console.log "#{r.model_id} | #{r.artifact} | #{r.prompt_variant} | n=#{r.n} " +
              "empty=#{pct r.empty_rate} sent_end=#{pct r.sent_end_rate} " +
              "avg=#{r.avg_len.toFixed 3} med=#{r.med_len}"

console.log "\n✅ Wrote summary:"
console.log "  JSON → #{SUM_JSON}"
console.log "  CSV  → #{SUM_CSV}"
