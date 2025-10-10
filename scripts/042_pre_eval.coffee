###
  scripts/042_pre_eval.coffee
  ------------------------------------------------------------
  Pre-Evaluation Sanity Checker
  - Runs first in the evaluation pipeline.
  - Reads eval_out/generations.jsonl (created by snapshot.py).
  - Computes summary stats: empties, avg length, prompt coverage.
  - Writes eval_out/pre_eval_summary.json and pre_eval_summary.csv
###

fs   = require 'fs'
path = require 'path'
yaml = require 'js-yaml'

# --- Helpers ---
readJSONLines = (p) ->
  return [] unless fs.existsSync(p)
  lines = fs.readFileSync(p, 'utf8').split(/\r?\n/)
  out = []
  for l in lines when l.trim().length
    try out.push JSON.parse(l)
    catch err
      console.warn "⚠️  bad JSON line:", err.message
  out

writeJSON = (p, obj) ->
  fs.writeFileSync(p, JSON.stringify(obj, null, 2), 'utf8')

writeCSV = (p, rows) ->
  return unless rows?.length
  keys = Object.keys(rows[0])
  buf = [keys.join(',')]
  for r in rows
    vals = (String(r[k] or '').replace(/,/g, ';') for k in keys)
    buf.push vals.join(',')
  fs.writeFileSync(p, buf.join('\n'), 'utf8')

mean = (xs) ->
  return 0 unless xs?.length
  sum = 0
  for x in xs when typeof x is 'number'
    sum += x
  sum / xs.length

timestampUTC = ->
  new Date().toISOString().replace(/\.\d+Z$/,'Z')

# --- Main ---
main = ->
  console.log "=== 042_pre_eval.coffee starting ==="

  EVAL_DIR = path.join(process.cwd(), 'eval_out')
  GEN_PATH = path.join(EVAL_DIR, 'generations.jsonl')
  OUT_JSON = path.join(EVAL_DIR, 'pre_eval_summary.json')
  OUT_CSV  = path.join(EVAL_DIR, 'pre_eval_summary.csv')

  unless fs.existsSync(GEN_PATH)
    console.error "❌ Missing #{GEN_PATH}. Did snapshot.py run?"
    process.exit(1)

  rows = readJSONLines(GEN_PATH)
  total = rows.length
  console.log "Loaded #{total} generation rows."

  if total is 0
    console.error "❌ No rows found; aborting pre-eval."
    process.exit(1)

  empty = 0
  tooShort = 0
  words = []
  prompts = new Set()

  for r in rows
    g = (r.generation or r.output_text or '').trim()
    w = g.split(/\s+/).length
    prompts.add(r.prompt or '')
    if g.length is 0 then empty++
    else if w < 5 then tooShort++
    else words.push(w)

  summary =
    timestamp: timestampUTC()
    total_rows: total
    empty_count: empty
    too_short: tooShort
    avg_words: mean(words).toFixed(2)
    unique_prompts: prompts.size
    empty_pct: (100 * empty / total).toFixed(1)
    short_pct: (100 * tooShort / total).toFixed(1)

  console.log "Summary:", summary

  writeJSON OUT_JSON, summary
  writeCSV OUT_CSV, [summary]

  console.log "✅ Wrote:"
  console.log "   JSON → #{OUT_JSON}"
  console.log "   CSV  → #{OUT_CSV}"
  console.log "=== Pre-evaluation complete ==="

main()
