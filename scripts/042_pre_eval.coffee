###
  scripts/042_pre_eval.coffee
  ------------------------------------------------------------
  New-Style Step (reactive Memo version)
  Pre-Evaluation Sanity Checker
  - Reads eval_out/generations.jsonl (created by snapshot.py)
  - Computes summary stats: empties, avg length, prompt coverage
  - Emits memo entries:
        "pre_eval:summary"         → summary object
        "pre_eval_summary.json"    → summary object  (auto-saved)
        "pre_eval_summary.csv"     → [summary] array (auto-saved)
###

fs   = require 'fs'
path = require 'path'

# --- Helpers ---------------------------------------------------------------
readJSONLines = (p) ->
  return [] unless fs.existsSync(p)
  lines = fs.readFileSync(p, 'utf8').split(/\r?\n/)
  out = []
  for l in lines when l.trim().length
    try out.push JSON.parse(l)
    catch err
      console.warn "⚠️ bad JSON line:", err.message
  out

mean = (xs) ->
  return 0 unless xs?.length
  s = 0
  for x in xs when typeof x is 'number'
    s += x
  s / xs.length

timestampUTC = ->
  new Date().toISOString().replace(/\.\d+Z$/,'Z')

# ---------------------------------------------------------------------------
# Step definition
# ---------------------------------------------------------------------------
@step =
  name: "pre_eval"
  desc: "Pre-evaluation sanity check for generations.jsonl"

  action: (M) ->
    spec = M.theLowdown("evaluation.yaml")?.value
    unless spec?
      throw new Error "evaluation.yaml not in memo"

    params = spec?.pre_eval?.params
    unless params?
      throw new Error "pre_eval.params missing in evaluation.yaml"

    # Validate required parameters — may not invent defaults
    for key in ['input_dir', 'output_dir']
      unless params[key]? and params[key]?.length
        throw new Error "❌ Missing required param pre_eval.params.#{key}"

    inputDir  = params.input_dir
    outputDir = params.output_dir
    fs.mkdirSync outputDir, {recursive:true}

    GEN_PATH = path.join(inputDir, 'generations.jsonl')
    unless fs.existsSync(GEN_PATH)
      throw new Error "❌ Missing #{GEN_PATH}. Did snapshot.py run?"

    console.log "=== Pre-Eval starting ==="
    console.log "Input:", GEN_PATH
    console.log "Output:", outputDir

    rows = readJSONLines(GEN_PATH)
    total = rows.length
    if total is 0
      throw new Error "❌ No rows found; aborting pre-eval."

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

    # --- Publish results to Memo (reactive persistence handles file writes)
    M.saveThis "pre_eval:summary", summary
    M.saveThis "pre_eval_summary.json", summary
    M.saveThis "pre_eval_summary.csv", [summary]
    M.saveThis "status:pre_eval", "done"

    console.log "=== Pre-evaluation complete ==="
    return summary
