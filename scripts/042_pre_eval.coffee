#!/usr/bin/env coffee
###
042_pre_eval.coffee — strict memo-aware version (2025)
-------------------------------------------------------
STEP — Pre-Evaluation Sanity Checker
Reads eval_out/generations.jsonl
Computes summary stats (empties, avg length, prompt coverage)
Emits:
  pre_eval:summary
  pre_eval_summary.json
  pre_eval_summary.csv
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
  desc: "Pre-evaluation sanity check for generations.jsonl"

  action: (M, stepName) ->
    throw new Error "Missing stepName argument" unless stepName?

    cfg = M.theLowdown("experiment.yaml")?.value or M.theLowdown("evaluation.yaml")?.value
    unless cfg?
      throw new Error "No config found in memo (experiment.yaml or evaluation.yaml)"

    stepCfg = cfg[stepName]
    throw new Error "Missing config for '#{stepName}' in yaml" unless stepCfg?
    params = stepCfg.params or {}
    for key in ['input_dir', 'output_dir']
      unless params[key]? and params[key].length
        throw new Error "Missing required param '#{key}' for step '#{stepName}'"

    inputDir  = params.input_dir
    outputDir = params.output_dir
    fs.mkdirSync(outputDir, {recursive:true})

    GEN_PATH = path.join(inputDir, 'generations.jsonl')
    unless fs.existsSync(GEN_PATH)
      throw new Error "❌ Missing #{GEN_PATH} — snapshot step must run first."

    console.log "=== Pre-Eval starting ==="
    console.log "Input:", GEN_PATH
    console.log "Output:", outputDir

    rows = readJSONLines(GEN_PATH)
    total = rows.length
    throw new Error "❌ No rows found; aborting pre-eval." if total is 0

    empty = 0; tooShort = 0; words = []; prompts = new Set()
    for r in rows
      g = (r.generation or r.output_text or '').trim()
      w = g.split(/\s+/).filter((x)->x.length).length
      prompts.add(r.prompt or '')
      if g.length is 0 then empty++
      else if w < 5 then tooShort++
      else words.push(w)

    summary =
      timestamp_utc: timestampUTC()
      total_rows: total
      empty_count: empty
      too_short: tooShort
      avg_words: Number(mean(words).toFixed(2))
      unique_prompts: prompts.size
      empty_pct: Number((100 * empty / total).toFixed(1))
      short_pct: Number((100 * tooShort / total).toFixed(1))

    console.log "Summary:", summary

    M.saveThis "pre_eval:summary", summary
    M.saveThis "pre_eval_summary.json", summary
    M.saveThis "pre_eval_summary.csv", [summary]
    M.saveThis "done:#{stepName}", true

    console.log "=== Pre-evaluation complete ==="
    return summary