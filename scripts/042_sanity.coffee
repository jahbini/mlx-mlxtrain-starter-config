#!/usr/bin/env coffee
###
042_sanity.coffee — strict memo-aware version (2025)
----------------------------------------------------
STEP — Sanity / Ablation Evaluation (no MLX generation)

Reads:
  eval_out/<ablations>.jsonl   ← produced by examination.coffee
Writes:
  eval_out/<ablations>_summary.json
  eval_out/<ablations>_summary.csv

Checks:
  - empty generations
  - sentence endings
  - average / median word counts
  - grouped by artifact × prompt_variant
###

fs   = require 'fs'
path = require 'path'
yaml = require 'js-yaml'
d3   = require 'd3-dsv'
_    = require 'lodash'

@step =
  desc: "Aggregate and summarize ablation results (sanity evaluation)"

  action: (M, stepName) ->
    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    stepCfg = cfg[stepName]
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?
    runCfg  = cfg['run']
    throw new Error "Missing run section in experiment.yaml" unless runCfg?

    # --- Required keys --------------------------------------------
    unless runCfg.eval_dir?
      throw new Error "Missing required key: run.eval_dir in experiment.yaml"
    unless stepCfg.ablations?
      throw new Error "Missing required key: #{stepName}.ablations in experiment.yaml"


    ABL_NAME  = runCfg.ablations
    ABL_JSONL = path.join( "#{ABL_NAME}.jsonl")
    SUM_JSON  = path.join( "#{ABL_NAME}_summary.json")
    SUM_CSV   = path.join( "#{ABL_NAME}_summary.csv")

    unless fs.existsSync(ABL_JSONL)
      throw new Error "Missing #{ABL_JSONL} — run examination step first."

    # --- Load data -------------------------------------------------
    rows = []
    for line in fs.readFileSync(ABL_JSONL, 'utf8').split(/\r?\n/)
      continue unless line.trim()
      try
        rows.push JSON.parse(line)
      catch err
        console.warn "⚠️ Skipped malformed line:", err.message

    throw new Error "No valid rows found in #{ABL_JSONL}" unless rows.length

    # --- Helpers ---------------------------------------------------
    pct = (x) -> "#{(x * 100).toFixed(1)}%"
    median = (xs) ->
      ys = _.sortBy(xs)
      return 0 unless ys.length
      mid = Math.floor(ys.length / 2)
      if ys.length % 2 then ys[mid] else (ys[mid - 1] + ys[mid]) / 2
    endsSentence = (s) -> /[.!?…]$/.test(String(s).trim())

    # --- Group and summarize --------------------------------------
    groups = _.groupBy(rows, (r) -> [r.model_id, r.artifact, r.prompt_variant].join('|'))
    agg = []

    for key, g of groups
      n = g.length
      empty = _.sumBy(g, (x) -> if x.is_empty then 1 else 0)
      sentEnd = _.sumBy(g, (x) -> if endsSentence(x.generation or '') then 1 else 0)
      lens = (x.len_words for x in g when typeof x.len_words is 'number')
      avgLen = _.mean(lens) or 0
      medLen = median(lens) or 0

      [model_id, artifact, prompt_variant] = key.split('|')
      agg.push {
        model_id, artifact, prompt_variant, n,
        empty_rate: empty / n,
        sent_end_rate: sentEnd / n,
        avg_len: avgLen,
        med_len: medLen
      }

    # --- Save outputs ----------------------------------------------
    ts = new Date().toISOString().replace(/\.\d+Z$/, 'Z')
    summary =
      created_utc: ts
      total_rows: rows.length
      groups: agg

    fs.writeFileSync(SUM_JSON, JSON.stringify(summary, null, 2), 'utf8')
    fs.writeFileSync(SUM_CSV, d3.csvFormat(agg), 'utf8')

    # --- Console report --------------------------------------------
    console.log "=== Sanity / Ablation Summary ==="
    for r in agg
      console.log "#{r.model_id} | #{r.artifact} | #{r.prompt_variant} | n=#{r.n} " +
                  "empty=#{pct(r.empty_rate)} sent_end=#{pct(r.sent_end_rate)} " +
                  "avg=#{r.avg_len.toFixed(3)} med=#{r.med_len}"

    console.log "\n✅ Wrote summary:"
    console.log "  JSON → #{SUM_JSON}"
    console.log "  CSV  → #{SUM_CSV}"

    # --- Memo save -------------------------------------------------
    M.saveThis "#{stepName}:summary", summary
    M.saveThis "#{stepName}_summary.json", summary
    M.saveThis "#{stepName}_summary.csv", agg
    M.saveThis "done:#{stepName}", true
    return summary
