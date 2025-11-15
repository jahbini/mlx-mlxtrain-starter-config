#!/usr/bin/env coffee

# -------------------------------------------------------------------
# 043_metrics.coffee — compute simple statistics on generations.jsonl
# -------------------------------------------------------------------

fs   = require 'fs'
path = require 'path'
yaml = require 'js-yaml'

@step =
  desc: "Compute simple metrics from snapshot generations"

  action: (M, stepName) ->

    cfg = M.theLowdown('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml" unless cfg?

    stepCfg = cfg[stepName]
    runCfg  = cfg['run']
    throw new Error "Missing metrics step config" unless stepCfg?
    throw new Error "Missing run config" unless runCfg?

    SNAP_NAME = stepCfg.snapshots
    JSONL_KEY = "#{SNAP_NAME}.jsonl"
    YAML_KEY  = "#{SNAP_NAME}.yaml"

    # ----------------------------------------------------------------
    # Load snapshot from memo (force wait if needed)
    # ----------------------------------------------------------------
    entry = M.theLowdown(JSONL_KEY)
    rowsTxt = entry.value

    if not rowsTxt?
      await entry.notifier
      rowsTxt = M.theLowdown(JSONL_KEY).value

    unless Array.isArray(rowsTxt)
      throw new Error "Snapshot JSONL missing or invalid in memo"

    # Convert array-of-lines back to objects
    rows = []
    for line in rowsTxt
      try
        rows.push JSON.parse(line)
      catch e
        console.warn "Bad JSONL row:", line, e.message

    # ----------------------------------------------------------------
    # Compute simple metrics
    # ----------------------------------------------------------------
    total = rows.length
    empty = rows.filter((r)->r.is_empty is 1).length
    avgChars = if total then rows.reduce(((a,r)->a+r.len_chars),0)/total else 0
    avgWords = if total then rows.reduce(((a,r)->a+r.len_words),0)/total else 0

    summary =
      snapshot: SNAP_NAME
      total_rows: total
      empty_rows: empty
      pct_empty: if total then (100.0 * empty / total).toFixed(2) else "0"
      avg_chars: avgChars
      avg_words: avgWords

    # ----------------------------------------------------------------
    # Save metrics via memo (NOT filesystem)
    # ----------------------------------------------------------------
    M.saveThis "#{SNAP_NAME}_metrics.json", summary
    M.saveThis "#{SNAP_NAME}_metrics.yaml", yaml.safeDump(summary)

    M.saveThis "done:#{stepName}", true
    return
