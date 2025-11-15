#!/usr/bin/env coffee

# -------------------------------------------------------------------
# 044_pre_eval.coffee — prepare data for evaluator
# -------------------------------------------------------------------

fs   = require 'fs'
path = require 'path'
yaml = require 'js-yaml'

@step =
  desc: "Prepare evaluation input from snapshot generations"

  action: (M, stepName) ->

    cfg = M.theLowdown('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml" unless cfg?

    stepCfg = cfg[stepName]
    runCfg  = cfg['run']
    throw new Error "Missing pre_eval config" unless stepCfg?
    throw new Error "Missing run config" unless runCfg?

    SNAP_NAME = stepCfg.snapshots
    JSONL_KEY = "#{SNAP_NAME}.jsonl"

    # --------------------------------------------------------------
    # Load snapshot lines from memo (wait if needed)
    # --------------------------------------------------------------
    entry = M.theLowdown(JSONL_KEY)
    lines = entry.value

    if not lines?
      await entry.notifier
      lines = M.theLowdown(JSONL_KEY).value

    unless Array.isArray(lines)
      throw new Error "Snapshot JSONL missing in memo"

    # Rehydrate objects
    rows = []
    for line in lines
      try
        rows.push JSON.parse(line)
      catch e
        console.warn "Bad generation row:", line, e.message

    # --------------------------------------------------------------
    # Pre-eval format (simple pairings)
    # --------------------------------------------------------------
    evalRecords = []
    for r in rows
      evalRecords.push
        prompt: r.prompt
        generation: r.generation
        len_chars: r.len_chars
        len_words: r.len_words
        is_empty: r.is_empty
        model_id: r.model_id
        artifact: r.artifact

    # --------------------------------------------------------------
    # Save as JSONL + YAML (memo handles filesystem)
    # --------------------------------------------------------------
    outJsonl = "pre_eval.jsonl"
    outYaml  = "pre_eval.yaml"

    M.saveThis outJsonl, evalRecords.map((r)->JSON.stringify(r))
    M.saveThis outYaml, yaml.safeDump(evalRecords)

    M.saveThis "done:#{stepName}", true
    return
