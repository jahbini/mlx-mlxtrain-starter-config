#!/usr/bin/env coffee
fs = require 'fs'   # kept only in case step module requires; unused

@step =
  desc: "Merge oracle emotion replies into marshalled story segments (memo-native)"

  action: (M, stepName) ->

    cfg = M.theLowdown("experiment.yaml")?.value
    throw new Error "Missing experiment.yaml" unless cfg?

    runCfg  = cfg.run
    stepCfg = cfg[stepName]

    throw new Error "Missing run section"  unless runCfg?
    throw new Error "Missing step config" unless stepCfg?

    # Input memo keys (NO FS)
    segKey  = runCfg.marshalled_stories
    emoKey  = runCfg.kag_emotions
    outKey  = runCfg.merged_segments

    throw new Error "Missing run.marshalled_stories" unless segKey?
    throw new Error "Missing run.kag_emotions"       unless emoKey?
    throw new Error "Missing run.merged_segments"    unless outKey?

    # ----------------------------
    # Load story segments (JSONL)
    # ----------------------------
    segEntry = M.demand(segKey)
    segRaw   = segEntry.value ? []
    segments = segRaw

    # ----------------------------
    # Load oracle replies (JSONL)
    # ----------------------------
    emoEntry = M.demand(emoKey)
    emoRaw   = emoEntry.value ? []
    replies  = emoRaw

    # Build lookup table: { "doc|idx" → emotions }
    lookup = {}
    for r in replies
      id = "#{r.meta?.doc_id}|#{r.meta?.paragraph_index}"
      lookup[id] = r.emotions

    merged = []

    # ----------------------------
    # Merge matching emotions
    # ----------------------------
    for s in segments
      id = "#{s.meta?.doc_id}|#{s.meta?.paragraph_index}"
      emos = lookup[id] ? null
      continue unless emos?        # only output rows that have oracle data

      merged.push
        meta: s.meta
        prompt: s.text ? s.prompt
        emotions: emos

    # Save merged rows as JSONL in memo
    M.saveThis outKey, merged

    M.saveThis "done:#{stepName}", true
    return
