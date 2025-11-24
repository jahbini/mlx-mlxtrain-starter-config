#!/usr/bin/env coffee
fs = require 'fs'

@step =
  desc: "Select a small batch of untagged paragraph segments and query MLX emotion oracle"

  action: (M, stepName) ->

    cfg = M.theLowdown("experiment.yaml")?.value
    throw new Error "Missing experiment.yaml" unless cfg?

    runCfg  = cfg.run
    stepCfg = cfg[stepName]

    throw new Error "Missing run section"  unless runCfg?
    throw new Error "Missing step section" unless stepCfg?

    segPath  = runCfg.marshalled_stories
    emoPath  = runCfg.kag_emotions
    batchSz  = stepCfg.batch_size

    throw new Error "Missing run.story_segments" unless segPath?
    throw new Error "Missing run.kag_emotions"   unless emoPath?
    throw new Error "Missing stepCfg.batch_size" unless batchSz?

    unless fs.existsSync(segPath)
      throw new Error "story_segments not found: #{segPath}"

    # ------------------------------------------------------------
    # Load story segments JSONL
    # ------------------------------------------------------------
    segLines = fs.readFileSync(segPath, 'utf8').trim().split(/\r?\n/)
    segments = segLines.map (l) -> JSON.parse(l)

    # Load already-tagged emotions
    tagged = new Set()
    if fs.existsSync(emoPath)
      emoLines = fs.readFileSync(emoPath, 'utf8').trim().split(/\r?\n/)
      for l in emoLines when l.length
        obj = JSON.parse(l)
        key = "#{obj.meta?.doc_id}|#{obj.meta?.paragraph_index}"
        tagged.add(key)

    # ------------------------------------------------------------
    # Select a batch of untagged segments
    # ------------------------------------------------------------
    pending = []
    for s in segments
      key = "#{s.meta?.doc_id}|#{s.meta?.paragraph_index}"
      continue if tagged.has(key)
      pending.push s
      break if pending.length >= batchSz

    if pending.length is 0
      console.log "oracle_ask: no new segments to tag."
      M.saveThis "oracle_ask:empty", true
      M.saveThis "done:#{stepName}", true
      return

    # ------------------------------------------------------------
    # Query MLX for each pending segment
    # ------------------------------------------------------------
    
    for seg in pending
      text = seg.text ? ""
      meta = seg.meta ? {}
      prompt = "google--gemma-2-2b"
      prompt = """
You are a classifier. Given this sample <<< #{text} >>> classify each emotion with classification of:
"none", "mild", "moderate", "strong", "extreme".

Return exactly like this:
{
  "anger": classification,
  "fear": classification,
  "joy": classification,
  "sadness": classification,
  "desire": classification,
  "curiosity": classification
}
"""
      # Save request to the memo: triggers MLX meta rule
      args =
        model: runCfg.model
        prompt: prompt
        "max-tokens": stepCfg.max_tokens ? 256

      entry = M.callMLX "generate",args
      # Wait for the MLX result

      extractJSON = (raw) ->
        return {} unless raw?
        # Extract anything between a pair of curly braces, longest match first
        m = raw.match(/\{[\s\S\n]*\}/)
        return {} unless m?

        block = m[0]

        # 2) Try parsing. If it fails, return null and let caller decide.
        try
          JSON.parse(block)
        catch err
          {}

      # Store result as one JSONL line
      out =
        meta:
          doc_id: meta.doc_id
          paragraph_index: meta.paragraph_index
        emotions: extractJSON entry

      if fs.existsSync(emoPath)
        fs.appendFileSync(emoPath, JSON.stringify(out) + "\n")
      else
        fs.writeFileSync(emoPath, JSON.stringify(out) + "\n")

      console.log "oracle_ask: tagged #{meta.doc_id} #{meta.paragraph_index}"

    M.saveThis "done:#{stepName}", true
    return
