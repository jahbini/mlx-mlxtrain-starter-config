#!/usr/bin/env coffee
###
prepare_prompts.coffee — KAG V1 (runCfg version)
• Reads emotion-tagged training JSONL (run.kag_input_jsonl)
• Produces KAG LoRA prompts JSONL (run.kag_output_jsonl)
• NO defaults — everything must be defined in run.*
• Fully memo-native: all writes via M.saveThis, JSONL handled by memo regex
###

fs   = require 'fs'
path = require 'path'

@step =
  desc: "Create KAG-friendly LoRA prompts (run.* paths only)"

  action: (M, stepName) ->

    # -------------------------------------------------------------
    # 1. Load experiment.yaml and step config
    # -------------------------------------------------------------
    cfg = M.theLowdown('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    runCfg= run = cfg.run
    throw new Error "Missing run section in experiment.yaml" unless run?

    # Step config exists but *does not define paths*
    stepCfg = cfg[stepName] ? {}
    # (we allow empty stepCfg — KAG V1 needs no step parameters)

    IN_KEY  = runCfg.merged_segments
    OUT_KEY = runCfg.kag_examples

    # -------------------------------------------------------------
    # 3. Load input JSONL from memo
    # -------------------------------------------------------------
    raw = M.theLowdown(IN_KEY)?.value
    throw new Error "Missing memo value for #{IN_KEY}" unless raw?

    unless typeof raw is 'string'
      throw new Error "Input #{IN_KEY} must be JSONL string"

    lines = raw.trim().split(/\r?\n/)
    rows  = []
    for ln in lines when ln.trim().length
      try
        rows.push JSON.parse ln
      catch e
        console.warn "[prepare_prompts] bad JSON:", e.message

    console.log "[prepare_prompts] loaded #{rows.length} emotion rows"

    # -------------------------------------------------------------
    # 4. Build KAG prompts (V1)
    # -------------------------------------------------------------
    outRows = []
    snappy =10
    for r in rows
      meta   = r.Meta ? {}
      story  = r.prompt 
      ems    = r.Emotions ? []
      continue unless story? and ems.length

      emoText = ems.map((e)-> "#{e.emotion} (#{e.intensity})").join(", ")

      prompt =
        "Instruction:\n" +
        "Using the narrator voice and tone from my stories, write a short passage that naturally expresses:\n" +
        "#{emoText}\n\n" +
        "Context:\n#{story}\n\n" +
        "Response:\n"

      outRows.push  prompt 

    console.log "[prepare_prompts] produced #{outRows.length} prompts"

    # -------------------------------------------------------------
    # 5. Save via memo (JSONL auto-written by memo regex)
    # -------------------------------------------------------------
    jsonlLines = []
    for item in outRows
      jsonlLines.push item

    M.saveThis OUT_KEY, jsonlLines
    M.saveThis "done:#{stepName}", true

    console.log "[prepare_prompts] wrote #{jsonlLines.length} → #{OUT_KEY}"
    return
