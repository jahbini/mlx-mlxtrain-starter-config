#!/usr/bin/env coffee

@step =
  desc: "Rotate merged → train.jsonl; old train appended → valid.jsonl (memo-pure)"

  action: (M, stepName) ->

    # ----------------------------------------------------------
    # Helper: build LoRA-style training prompt from one segment
    # ----------------------------------------------------------
    build_lora_prompt = (seg) ->
      emos = []
      for k,v of seg.emotions
        emos.push(  k + ": " + v ) unless v == "none"
      emosLine = if emos.length then emos.join(", ")  else "neutral"

      ctx = seg.prompt ? seg.text ? ""
      ctxStr = String(ctx).trim()

      [
        "Instruction:",
        "Using the narrator voice and tone from my stories, write a short passage that naturally expresses: #{emosLine}",
        "\n",
        "Context:",
        ctxStr,
        "\n",
        "Response:",
        "\n"
      ].join("\n")


    cfg = M.theLowdown("experiment.yaml")?.value
    throw new Error "Missing experiment.yaml" unless cfg?

    runCfg  = cfg.run
    stepCfg = cfg[stepName] ? {}

    # Required run: values
    for k in ['merged_segments','train_file','valid_file']
      throw new Error "Missing run.#{k}" unless runCfg[k]?

    mergedKey = runCfg.merged_segments
    trainKey  = runCfg.train_file
    validKey  = runCfg.valid_file
    testKey   = runCfg.test_file

    # ----------------------------------------------------------
    # Load memo data
    # ----------------------------------------------------------

    mergedEntry = M.demand(mergedKey)
    mergedRows  = mergedEntry?.value ? []
    unless Array.isArray(mergedRows)
      throw new Error "Merged segments in #{mergedKey} must be array"

    # Old train
    trainEntry = M.demand(trainKey)
    oldTrain   = trainEntry?.value ? []
    unless Array.isArray(oldTrain)
      oldTrain = []

    # Old valid
    validEntry = M.demand(validKey)
    oldValid   = validEntry?.value ? []
    unless Array.isArray(oldValid)
      oldValid = []

    # ----------------------------------------------------------
    # Perform rotation:
    #   newTrain = ALL merged
    #   newValid = oldValid + oldTrain
    # ----------------------------------------------------------

    newTrain = mergedRows.map (seg) ->
      text = build_lora_prompt seg
      { text }


    newValid = oldValid.concat(oldTrain)

    # ----------------------------------------------------------
    # Save to memo (auto JSONL)
    # ----------------------------------------------------------

    M.saveThis trainKey, newTrain
    M.saveThis validKey, newValid
    M.saveThis testKey,  oldTrain

    # For debugging/transparency
    console.log "rotate_merge:"
    console.log "  merged rows:", mergedRows.length
    console.log "  old train:", oldTrain.length
    console.log "  old valid:", oldValid.length
    console.log "  new train:", newTrain.length
    console.log "  new valid:", newValid.length

    M.saveThis "done:#{stepName}", true
    return
