#!/usr/bin/env coffee
###
train_kagnam.coffee — MLX LoRA training using kag_examples
- Uses experiments.csv created by prepare_kagnam_experiments
- Dispatches MLX training via memo key "mlx-lm:lora"
###

fs   = require 'fs'
path = require 'path'

@step =
  desc: "Run MLX LoRA training for KAG examples (kagnam pipeline)"

  action: (M, stepName) ->

    throw new Error "Missing stepName" unless stepName?

    cfg = M.theLowdown('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml" unless cfg?

    runCfg = cfg.run
    throw new Error "Missing run{} section" unless runCfg?

    EXP_CSV_KEY = runCfg.experiments_csv
    TRAIN_FILE  = runCfg.train_file
    MODEL_ID    = runCfg.model

    # --- Read experiments.csv from memo ---
    csvText = M.theLowdown(EXP_CSV_KEY)?.value
    throw new Error "experiments.csv missing in memo: #{EXP_CSV_KEY}" unless csvText?

    lines = csvText.trim().split '\n'
    headers = lines[0].split ','
    values  = lines[1].split ','

    row = {}
    for v,i in values
      row[ headers[i] ] = v

    adapterPath = row.adapter_path
    throw new Error "Missing adapter_path in experiments.csv" unless adapterPath?

    # --- Training payload for MLX agent ---
    payload =
      op:            "lora"
      model_id:      row.model_id
      data:          row.train_file
      batch_size:    parseInt row.batch_size
      iters:         parseInt row.iters
      max_seq_length:parseInt row.max_seq_length
      grad_accum:    parseInt row.grad_accum
      learning_rate: parseFloat row.learning_rate
      adapter_path:  adapterPath

    # === Dispatch MLX LoRA training ===
    M.saveThis "mlx-lm:lora", payload

    # --- Wait for result ---
    mo = M.theLowdown "mlx-lm:lora"
    res = await mo.notifier

    if res?.error?
      throw new Error "LoRA training failed: #{res.error}"

    M.saveThis "train_kagnam:result", res
    M.saveThis "done:#{stepName}", true

    console.log "KAG LoRA training finished."
    return
