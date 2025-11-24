#!/usr/bin/env coffee
fs   = require 'fs'
path = require 'path'

@step =
  desc: "Run MLX LoRA training inside loraLand (memo-native)"

  action: (M, stepName) ->

    cfg    = M.theLowdown("experiment.yaml")?.value
    throw new Error "Missing experiment.yaml" unless cfg?

    runCfg  = cfg.run
    stepCfg = cfg[stepName]

    throw new Error "Missing run section"  unless runCfg?
    throw new Error "Missing step config" unless stepCfg?

    modelId   = runCfg.model
    land      = path.resolve(runCfg.loraLand)

    adapterDir = path.join(land, "adapter")
    fs.mkdirSync(adapterDir, {recursive:true})

    args =
        model: modelId
        train: ''
        data: runCfg.loraLand
        "adapter-path": adapterDir
        "batch-size": stepCfg.batch_size
        iters:      stepCfg.iters
        "max-seq-length": stepCfg.max_seq_length
        "learning-rate":  stepCfg.learning_rate
    console.log "JIM lora args", args
    stdout = M.callMLX "lora",args

    M.saveThis "#{stepName}:stdout", stdout
    M.saveThis "done:#{stepName}", true
    return
