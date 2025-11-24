#!/usr/bin/env coffee
fs   = require 'fs'
path = require 'path'

@step =
  desc: "Fuse LoRA adapter into final model (memo-native)"

  action: (M, stepName) ->

    cfg     = M.theLowdown("experiment.yaml")?.value
    runCfg  = cfg.run
    stepCfg = cfg[stepName]

    throw new Error "Missing experiment.yaml" unless cfg?
    throw new Error "Missing run.loraLand"  unless runCfg.loraLand?

    land        = path.resolve(runCfg.loraLand)
    adapterDir  = path.join(land, "adapter")
    fusedDir    = path.join(land, "fused")
    fs.mkdirSync(fusedDir, {recursive:true})

    args =
      model: runCfg.model
      "adapter-path": adapterDir
      "save-path": fusedDir

    stdout = M.callMLX "fuse",args

    M.saveThis "#{stepName}:stdout", stdout
    M.saveThis "done:#{stepName}", true
    return
