#!/usr/bin/env coffee
###
032_fuse.coffee — clean memo-native version
-------------------------------------------
STEP — Fuse and Quantize Models
  • Reads artifacts.json from memo (not disk)
  • Runs mlx_lm fuse + convert as needed
  • Updates and re-saves artifacts in memo
###

fs = require 'fs'
path = require 'path'
child = require 'child_process'

@step =
  desc: "Fuse and quantize models (lightweight, memo-native)"

  action: (M, stepName) ->

    throw new Error "Missing stepName argument" unless stepName?
    cfg = M.theLowdown('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    stepCfg = cfg[stepName]
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?
    runCfg = cfg.run
    throw new Error "Missing run section in config" unless runCfg?

    ART_PATH = runCfg.artifacts
    registry = M.theLowdown(ART_PATH)?.value
    throw new Error "Missing artifacts in memo (#{ART_PATH})" unless registry?

    runs = registry.runs or []
    throw new Error "No runs found in artifacts registry" unless runs.length

    DO_FUSE  = !!stepCfg.do_fuse
    DRY_RUN  = !!stepCfg.dry_run
    Q_BITS   = parseInt(stepCfg.q_bits or 4)
    Q_GROUP  = parseInt(stepCfg.q_group or 32)
    DTYPE    = stepCfg.dtype or 'float16'
    PYTHON   = process.env.PYTHON_EXECUTABLE or 'python'

    log = (msg) -> console.log "[fuse] #{msg}"

    runCmd = (cmd) ->
      log cmd
      return 0 if DRY_RUN
      try
        child.execSync(cmd, {stdio:'inherit'})
        0
      catch e
        console.error "❌ #{cmd} failed", e.status
        e.status or 1

    for entry in runs
      modelId = entry.model_id
      adapterDir = entry.adapter_dir
      fusedDir   = entry.fused_dir or path.join(path.dirname(ART_PATH), 'fused')
      quantDir   = entry.quantized_dir or path.join(path.dirname(ART_PATH), 'quantized')

      if DO_FUSE
        cmdFuse = "#{PYTHON} -m mlx_lm fuse --model '#{modelId}' " +
                  "--adapter-path '#{adapterDir}' --save-path '#{fusedDir}'"
        rc = runCmd(cmdFuse)
        if rc is 0
          entry.fused_dir = fusedDir
          log "✅ fused → #{fusedDir}"
        else
          log "❌ fuse failed for #{modelId}"
          continue

      cmdQ = "#{PYTHON} -m mlx_lm convert --hf-path '#{fusedDir}' " +
             "--mlx-path '#{quantDir}' --q-bits #{Q_BITS} --q-group-size #{Q_GROUP} " +
             "--dtype #{DTYPE} -q"

      rc = runCmd(cmdQ)
      if rc is 0
        entry.quantized_dir = quantDir
        entry.quantize_bits = Q_BITS
        entry.q_group_size = Q_GROUP
        log "✅ quantized → #{quantDir}"
      else
        log "❌ quantize failed for #{modelId}"

    registry.updated_utc = new Date().toISOString()
    M.saveThis ART_PATH, registry
    M.saveThis "done:#{stepName}", true
    log "📘 Updated artifacts in memo."
    return
