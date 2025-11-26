# kag_oracle/finalize_tensors.coffee
# -------------------------------------------------------------
# LoRA safetensor accumulator — merges today's adapter into the
# cumulative finalized adapter.
#
# Inputs (from config):
#   run.new_adapter        = path to today's LoRA safetensor
#   run.finalized_adapter  = cumulative adapter to update
#
# Output:
#   Writes updated finalized_adapter back to disk
#   Writes log file via memo saveThis
#
# This step MUST run after lora_fuse (or lora_train if direct)
# -------------------------------------------------------------

fs = require 'fs'
path = require 'path'
CoffeeScript = require 'coffeescript'
CoffeeScript.register()
{ safeLoadTensors, safeSaveTensors } = require '../utils/safetensor_utils.coffee'

@step =
  desc: "Merge new LoRA adapter weights into cumulative finalized adapter"
  action: (M, stepName) ->

    console.log "\n=== finalize_tensors: starting ==="

    # ---------------------------------------------------------
    # Resolve config paths
    # ---------------------------------------------------------
    #spec   = M.M["experiment.yaml"]?.value or {}
    #runCfg = spec.run or {}

    cfg = M.theLowdown("experiment.yaml")?.value
    throw new Error "Missing experiment.yaml" unless cfg?

    runCfg = cfg.run

    newPath   = runCfg.adapter_safetensors
    finalPath = runCfg.finalized_tensor

    console.log "→ new adapter:    #{newPath}"
    console.log "→ finalized base: #{finalPath}"

    # ---------------------------------------------------------
    # Load both safetensors (sync)
    # ---------------------------------------------------------
    newAbs   = path.join(process.cwd(), newPath)
    finalAbs = path.join(process.cwd(), finalPath)

    unless fs.existsSync(newAbs)
      throw new Error "new_adapter does not exist: #{newAbs}"

    unless fs.existsSync(finalAbs)
      console.log "⚠️ No existing finalized adapter; initializing with new..."
      # copy the new one
      raw = fs.readFileSync(newAbs)
      fs.mkdirSync(path.dirname(finalAbs), {recursive:true})
      fs.writeFileSync(finalAbs, raw)
      M.saveThis "#{stepName}:log", "Initialized finalized adapter from new."
      return true

    console.log "→ loading tensors…"
    newT   = safeLoadTensors(newAbs)
    baseT  = safeLoadTensors(finalAbs)

    # ---------------------------------------------------------
    # Merge layers
    # ---------------------------------------------------------
    merged = {}
    addedLayers = []
    updatedLayers = []

    for own name, arrNew of newT
      if baseT[name]?
        # elementwise addition
        arrBase = baseT[name]
        if arrBase.length isnt arrNew.length
          console.log "⚠️ Shape mismatch for #{name}; skipping"
          continue

        out = new Float32Array(arrBase.length)
        for i in [0...arrBase.length]
          out[i] = arrBase[i] + arrNew[i]
        merged[name] = out
        updatedLayers.push name
      else
        # new layer appears
        merged[name] = arrNew
        addedLayers.push name

    # ---------------------------------------------------------
    # Preserve layers present only in base
    # ---------------------------------------------------------
    for own name, arrBase of baseT
      continue if merged[name]?
      merged[name] = arrBase

    # ---------------------------------------------------------
    # Save merged tensor map
    # ---------------------------------------------------------
    console.log "→ saving merged adapter to #{finalAbs}"
    safeSaveTensors(finalAbs, merged)

    # ---------------------------------------------------------
    # Log metadata for the pipeline
    # ---------------------------------------------------------
    log =
      merged_layers: updatedLayers
      added_layers:  addedLayers
      merged_count:  updatedLayers.length
      added_count:   addedLayers.length
      timestamp:     new Date().toISOString()

    M.saveThis "#{stepName}:log.json", log
    console.log "=== finalize_tensors: finished ==="

    return true
