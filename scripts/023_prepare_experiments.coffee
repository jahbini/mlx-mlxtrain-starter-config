#!/usr/bin/env coffee
###
022_prepare_experiment.coffee — strict memo-aware version (2025)
----------------------------------------------------------------
STEP — Build experiment_manifest.json by merging:
• data_contract.json
• prompt_policy.json
• global run configuration
###

fs   = require 'fs'
path = require 'path'

@step =
  desc: "Fuse contract + prompt policy into a single experiment manifest"

  action: (M, stepName) ->

    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    stepCfg = cfg[stepName]
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?
    runCfg  = cfg['run']
    throw new Error "Missing global 'run' section in experiment.yaml" unless runCfg?

    # --- Required keys ---
    for k in ['data_dir','contract','prompt_policy','experiment_manifest']
      throw new Error "Missing required run.#{k}" unless k of runCfg

    DATA_DIR  = path.resolve(runCfg.data_dir)
    CONTRACT  = path.join(DATA_DIR, runCfg.contract)
    POLICY    = path.join(DATA_DIR, runCfg.prompt_policy)
    OUT_PATH  = path.join(DATA_DIR, runCfg.experiment_manifest)

    fs.mkdirSync(DATA_DIR, {recursive:true})

    readJSON = (p) ->
      try
        JSON.parse(fs.readFileSync(p, 'utf8'))
      catch e
        console.error "Failed to read JSON:", p, e.message
        {}

    contract = readJSON(CONTRACT)
    policy   = readJSON(POLICY)

    manifest =
      created_utc: new Date().toISOString().replace(/\.\d+Z$/, 'Z')
      run:
        output_dir: runCfg.output_dir
        data_dir: runCfg.data_dir
        eval_dir: runCfg.eval_dir
        snapshot_dir: runCfg.snapshot_dir
        experiments_csv: runCfg.experiments_csv
      contract:
        path: CONTRACT
        schema: contract.schema
        data_dir: contract.data_dir
        files: contract.filenames
      prompt_policy:
        template_name: policy.template_name
        stop_strings: policy.stop_strings
        use_eos_token: policy.use_eos_token
        text_field: policy.text_field
      notes: [
        "This manifest consolidates data + prompt configuration into a single reference point.",
        "All downstream steps (train, snapshot, eval) can read this file for consistency."
      ]

    fs.writeFileSync(OUT_PATH, JSON.stringify(manifest, null, 2), 'utf8')
    console.log "📘 Wrote #{OUT_PATH}"

    console.log "\n=== EXPERIMENT MANIFEST ==="
    console.log "Data dir:   #{manifest.contract.data_dir}"
    console.log "Template:   #{manifest.prompt_policy.template_name}"
    console.log "Stop tokens:", (manifest.prompt_policy.stop_strings or []).join(', ') or '(none)'
    console.log "EOS token:  #{manifest.prompt_policy.use_eos_token}"
    console.log "Schema keys:", Object.keys(manifest.contract.schema?.fields or {}).join(', ') or '(none)'
    console.log "Files:", Object.keys(manifest.contract.files or {}).join(', ') or '(none)'

    M.saveThis "prepare_experiment:manifest", manifest
    M.saveThis "done:#{stepName}", true
    return