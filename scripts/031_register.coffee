#!/usr/bin/env coffee
###
031_register.coffee — strict memo-aware version (2025)
------------------------------------------------------
STEP — Build artifact registry from LoRA runs.

Merges:
• experiments.csv
• adapter + log files → SHA256 registry
• latest symlinks
Writes → artifacts.json
###

fs      = require 'fs'
path    = require 'path'
crypto  = require 'crypto'

@step =
  desc: "Register LoRA training artifacts and build registry"

  action: (M, stepName) ->

    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    stepCfg = cfg[stepName]
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?
    runCfg = cfg['run']
    throw new Error "Missing global 'run' section in experiment.yaml" unless runCfg?

    # --- Required keys ---
    for k in ['data_dir','output_dir','experiments_csv','artifacts']
      throw new Error "Missing required run.#{k}" unless k of runCfg

    DATA_DIR  = path.resolve(runCfg.data_dir)
    OUT_DIR   = path.resolve(runCfg.output_dir)
    fs.mkdirSync(DATA_DIR, {recursive:true})
    fs.mkdirSync(OUT_DIR, {recursive:true})

    EXPERIMENTS_CSV = path.join(DATA_DIR, runCfg.experiments_csv)
    ARTIFACTS_JSON  = path.join(DATA_DIR, runCfg.artifacts)

    sha256File = (p) ->
      h = crypto.createHash('sha256')
      f = fs.openSync(p, 'r')
      buf = Buffer.alloc(1024*1024)
      loop
        bytes = fs.readSync(f, buf, 0, buf.length, null)
        break if bytes is 0
        h.update buf.subarray(0, bytes)
      fs.closeSync(f)
      h.digest('hex')

    gatherDirFiles = (root) ->
      out = []
      return out unless fs.existsSync(root)
      for relPath in fs.readdirSync(root)
        full = path.join(root, relPath)
        stats = fs.statSync(full)
        if stats.isDirectory()
          sub = gatherDirFiles(full)
          out = out.concat(sub)
        else
          out.push
            path: path.resolve(full)
            rel: path.relative(root, full)
            bytes: stats.size
            sha256: sha256File(full)
            mtime_utc: new Date(stats.mtime).toISOString().replace(/\.\d+Z$/,'Z')
      out

    loadRows = (p) ->
      unless fs.existsSync(p)
        throw new Error "experiments.csv not found (run train step first)."
      txt = fs.readFileSync(p, 'utf8').split(/\r?\n/)
      hdr = null
      rows = []
      for line in txt when line.trim().length
        cols = line.split(',')
        if not hdr then hdr = cols; continue
        row = {}
        for i in [0...hdr.length]
          row[hdr[i].trim()] = cols[i]?.trim() or ''
        rows.push(row)
      rows

    rows = loadRows(EXPERIMENTS_CSV)
    registry =
      created_utc: new Date().toISOString().replace(/\.\d+Z$/,'Z')
      runs: []

    for r in rows
      modelId = r.model_id
      modelTag = modelId.replace(/\//g, '--')
      outRoot = path.join(DATA_DIR, modelTag)
      adapterDir = path.resolve(r.adapter_path)
      logsDir = path.resolve(r.log_dir)

      fusedDir = path.join(outRoot, 'fused', 'model')
      quantDir = path.join(outRoot, 'quantized', 'model')
      fs.mkdirSync(path.dirname(fusedDir), {recursive:true})
      fs.mkdirSync(path.dirname(quantDir), {recursive:true})

      # Symlinks
      try
        latestAdapter = path.join(outRoot, 'latest_adapter')
        if fs.existsSync(latestAdapter) then fs.unlinkSync(latestAdapter)
        fs.symlinkSync(path.basename(adapterDir), latestAdapter)
      catch e
        console.warn "(symlink adapter)", e.message

      try
        latestLogs = path.join(outRoot, 'latest_logs')
        if fs.existsSync(latestLogs) then fs.unlinkSync(latestLogs)
        fs.symlinkSync(path.basename(logsDir), latestLogs)
      catch e
        console.warn "(symlink logs)", e.message

      entry =
        model_id: modelId
        output_root: path.resolve(outRoot)
        adapter_dir: path.resolve(adapterDir)
        logs_dir: path.resolve(logsDir)
        fused_dir: path.resolve(fusedDir)
        quantized_dir: path.resolve(quantDir)
        files:
          adapter: gatherDirFiles(adapterDir)
          logs: gatherDirFiles(logsDir)
        training_params:
          iters: parseInt(r.iters or 0)
          batch_size: parseInt(r.batch_size or 0)
          max_seq_length: parseInt(r.max_seq_length or 0)

      registry.runs.push(entry)

    fs.writeFileSync(ARTIFACTS_JSON, JSON.stringify(registry, null, 2), 'utf8')
    console.log "📦 Wrote artifact registry: #{ARTIFACTS_JSON}"

    M.saveThis "register:artifacts", registry
    M.saveThis "done:#{stepName}", true
    return