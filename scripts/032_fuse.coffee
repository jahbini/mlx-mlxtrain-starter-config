#!/usr/bin/env coffee
###
032_fuse.coffee — strict memo-aware version (2025)
--------------------------------------------------
STEP — Fuse and Quantize Models (MLX pipeline)
  • Reads artifacts.json
  • Optionally fuses adapter + base
  • Quantizes fused model to mlx
  • Updates artifacts.json
  • Logs to <data_dir>/logs/032_fuse.log
###

fs      = require 'fs'
path    = require 'path'
crypto  = require 'crypto'
child   = require 'child_process'
shlex   = require 'shell-quote'
shutil  = require 'fs-extra'

@step =
  desc: "Fuse and quantize models, update artifacts.json"

  action: (M, stepName) ->

    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    stepCfg = cfg[stepName]
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?
    runCfg  = cfg['run']
    throw new Error "Missing 'run' section in experiment.yaml" unless runCfg?

    # --- Required keys ---
    for k in ['data_dir','output_dir','artifacts']
      throw new Error "Missing required run.#{k}" unless k of runCfg

    DATA_DIR  = path.resolve(runCfg.data_dir)
    OUT_DIR   = path.resolve(runCfg.output_dir)
    ARTIFACTS = path.join(DATA_DIR, runCfg.artifacts)
    LOG_DIR   = path.join(DATA_DIR, 'logs')
    fs.mkdirSync(LOG_DIR, {recursive:true})
    LOG_PATH  = path.join(LOG_DIR, "#{stepName}.log")

    log = (msg) ->
      stamp = new Date().toISOString().replace('T',' ').replace(/\.\d+Z$/,'')
      line  = "[#{stamp}] #{msg}"
      fs.appendFileSync(LOG_PATH, line + "\n")
      console.log line

    # --- Controls ---
    DO_FUSE = !!stepCfg.do_fuse
    Q_BITS  = parseInt(stepCfg.q_bits or 4)
    Q_GROUP = parseInt(stepCfg.q_group or 32)
    DTYPE   = stepCfg.dtype or 'float16'
    DRY_RUN = !!stepCfg.dry_run

    sha256File = (p) ->
      h = crypto.createHash('sha256')
      fd = fs.openSync(p, 'r')
      buf = Buffer.alloc(1024*1024)
      loop
        bytes = fs.readSync(fd, buf, 0, buf.length, null)
        break if bytes is 0
        h.update(buf.subarray(0, bytes))
      fs.closeSync(fd)
      h.digest('hex')

    listFiles = (root) ->
      out = []
      return out unless fs.existsSync(root)
      for file in fs.readdirSync(root)
        full = path.join(root, file)
        stats = fs.statSync(full)
        if stats.isDirectory()
          out = out.concat(listFiles(full))
        else
          out.push
            path: path.resolve(full)
            rel: path.relative(root, full)
            bytes: stats.size
            sha256: sha256File(full)
            mtime_utc: new Date(stats.mtime).toISOString().replace(/\.\d+Z$/,'Z')
      out

    runCmd = (cmd) ->
      log "[MLX] #{cmd}"
      if DRY_RUN
        log "DRY_RUN=True → not executing."
        return 0
      try
        child.execSync(cmd, {stdio:'inherit'})
        return 0
      catch e
        return e.status or 1

    unless fs.existsSync(ARTIFACTS)
      throw new Error "Missing artifacts.json (run register first)."

    registry = JSON.parse(fs.readFileSync(ARTIFACTS, 'utf8'))
    runs = registry.runs or []
    throw new Error "No runs found in artifacts.json." unless runs.length

    updated = false
    py = shlex.quote(process.argv[0])

    for entry in runs
      modelId = entry.model_id
      outputDir  = path.resolve(entry.output_root)
      adapterDir = path.resolve(entry.adapter_dir)
      fusedDir   = entry.fused_dir or path.join(outputDir, 'fused', 'model')

      # --- Fuse ------------------------------------------------------
      if DO_FUSE and not fs.existsSync(fusedDir)
        fs.mkdirSync(path.dirname(fusedDir), {recursive:true})
        cmdFuse = "#{py} -m mlx_lm fuse --model #{shlex.quote(modelId)} " +
                  "--adapter-path #{shlex.quote(adapterDir)} " +
                  "--save-path #{shlex.quote(fusedDir)}"
        log "=== FUSE #{modelId} ==="
        rc = runCmd(cmdFuse)
        if rc isnt 0
          log "❌ Fuse failed for #{modelId}"
          continue
        entry.fused_dir = path.resolve(fusedDir)
        entry.files ?= {}
        entry.files.fused = listFiles(fusedDir)
        updated = true
      else if fs.existsSync(fusedDir)
        entry.fused_dir = path.resolve(fusedDir)
        entry.files ?= {}
        entry.files.fused = listFiles(fusedDir)
      else
        log "Skipping quantize: missing fused dir for #{modelId}"
        continue

      # --- Quantize --------------------------------------------------
      qDir = path.join(outputDir, 'quantized')
      if fs.existsSync(qDir)
        log "Removing existing quantized dir: #{qDir}"
        shutil.removeSync(qDir)

      cmdQ = "#{py} -m mlx_lm convert --hf-path #{shlex.quote(fusedDir)} " +
             "--mlx-path #{shlex.quote(qDir)} " +
             "--q-bits #{Q_BITS} --q-group-size #{Q_GROUP} " +
             "--dtype #{shlex.quote(DTYPE)} -q"

      log "=== QUANTIZE #{modelId} ==="
      rc = runCmd(cmdQ)
      if rc isnt 0
        log "❌ Quantize failed for #{modelId}"
        continue

      entry.quantized_dir = path.resolve(qDir)
      entry.quantize_bits = Q_BITS
      entry.q_group_size  = Q_GROUP
      entry.files ?= {}
      entry.files.quantized = listFiles(qDir)
      updated = true

    if updated
      registry.updated_utc = new Date().toISOString().replace(/\.\d+Z$/,'Z')
      fs.writeFileSync(ARTIFACTS, JSON.stringify(registry, null, 2), 'utf8')

    log "=== FUSE/QUANTIZE SUMMARY ==="
    log "Wrote: #{ARTIFACTS}"
    for e in registry.runs or []
      log "- #{e.model_id}"
      if e.fused_dir?
        log "   fused_dir: #{e.fused_dir} (#{(e.files?.fused?.length) or 0} files)"
      if e.quantized_dir?
        log "   quantized_dir: #{e.quantized_dir} (q#{e.quantize_bits}, group=#{e.q_group_size}) " +
            "files=#{(e.files?.quantized?.length) or 0}"

    M.saveThis "fuse:artifacts", registry
    M.saveThis "done:#{stepName}", true
    return