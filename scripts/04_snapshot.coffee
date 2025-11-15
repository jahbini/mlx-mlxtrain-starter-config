#!/usr/bin/env coffee

# -------------------------------------------------------------------
# 041_snapshot.coffee — MLX meta-runner snapshot generator
# Produces generations.jsonl + generations.yaml
# -------------------------------------------------------------------

fs   = require 'fs'
path = require 'path'
yaml = require 'js-yaml'

@step =
  desc: "Generate prompt snapshots using MLX (base, fused, quantized)"

  action: (M, stepName) ->

    # ---------------------------------------------------------------
    # Load experiment.yaml
    # ---------------------------------------------------------------
    cfg = M.theLowdown('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml" unless cfg?

    stepCfg = cfg[stepName]
    runCfg  = cfg['run']
    throw new Error "Missing step config '#{stepName}'" unless stepCfg?
    throw new Error "Missing run section" unless runCfg?

    # ---------------------------------------------------------------
    # Inputs from config (NO defaults silently inserted)
    # ---------------------------------------------------------------
    SNAP_NAME      = stepCfg.snapshots         # e.g. "generations"
    PROMPTS        = stepCfg.prompts or []
    MAX_NEW        = stepCfg.max_new_tokens
    ONLY_MODEL_ID  = stepCfg.only_model_id

    # ---------------------------------------------------------------
    # Artifact registry
    # ---------------------------------------------------------------
    artPath = path.join(runCfg.data_dir, runCfg.artifacts)
    reg = JSON.parse(fs.readFileSync(artPath, 'utf8'))
    runs = reg.runs or []
    throw new Error "No runs found in artifacts.json" unless runs.length

    if ONLY_MODEL_ID? and ONLY_MODEL_ID.length > 0
      runs = runs.filter (r) -> r.model_id is ONLY_MODEL_ID
    throw new Error "No matching runs" unless runs.length

    # ---------------------------------------------------------------
    # Artifact selection: quantized → fused → base+adapter
    # ---------------------------------------------------------------
    pickArtifacts = (re) ->
      out = []
      if re.quantized_dir? then out.push [re.quantized_dir, null, 'quantized']
      if re.fused_dir?     then out.push [re.fused_dir, null, 'fused']
      out.push [re.model_id, re.adapter_dir, 'base+adapter']
      uniq = []
      seen = new Set()
      for [m,a,label] in out
        key = "#{m}|#{a or ''}"
        continue if seen.has(key)
        seen.add(key)
        uniq.push [m,a,label]
      uniq

    # ---------------------------------------------------------------
    # Helper to run MLX text generation through memo
    # ---------------------------------------------------------------
    runOne = (modelPath, adapterPath, prompts, maxTokens) ->
      req =
        op: "generate"
        model_path: modelPath
        adapter_path: adapterPath
        prompt: null
        max_tokens: maxTokens

      outs = []

      for p in prompts
        req.prompt = p
        M.saveThis "mlx-lm:generate", req
        mo = M.theLowdown "mlx-lm:generate"
        res = await mo.notifier

        if res?.error?
          throw new Error "mlx-lm.generate error: #{res.error}"

        txt = res.output or res.text or ""
        c = if txt.startsWith(p) then txt.slice(p.length) else txt
        outs.push c.trim()

      outs

    # ---------------------------------------------------------------
    # Main snapshot loop
    # ---------------------------------------------------------------
    allRows = []
    stamp = new Date().toISOString().replace(/\.\d+Z$/, 'Z')

    for re in runs
      arts = pickArtifacts(re)

      for [modelPath, adapterPath, artLabel] in arts
        outs = await runOne(modelPath, adapterPath, PROMPTS, MAX_NEW)

        for idx in [0...PROMPTS.length]
          p = PROMPTS[idx]
          g = outs[idx] or ''

          allRows.push
            timestamp_utc: stamp
            model_id: re.model_id
            artifact: artLabel
            prompt: p
            generation: g
            len_chars: g.length
            len_words: g.split(/\s+/).filter((x)->x.length).length
            is_empty: if g.trim().length is 0 then 1 else 0

    # ---------------------------------------------------------------
    # Write snapshot outputs: JSONL + YAML via memo
    # ---------------------------------------------------------------
    jsonlKey = "#{SNAP_NAME}.jsonl"
    yamlKey  = "#{SNAP_NAME}.yaml"

    # JSONL: provide object-per-line as strings, letting JSONL-meta wrap
    M.saveThis jsonlKey, allRows.map (r) -> JSON.stringify(r)

    # YAML grouped by prompt
    grouped = {}
    for r in allRows
      pr = (r.prompt or '').trim()
      grouped[pr] ?= []
      grouped[pr].push r

    M.saveThis yamlKey, yaml.safeDump(grouped, {sortKeys:false})

    M.saveThis "done:#{stepName}", true
    return
