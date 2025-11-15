#!/usr/bin/env coffee

# -------------------------------------------------------------------
# 042_examination.coffee — meta-aware, MLX-runner clean version
# -------------------------------------------------------------------

fs   = require 'fs'
path = require 'path'
yaml = require 'js-yaml'

@step =
  desc: "Run regeneration ablations using MLX memo agent"

  action: (M, stepName) ->

    # ---------------------------------------------------------------
    # Load experiment.yaml from memo (must be present)
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
    PROMPTS         = stepCfg.prompts or []
    ABLATIONS       = stepCfg.ablations
    MAX_SHORT       = stepCfg.max_new_short
    MAX_LONG        = stepCfg.max_new_long
    ONLY_MODEL_ID   = stepCfg.only_model_id

    # ---------------------------------------------------------------
    # Artifact registry
    # ---------------------------------------------------------------
    artPath = path.join(runCfg.data_dir, runCfg.artifacts)
    reg = JSON.parse(fs.readFileSync(artPath, 'utf8'))
    runs = reg.runs or []
    throw new Error "No runs in artifacts.json" unless runs.length

    if ONLY_MODEL_ID? and ONLY_MODEL_ID.length > 0
      runs = runs.filter (r) -> r.model_id is ONLY_MODEL_ID

    throw new Error "No matching runs" unless runs.length

    # ---------------------------------------------------------------
    # Pick artifacts for each runEntry
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
    # Prompt transformations
    # ---------------------------------------------------------------
    pvPlain = (p) -> p
    pvDirective = (p) -> "#{p}\n\nAnswer with a single important thought:"
    pvFewshot = (p) ->
      shots = [
        "The moon does not race the tide."
        "A river carves stone by lingering."
      ]
      "Proverbs:\n- #{shots.join('\n- ')}\n\n#{p}\n- "

    PROMPT_VARIANTS = [
      ['plain', pvPlain]
      ['directive', pvDirective]
      ['fewshot', pvFewshot]
    ]

    # ---------------------------------------------------------------
    # Helper to run MLX generation through memo
    # ---------------------------------------------------------------
    runOne = (modelPath, adapterPath, prompts, maxTokens) ->
      req =
        op: "generate"
        model_path: modelPath
        adapter_path: adapterPath
        prompt: null           # will be one prompt at a time
        max_tokens: maxTokens

      outs = []

      for p in prompts
        req.prompt = p
        M.saveThis "mlx-lm:generate", req
        mo = M.theLowdown "mlx-lm:generate"
        res = await mo.notifier

        if res?.error?
          throw new Error "mlx-lm.generate error: #{res.error}"

        # MLX generate returns {text: "..."} or equivalent
        txt = res.output or res.text or ""
        c = if txt.startsWith(p) then txt.slice(p.length) else txt
        outs.push c.trim()

      outs

    # ---------------------------------------------------------------
    # Main loop
    # ---------------------------------------------------------------
    allRows = []
    stamp = new Date().toISOString().replace(/\.\d+Z$/, 'Z')

    for re in runs
      arts = pickArtifacts(re)

      for [modelPath, adapterPath, artLabel] in arts
        for [pvLabel, pvFn] in PROMPT_VARIANTS

          promptsV = PROMPTS.map(pvFn)

          shortOuts = await runOne(modelPath, adapterPath, promptsV, MAX_SHORT)
          longOuts  = await runOne(modelPath, adapterPath, promptsV, MAX_LONG)

          # Build output rows (one per prompt per budget)
          for idx in [0...PROMPTS.length]
            p = PROMPTS[idx]
            s = shortOuts[idx] or ''
            l = longOuts[idx] or ''
            allRows.push
              timestamp_utc: stamp
              model_id: re.model_id
              artifact: artLabel
              prompt_variant: pvLabel
              budget: 'short'
              prompt: p
              generation: s
              len_chars: s.length
              len_words: s.split(/\s+/).filter((x)->x.length).length
              is_empty: if s.trim().length is 0 then 1 else 0

            allRows.push
              timestamp_utc: stamp
              model_id: re.model_id
              artifact: artLabel
              prompt_variant: pvLabel
              budget: 'long'
              prompt: p
              generation: l
              len_chars: l.length
              len_words: l.split(/\s+/).filter((x)->x.length).length
              is_empty: if l.trim().length is 0 then 1 else 0

    # ---------------------------------------------------------------
    # Dump results: JSONL + YAML through memo meta rules
    # ---------------------------------------------------------------
    jsonlKey = "#{ABLATIONS}.jsonl"
    yamlKey  = "#{ABLATIONS}.yaml"

    # JSONL wants array of strings or objects? Our JSONL meta writes {text:...}
    # So convert rows → JSON-lines-of-object manually
    M.saveThis jsonlKey, allRows.map (r) -> JSON.stringify(r)

    # For YAML grouping
    grouped = {}
    for r in allRows
      pr = (r.prompt or '').trim()
      grouped[pr] ?= []
      grouped[pr].push r

    M.saveThis yamlKey, yaml.safeDump(grouped, {sortKeys:false})

    # Mark completion
    M.saveThis "done:#{stepName}", true

    return
