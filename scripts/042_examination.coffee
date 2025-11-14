#!/usr/bin/env coffee

fs   = require 'fs'
path = require 'path'
yaml = require 'js-yaml'

@step =
  desc: "Run regeneration ablations (artifact × prompt variants)"

  action: (M, stepName) ->

    throw new Error "Missing stepName" unless stepName?

    cfg = M.theLowdown('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml" unless cfg?

    stepCfg = cfg[stepName]
    runCfg  = cfg.run

    # Intentionally no prechecks. If something is missing, it blows up where used.

    ARTIFACTS = path.join runCfg.artifacts
    runs = (JSON.parse fs.readFileSync(ARTIFACTS, 'utf8')).runs or []
    console.log "JIM runs",runs

    PROMPTS      = stepCfg.prompts
    MAX_NEW_SHORT = stepCfg.max_new_short
    MAX_NEW_LONG  = stepCfg.max_new_long
    ONLY_MODEL_ID = stepCfg.only_model_id

    if ONLY_MODEL_ID
      runs = runs.filter (r)-> r.model_id is ONLY_MODEL_ID

    # Where output should go. If missing → fail here (first use).
    ABL_BASENAME = runCfg.ablations

    jsonlPath = path.join( "#{ABL_BASENAME}.jsonl")
    yamlPath  = path.join("#{ABL_BASENAME}.yaml")

    rows = []

    # Prompt variants
    pv =
      plain:      (p)-> p
      directive:  (p)-> "#{p}\n\nAnswer with a single important thought:"
      fewshot:    (p)->
        "Proverbs:\n- The moon does not race the tide.\n- A river carves stone by lingering.\n\n#{p}\n- "

    promptVariants = Object.entries(pv)  # [['plain',fn], ['directive',fn], ...]

    # Artifact selector: remove duplicates
    pickArtifacts = (run) ->
      out = []
      if run.quantized_dir? then out.push {label:'quantized', model:run.quantized_dir, adapter:null}
      if run.fused_dir?     then out.push {label:'fused',     model:run.fused_dir,    adapter:null}
      out.push {label:'base+adapter', model:run.model_id, adapter:run.adapter_dir}
      uniq = {}
      final = []
      for a in out
        k = "#{a.model}|#{a.adapter or ''}"
        continue if uniq[k]
        uniq[k] = true
        final.push a
      final

    # Submit MLX request → await notifier → get result
    runOne = (modelPath, adapterPath, prompts, maxTokens) ->
      M.saveThis "mlx-lm:generate",
        op: "generate"
        model: modelPath
        adapter: adapterPath
        prompts: prompts
        max_tokens: maxTokens
      mo = M.theLowdown "mlx-lm:generate"
      res = await mo.notifier
      if res?.error?
        throw new Error "mlx-lm.generate error: #{res.error}"
      return res.outputs or []

    stamp = new Date().toISOString().replace(/\.\d+Z$/, 'Z')

    for run in runs
      arts = pickArtifacts(run)

      for art in arts
        for [pvLabel, pvFn] in promptVariants

          promptsV = PROMPTS.map pvFn

          # --- SHORT ---
          outsShort = await runOne(art.model, art.adapter, promptsV, MAX_NEW_SHORT)
          for i in [0...PROMPTS.length]
            o = outsShort[i] or ''
            rows.push
              timestamp_utc: stamp
              model_id: run.model_id
              artifact: art.label
              prompt_variant: pvLabel
              budget: 'short'
              prompt: PROMPTS[i]
              generation: o
              len_chars: o.length
              len_words: o.split(/\s+/).filter((x)->x).length
              is_empty: if o.trim().length is 0 then 1 else 0

          # --- LONG ---
          outsLong = await runOne(art.model, art.adapter, promptsV, MAX_NEW_LONG)
          for i in [0...PROMPTS.length]
            o = outsLong[i] or ''
            rows.push
              timestamp_utc: stamp
              model_id: run.model_id
              artifact: art.label
              prompt_variant: pvLabel
              budget: 'long'
              prompt: PROMPTS[i]
              generation: o
              len_chars: o.length
              len_words: o.split(/\s+/).filter((x)->x).length
              is_empty: if o.trim().length is 0 then 1 else 0

    # --- Outputs -----------------------------------------------------

    jsonlOut = rows.map((r)-> JSON.stringify(r)).join('\n') + '\n'
    M.saveThis jsonlPath, jsonlOut

    grouped = {}
    for r in rows
      pr = r.prompt.trim()
      grouped[pr] ?= []
      grouped[pr].push r

    M.saveThis yamlPath, yaml.safeDump(grouped, {sortKeys:false, lineWidth:140})

    M.saveThis "done:#{stepName}", true
    console.log "📘 Examination complete: #{jsonlPath}"
    return
