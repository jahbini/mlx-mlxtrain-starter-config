#!/usr/bin/env coffee
###
042_examination.coffee — strict memo-aware version (2025)
---------------------------------------------------------
STEP — Regeneration Sanity Checks (artifact + prompt ablation)

• Diagnoses empty / degenerate outputs by varying:
   - Artifact type: quantized, fused, base+adapter
   - Prompt style: plain, directive, fewshot
• Outputs:
   eval_out/ablations.jsonl
   eval_out/ablations.yaml
###

fs      = require 'fs'
path    = require 'path'
yaml    = require 'js-yaml'
textwrap = require 'textwrap'
{ load, generate } = require 'mlx_lm'

@step =
  desc: "Run regeneration ablations (artifact × prompt variants)"

  action: (M, stepName) ->

    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    stepCfg = cfg[stepName]
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?
    runCfg  = cfg['run']
    throw new Error "Missing global 'run' section in experiment.yaml" unless runCfg?

    DATA_DIR  = path.resolve(runCfg.data_dir)
    OUT_DIR   = path.resolve(runCfg.output_dir)
    EVAL_DIR  = path.resolve(runCfg.eval_dir or path.join(OUT_DIR, 'eval_out'))
    fs.mkdirSync(EVAL_DIR, {recursive:true})

    ARTIFACTS = path.join(DATA_DIR, runCfg.artifacts)
    ABL_JSONL = path.join(EVAL_DIR, "#{stepCfg.ablations or 'ablations'}.jsonl")
    ABL_YAML  = path.join(EVAL_DIR, "#{stepCfg.ablations or 'ablations'}.yaml")

    ONLY_MODEL_ID = stepCfg.only_model_id or ''
    PROMPTS       = stepCfg.prompts or ['Share an important thought.']
    MAX_NEW_SHORT = stepCfg.max_new_short or 64
    MAX_NEW_LONG  = stepCfg.max_new_long or 128

    # --- Helpers ---------------------------------------------------
    readJSON = (p) -> JSON.parse(fs.readFileSync(p, 'utf8'))
    preview = (txt, width=120) -> textwrap.shorten(txt.replace(/\n/g, ' ⏎ '), width, {placeholder:'…'})

    loadRuns = ->
      unless fs.existsSync(ARTIFACTS)
        throw new Error "Missing artifacts.json (run register step first)."
      reg = readJSON(ARTIFACTS)
      runs = reg.runs or []
      if ONLY_MODEL_ID.length > 0
        runs = runs.filter((r)-> r.model_id is ONLY_MODEL_ID)
      throw new Error "No runs found in artifacts.json." unless runs.length
      runs

    pickArtifacts = (runEntry) ->
      out = []
      if runEntry.quantized_dir? then out.push [runEntry.quantized_dir, null, 'quantized']
      if runEntry.fused_dir?     then out.push [runEntry.fused_dir, null, 'fused']
      out.push [runEntry.model_id, runEntry.adapter_dir, 'base+adapter']
      seen = new Set()
      uniq = []
      for [m,a,label] in out
        key = "#{m}|#{a or ''}"
        continue if seen.has(key)
        seen.add(key)
        uniq.push [m,a,label]
      uniq

    # Prompt variants
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

    # --- MLX wrapper -----------------------------------------------
    runGeneration = (modelPath, adapterPath, prompts, maxNew) ->
      { model, tokenizer } = load(modelPath, adapter_path: adapterPath or null)
      outs = []
      for p in prompts
        txt = generate(model: model, tokenizer: tokenizer, prompt: p, max_tokens: maxNew)
        cont = if txt.startsWith(p) then txt.slice(p.length) else txt
        outs.push(cont.trim())
      meta =
        eos_token: tokenizer.eos_token or null
        eos_token_id: tokenizer.eos_token_id or null
        pad_token: tokenizer.pad_token or null
        pad_token_id: tokenizer.pad_token_id or null
      [outs, meta]

    # --- Main -------------------------------------------------------
    runs = loadRuns()
    stamp = new Date().toISOString().replace(/\.\d+Z$/, 'Z')
    rows = []

    for run in runs
      artList = pickArtifacts(run)
      for [modelPath, adapterPath, artLabel] in artList
        for [pvLabel, pvFn] in PROMPT_VARIANTS
          promptsV = PROMPTS.map(pvFn)
          [outsShort, meta] = runGeneration(modelPath, adapterPath, promptsV, MAX_NEW_SHORT)
          [outsLong, _]     = runGeneration(modelPath, adapterPath, promptsV, MAX_NEW_LONG)

          console.log "\n=== #{run.model_id} | #{artLabel} | #{pvLabel} | short ==="
          for i in [0...promptsV.length]
            console.log "- #{promptsV[i]}\n→ #{preview(outsShort[i] or '')}"

          console.log "\n=== #{run.model_id} | #{artLabel} | #{pvLabel} | long ==="
          for i in [0...promptsV.length]
            console.log "- #{promptsV[i]}\n→ #{preview(outsLong[i] or '')}"

          for [budget, outs] in [['short', outsShort], ['long', outsLong]]
            for i in [0...PROMPTS.length]
              p = PROMPTS[i]
              o = outs[i] or ''
              rows.push
                timestamp_utc: stamp
                model_id: run.model_id
                artifact: artLabel
                prompt_variant: pvLabel
                budget: budget
                model_path: modelPath
                adapter_path: adapterPath or ''
                eos_token: meta.eos_token
                eos_token_id: meta.eos_token_id
                prompt: p
                generation: o
                len_chars: o.length
                len_words: o.split(/\s+/).filter((x)->x.length).length
                is_empty: if o.trim().length is 0 then 1 else 0

    # --- Write outputs ---------------------------------------------
    fs.writeFileSync(ABL_JSONL, rows.map((r)-> JSON.stringify(r)).join('\n') + '\n', 'utf8')

    grouped = {}
    for r in rows
      pr = (r.prompt or '').trim()
      grouped[pr] ?= []
      grouped[pr].push(r)
    fs.writeFileSync(ABL_YAML, yaml.safeDump(grouped, {sortKeys:false, lineWidth:140}), 'utf8')

    console.log "\n📘 Ablation results written:"
    console.log "- JSONL: #{ABL_JSONL}"
    console.log "- YAML:  #{ABL_YAML}"
    console.log "Tip: Compare 'fused+fewshot' vs 'quantized+plain' to spot degeneracy."

    M.saveThis "examination:ablations", rows
    M.saveThis "done:#{stepName}", true
    return