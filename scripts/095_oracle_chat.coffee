#!/usr/bin/env coffee
###
095_oracle_chat.coffee — strict memo-aware (2025)
-------------------------------------------------
STEP — Simple chat REPL using current pipeline model.

Uses:
  - experiment.yaml
  - memo (M) to locate current artifact + policy
  - mlx_lm.generate backend

Usage:
  STEP_NAME=oracle_chat coffee scripts/095_oracle_chat.coffee
  or:
  coffee scripts/095_oracle_chat.coffee "your question here"
###

fs        = require 'fs'
path      = require 'path'
child     = require 'child_process'
readline  = require 'readline'

@step =
  desc: "Interactive chat using the current pipeline model"

  action: (M, stepName) ->
    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    runCfg  = cfg['run']
    evalCfg = cfg['eval']
    throw new Error "Missing 'run' section" unless runCfg?

    DATA_DIR = path.resolve(runCfg.data_dir or 'run/data')
    EVAL_DIR = path.resolve(evalCfg?.output_dir or path.join(DATA_DIR, 'eval'))
    ARTIFACTS_JSON = path.join(DATA_DIR, 'artifacts.json')
    POLICY_FILE    = path.join(EVAL_DIR, 'policy.yaml')

    unless fs.existsSync(ARTIFACTS_JSON)
      throw new Error "Missing artifacts.json"

    load_policy = ->
      if fs.existsSync(POLICY_FILE)
        require('js-yaml').load fs.readFileSync(POLICY_FILE, 'utf8')
      else
        {prompt_policy:{name:'plain'},artifact_preference:['quantized','fused','adapter']}

    pick_artifact = (artifactsPath, policy) ->
      pref = policy.artifact_preference or ['quantized','fused','adapter']
      data = JSON.parse(fs.readFileSync(artifactsPath,'utf8'))
      runs = data.runs or []
      throw new Error "No runs in artifacts.json" unless runs.length
      cands = []
      for run in runs by -1
        model_id = run.model_id?.trim() or ''
        adapter  = run.adapter_dir?.trim() or null
        fused    = run.fused_dir?.trim() or null
        quant    = run.quantized_dir?.trim() or null
        if quant and fs.existsSync(quant) then cands.push ['quantized', quant, null]
        if fused and fs.existsSync(fused) then cands.push ['fused', fused, null]
        if adapter and model_id then cands.push ['adapter', model_id, adapter]
      for want in pref
        for [lab,mpath,apath] in cands when lab is want
          return [mpath, apath, lab]
      cands[0]

    run_prompt = (model, adapter, prompt, max_tokens=256) ->
      args = ['-m','mlx_lm.generate','--model',model,'--prompt',prompt,'--max-tokens',max_tokens]
      if adapter? and adapter.length>0
        args.push '--adapter', adapter
      result = child.spawnSync('python3', args, {encoding:'utf8'})
      if result.status isnt 0
        console.error "[FAIL] mlx_lm.generate failed:\n", result.stderr
        return ''
      result.stdout.trim()

    # ----------------------------------------------------
    # Initialize
    # ----------------------------------------------------
    policy = load_policy()
    [model_path, adapter_path, label] = pick_artifact(ARTIFACTS_JSON, policy)

    console.log "🧭 Oracle Chat using model=#{model_path}"
    console.log "Adapter=#{adapter_path or '(none)'}  Label=#{label}"
    console.log "Type ':quit' or Ctrl-D to exit.\n"

    # ----------------------------------------------------
    # CLI arg or interactive loop
    # ----------------------------------------------------
    args = process.argv.slice(2)
    if args.length > 0
      prompt = args.join(' ')
      reply = run_prompt(model_path, adapter_path, prompt)
      console.log "💬 #{reply}"
      process.exit 0

    rl = readline.createInterface
      input: process.stdin
      output: process.stdout
      terminal: true

    ask = ->
      rl.question "You: ", (line) ->
        if not line or line.trim().toLowerCase() in [':quit','exit','quit']
          rl.close(); return
        reply = run_prompt(model_path, adapter_path, line)
        console.log "🤖 #{reply}\n"
        ask()
    ask()
    return