#!/usr/bin/env coffee
###
115_entropy.coffee — strict memo-aware (2025)
----------------------------------------------
STEP — Entropy Meter for Generation Confidence

Analyzes per-token uncertainty (Shannon entropy)
from MLX-LM stream_generate outputs.

Reads:
  - eval_out/generations.jsonl
  - run/artifacts.json
  - eval/policy.yaml (optional)

Writes:
  - eval_out/entropy_tokens.jsonl  (entropy per token)
  - eval_out/entropy_summary.csv   (aggregate stats per sample)
###

fs        = require 'fs'
path      = require 'path'
yaml      = require 'js-yaml'
{spawnSync} = require 'child_process'

@step =
  desc: "Compute token- and sample-level entropy from model generations (strict memo-aware)"

  action: (M, stepName) ->
    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    runCfg  = cfg['run']
    stepCfg = cfg[stepName]
    throw new Error "Missing 'run' section" unless runCfg?
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?

    for k in ['output_dir','data_dir','eval_dir']
      throw new Error "Missing required run.#{k}" unless k of runCfg

    EVAL_DIR = path.resolve(runCfg.eval_dir)
    RUN_DIR  = path.resolve(runCfg.output_dir)
    DATA_DIR = path.resolve(runCfg.data_dir)
    fs.mkdirSync(EVAL_DIR, {recursive:true})

    ARTIFACTS_JSON = path.join(DATA_DIR, 'artifacts.json')
    POLICY_FILE    = path.join(EVAL_DIR, 'policy.yaml')
    GEN_JSONL      = path.join(EVAL_DIR, 'generations.jsonl')
    TOK_PATH       = path.join(EVAL_DIR, 'entropy_tokens.jsonl')
    SUM_PATH       = path.join(EVAL_DIR, 'entropy_summary.csv')

    # --- Required files check ---
    for f in [ARTIFACTS_JSON, GEN_JSONL]
      unless fs.existsSync(f)
        throw new Error "Missing required input file: #{f}"

    # --- Parameters (strict) ---
    params = stepCfg.params
    throw new Error "Missing #{stepName}.params block" unless params?

    for k in ['max_new_tokens','stop_strings']
      throw new Error "Missing required #{stepName}.params.#{k}" unless k of params

    MAX_NEW = parseInt(params.max_new_tokens)
    STOP_STRS = params.stop_strings
    unless Array.isArray(STOP_STRS) and STOP_STRS.length
      throw new Error "stop_strings must be non-empty array"

    # --- Logging ---
    LOG_PATH = path.join(EVAL_DIR, "#{stepName}.log")
    log = (msg) ->
      stamp = new Date().toISOString().replace('T',' ').replace(/\.\d+Z$/,'')
      line  = "[#{stamp}] #{msg}"
      try fs.appendFileSync(LOG_PATH, line + '\n', 'utf8') catch e then null
      console.log line

    log "[INFO] Starting step #{stepName}"

    # -------------------------------------------------------------------
    #  Helpers
    # -------------------------------------------------------------------
    load_policy = ->
      if fs.existsSync(POLICY_FILE)
        yaml.load fs.readFileSync(POLICY_FILE, 'utf8')
      else
        {prompt_policy:{name:'plain'},artifact_preference:['quantized','fused','adapter']}

    load_prompts = (jsonlPath) ->
      rows = []
      for line in fs.readFileSync(jsonlPath, 'utf8').split(/\r?\n/)
        continue unless line.trim()
        try
          obj = JSON.parse(line)
          rows.push obj.prompt if obj.prompt?
        catch then null
      rows

    pick_artifact = (artifactsPath, policy) ->
      pref = policy.artifact_preference or ['quantized','fused','adapter']
      data = JSON.parse(fs.readFileSync(artifactsPath, 'utf8'))
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

    entropy_from_logprobs = (logprobs) ->
      vals = Array.from(logprobs)
      m = Math.max(...vals)
      exps = vals.map((v)-> Math.exp(v - m))
      Z = exps.reduce((a,b)->a+b, 0)
      ps = exps.map((e)-> e/(Z + 1e-12))
      -ps.reduce((a,p)-> a + p*Math.log(p+1e-12), 0)

    median = (xs) ->
      return 0 unless xs.length
      ys = xs.slice().sort((a,b)->a-b)
      n = ys.length
      if n % 2 then ys[Math.floor(n/2)] else 0.5*(ys[n/2-1]+ys[n/2])

    apply_prompt_policy = (prompt, policy) ->
      pp = policy.prompt_policy or {name:'plain'}
      switch pp.name
        when 'directive'
          "#{prompt}#{pp.directive?.suffix or ''}"
        when 'fewshot'
          fspec = pp.fewshot or {}
          prefix = fspec.prefix or ''
          joiner = fspec.joiner or '\n'
          suffix = fspec.suffix or '\n'
          shots  = fspec.shots or []
          "#{prefix}#{shots.join(joiner)}#{suffix}".replace '{prompt}', prompt
        else prompt

    # -------------------------------------------------------------------
    #  Main logic
    # -------------------------------------------------------------------
    policy = load_policy()
    prompts = load_prompts(GEN_JSONL)
    [model_path, adapter_path, artifact_label] = pick_artifact(ARTIFACTS_JSON, policy)

    log "[INFO] Using model: #{model_path}"
    log "[INFO] Adapter: #{adapter_path or '(none)'}"
    log "[INFO] Artifact label: #{artifact_label}"

    TOK = fs.createWriteStream(TOK_PATH, {encoding:'utf8'})
    SUM = fs.createWriteStream(SUM_PATH, {encoding:'utf8'})
    SUM.write "artifact,prompt_idx,tokens,mean_entropy,median_entropy,min_entropy,max_entropy\n"

    for i, prompt of prompts
      full_prompt = apply_prompt_policy(prompt, policy)
      cmd = [
        'python3', '-m', 'mlx_lm.generate',
        '--model', model_path,
        '--max-tokens', MAX_NEW,
        '--prompt', full_prompt
      ]
      if adapter_path? and adapter_path.length > 0
        cmd.push '--adapter', adapter_path

      result = spawnSync(cmd[0], cmd.slice(1), {encoding:'utf8'})
      if result.status isnt 0
        log "[FAIL] mlx_lm.generate failed: #{result.stderr}"
        continue

      try
        data = JSON.parse(result.stdout)
        Hs = data.entropies or []
        mean_H = Hs.reduce((a,b)->a+b,0)/(Hs.length or 1)
        med_H = median(Hs)
        min_H = Math.min(...Hs)
        max_H = Math.max(...Hs)
        SUM.write "#{artifact_label},#{i},#{Hs.length},#{mean_H.toFixed(4)},#{med_H.toFixed(4)},#{min_H.toFixed(4)},#{max_H.toFixed(4)}\n"
        for rec in data.records or []
          TOK.write JSON.stringify(rec) + '\n'
      catch err
        log "[WARN] Failed to parse entropy JSON for prompt #{i}: #{err.message}"

    TOK.end(); SUM.end()
    log "[OK] Wrote per-token: #{TOK_PATH}"
    log "[OK] Wrote per-sample: #{SUM_PATH}"

    M.saveThis "done:#{stepName}", true
    M.saveThis "#{stepName}:paths", {tokens:TOK_PATH, summary:SUM_PATH}
    return