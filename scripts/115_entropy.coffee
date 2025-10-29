#!/usr/bin/env coffee
###
115_entropy.coffee
----------------------------------------
STEP — Entropy Meter for Generation Confidence

Analyzes per-token uncertainty (Shannon entropy) from MLX-LM stream_generate.

Outputs:
  - eval_out/entropy_tokens.jsonl  (entropy per token)
  - eval_out/entropy_summary.csv   (aggregate stats per sample)
###

fs      = require 'fs'
path    = require 'path'
yaml    = require 'js-yaml'
{spawnSync} = require 'child_process'

# -------------------------------------------------------------------
# 1) Config loader
# -------------------------------------------------------------------
{load_config} = require '../config_loader'
CFG       = load_config()
STEP_NAME = process.env.STEP_NAME or 'entropy'
STEP_CFG  = CFG[STEP_NAME] or {}
PARAMS    = STEP_CFG.params or {}

EVAL_DIR  = path.resolve CFG.eval.output_dir
RUN_DIR   = path.resolve CFG.run.output_dir
ARTIFACTS = path.join RUN_DIR, CFG.data.artifacts
POLICY_JSON = path.join EVAL_DIR, CFG.eval.policy
GEN_JSONL = path.join EVAL_DIR, "#{CFG.eval.generations}.jsonl"
TOK_PATH  = path.join EVAL_DIR, 'entropy_tokens.jsonl'
SUM_PATH  = path.join EVAL_DIR, 'entropy_summary.csv'

fs.mkdirSync EVAL_DIR, {recursive:true}

MAX_NEW   = parseInt PARAMS.max_new_tokens or 128
STOP_STRS = PARAMS.stop_strings or ["\n\n","==="]

# -------------------------------------------------------------------
# 2) Helpers
# -------------------------------------------------------------------
load_policy = ->
  if fs.existsSync POLICY_JSON
    yaml.load fs.readFileSync(POLICY_JSON,'utf8')
  else
    {prompt_policy:{name:'plain'},artifact_preference:['quantized','fused','adapter']}

load_prompts = (jsonlPath) ->
  prompts = []
  for line in fs.readFileSync(jsonlPath,'utf8').split('\n')
    continue unless line.trim()
    try
      obj = JSON.parse(line)
      prompts.push obj.prompt if obj.prompt?
    catch err then continue
  prompts

pick_artifact = (artifactsPath, policy) ->
  pref = policy.artifact_preference or ['quantized','fused','adapter']
  data = JSON.parse fs.readFileSync(artifactsPath,'utf8')
  runs = data.runs or []
  throw new Error "No runs in artifacts.json" unless runs.length
  cands = []
  for run in runs by -1
    model_id = run.model_id?.trim() or ''
    adapter  = run.adapter_dir?.trim() or null
    fused    = run.fused_dir?.trim() or null
    quant    = run.quantized_dir?.trim() or null
    if quant and fs.existsSync quant then cands.push ['quantized',quant,null]
    if fused and fs.existsSync fused then cands.push ['fused',fused,null]
    if adapter and model_id then cands.push ['adapter',model_id,adapter]
  for want in pref
    for [lab,mpath,apath] in cands when lab is want
      return [mpath,apath,lab]
  cands[0]

entropy_from_logprobs = (logprobs) ->
  vals = Array.from logprobs
  m = Math.max.apply(null, vals)
  exps = vals.map (v)-> Math.exp(v - m)
  Z = exps.reduce((a,b)->a+b,0)
  ps = exps.map (e)-> e/(Z+1e-12)
  -ps.reduce((a,p)-> a + p*Math.log(p+1e-12),0)

trim_on_stops = (txt, stops) ->
  cut = txt.length
  for s in stops
    i = txt.indexOf s
    if i isnt -1 then cut = Math.min cut, i
  txt.slice 0, cut

median = (xs) ->
  return 0.0 unless xs.length
  ys = xs.sort (a,b)-> a-b
  n = ys.length
  h = Math.floor n/2
  if n%2 then ys[h] else 0.5*(ys[h-1]+ys[h])

apply_prompt_policy = (prompt, policy) ->
  pp = policy.prompt_policy or {name:'plain'}
  switch pp.name
    when 'directive'
      "#{prompt}#{pp.directive?.suffix or ''}"
    when 'fewshot'
      fs = pp.fewshot or {}
      prefix = fs.prefix or ''
      joiner = fs.joiner or '\n'
      suffix = fs.suffix or '\n'
      shots  = fs.shots or []
      "#{prefix}#{shots.join(joiner)}#{suffix}".replace '{prompt}', prompt
    else
      prompt

# -------------------------------------------------------------------
# 3) Main
# -------------------------------------------------------------------
main = ->
  policy = load_policy()
  prompts = load_prompts GEN_JSONL
  [model_path, adapter_path, artifact_label] = pick_artifact ARTIFACTS, policy

  console.log "[INFO] Using model=#{model_path}, adapter=#{adapter_path}, label=#{artifact_label}"

  # spawn mlx runner for entropy capture
  TOK = fs.createWriteStream TOK_PATH, encoding:'utf8'
  SUM = fs.createWriteStream SUM_PATH, encoding:'utf8'
  SUM.write "artifact,prompt_idx,tokens,mean_entropy,median_entropy,min_entropy,max_entropy\n"

  for i,prompt of prompts
    full_prompt = apply_prompt_policy prompt, policy
    cmd = [
      'python3'
      '-m', 'mlx_lm.generate'
      '--model', model_path
      '--adapter', adapter_path or ''
      '--max-tokens', MAX_NEW
      '--prompt', full_prompt
      '--entropy'  # hypothetical flag—future improvement
    ].filter(Boolean)
    result = spawnSync cmd[0], cmd.slice(1), {encoding:'utf8'}

    if result.status isnt 0
      console.error "[FAIL] mlx_lm.generate failed: #{result.stderr}"
      continue

    try
      data = JSON.parse result.stdout
      Hs = data.entropies or []
      mean_H = Hs.reduce((a,b)->a+b,0)/(Hs.length or 1)
      SUM.write "#{artifact_label},#{i},#{Hs.length},#{mean_H.toFixed(4)},#{median(Hs).toFixed(4)},#{Math.min(...Hs).toFixed(4)},#{Math.max(...Hs).toFixed(4)}\n"
      for rec in data.records or []
        TOK.write JSON.stringify(rec)+'\n'
    catch err
      console.error "[WARN] Failed to parse entropy JSON for prompt #{i}: #{err}"

  TOK.end(); SUM.end()
  console.log "[OK] Wrote per-token → #{TOK_PATH}"
  console.log "[OK] Wrote per-sample → #{SUM_PATH}"

main()