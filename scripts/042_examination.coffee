###
  042_examination.coffee
  -----------------------
  STEP 12 — Regeneration Sanity Checks (artifact + prompt ablation)
  ✅ Diagnoses empty / degenerate outputs by varying:
     - Artifact type: quantized, fused, base+adapter
     - Prompt style: plain, directive, fewshot
  ✅ Compatible with MLX 0.28+ (load + generate)
  ✅ Outputs:
       eval_out/ablations.jsonl
       eval_out/ablations.yaml
###

fs      = require 'fs'
path    = require 'path'
yaml    = require 'js-yaml'
textwrap = require 'textwrap'
{ load, generate } = require 'mlx_lm'

# --- STEP-AWARE CONFIG ---
CFG_PATH  = process.env.CFG_OVERRIDE or path.join(process.cwd(), 'experiment.yaml')
STEP_NAME = process.env.STEP_NAME or 'examination'
cfgFull   = yaml.load(fs.readFileSync(CFG_PATH, 'utf8'))
STEP_CFG  = cfgFull[STEP_NAME] or {}
RUN_CFG   = cfgFull['run'] or {}

OUT_DIR   = path.resolve(RUN_CFG.output_dir or 'run')
DATA_DIR  = path.resolve(RUN_CFG.data_dir or 'data')
EVAL_DIR  = path.resolve(RUN_CFG.eval_dir or path.join(OUT_DIR, 'eval_out'))
fs.mkdirSync(OUT_DIR, {recursive:true})
fs.mkdirSync(EVAL_DIR, {recursive:true})

ARTIFACTS = path.join(DATA_DIR, RUN_CFG.artifacts or 'artifacts.json')
CONTRACT  = path.join(OUT_DIR, RUN_CFG.contract or 'data_contract.json')

GEN_JSONL = path.join(EVAL_DIR, (RUN_CFG.generations or 'generations') + '.jsonl')
GEN_CSV   = path.join(EVAL_DIR, (RUN_CFG.generations or 'generations') + '.csv')
OUT_SUM   = path.join(EVAL_DIR, (STEP_CFG.summary or 'summary') + '.csv')
OUT_JSON  = path.join(EVAL_DIR, (STEP_CFG.analysis or 'analysis') + '.json')
ABL_PATH  = path.join(EVAL_DIR, (STEP_CFG.ablations or 'ablations') + '.jsonl')
ABL_YAML  = path.join(EVAL_DIR, (STEP_CFG.ablations or 'ablations') + '.yaml')

# --- Controls ---
ONLY_MODEL_ID = STEP_CFG.only_model_id or ''
PROMPTS       = STEP_CFG.prompts or ['Share an important thought.']
MAX_NEW_SHORT = STEP_CFG.max_new_short or 64
MAX_NEW_LONG  = STEP_CFG.max_new_long or 128

# --- Utilities ---
readJSON = (p) -> JSON.parse(fs.readFileSync(p, 'utf8'))
preview = (text, width=120) -> textwrap.shorten(text.replace(/\n/g, ' ⏎ '), width, {placeholder:'…'})

# --- Load artifact runs ---
loadRuns = ->
  unless fs.existsSync(ARTIFACTS)
    console.error "Missing artifacts.json"
    process.exit(1)
  reg = readJSON(ARTIFACTS)
  runs = reg.runs or []
  if ONLY_MODEL_ID.length > 0
    runs = runs.filter((r)-> r.model_id is ONLY_MODEL_ID)
  if runs.length is 0
    console.error "No matching runs in artifacts.json."
    process.exit(1)
  runs

# --- Artifact preference list ---
pickArtifacts = (runEntry) ->
  out = []
  if runEntry.quantized_dir?
    out.push [runEntry.quantized_dir, null, 'quantized']
  if runEntry.fused_dir?
    out.push [runEntry.fused_dir, null, 'fused']
  out.push [runEntry.model_id, runEntry.adapter_dir, 'base+adapter']
  seen = new Set()
  uniq = []
  for [m,a,label] in out
    key = "#{m}|#{a or ''}"
    continue if seen.has(key)
    seen.add(key)
    uniq.push [m,a,label]
  uniq

# --- Prompt variants ---
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

# --- Generation wrapper ---
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

# --- Main orchestration ---
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

      console.log "\n=== #{run.model_id} | #{artLabel} | #{pvLabel} | max_new=#{MAX_NEW_SHORT} ==="
      for p,o of promptsV
        console.log "- #{p}\n→ #{preview(o)}"

      console.log "\n=== #{run.model_id} | #{artLabel} | #{pvLabel} | max_new=#{MAX_NEW_LONG} ==="
      for p,o of promptsV
        console.log "- #{p}\n→ #{preview(o)}"

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

# --- Save JSONL ---
outTxt = rows.map((r)-> JSON.stringify(r)).join('\n') + '\n'
fs.writeFileSync(ABL_PATH, outTxt, 'utf8')
console.log "\nSaved detailed ablation outputs to #{ABL_PATH}"

# --- Save grouped YAML ---
grouped = {}
for r in rows
  prompt = (r.prompt or '').trim()
  grouped[prompt] ?= []
  grouped[prompt].push(r)
fs.writeFileSync(ABL_YAML, yaml.safeDump(grouped, {sortKeys:false, lineWidth:140}), 'utf8')

console.log "Wrote grouped YAML → #{ABL_YAML}"
console.log "Tip: Look for cases where 'fused' + 'fewshot' fills in while 'quantized' + 'plain' is empty."

# Optional memo save
try
  if global.M? and typeof global.M.saveThis is 'function'
    global.M.saveThis 'examination:ablations', rows
catch e
  console.warn "(memo skip)", e.message