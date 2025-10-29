###
  05_eval.coffee
  ---------------
  Direct CoffeeScript port of 05_eval.py
  ✅ Could later delegate metric aggregation to @mlx_runner or @policy_summarizer.

  Function:
    - Loads eval_out/ablations.jsonl
    - Computes per-artifact and prompt metrics
    - Selects winner + runner-up policies
    - Writes report.md and generation_policy.json
###

fs      = require 'fs'
path    = require 'path'
yaml    = require 'js-yaml'
os      = require 'os'
time    = require 'time-constants' ? null  # harmless
pd      = require 'danfojs-node'  # Pandas-like DataFrame library for Node

# --- Config ---
CFG_PATH  = process.env.CFG_OVERRIDE or path.join(process.cwd(), 'experiment.yaml')
STEP_NAME = process.env.STEP_NAME or 'eval'
cfgFull   = yaml.load(fs.readFileSync(CFG_PATH, 'utf8'))
STEP_CFG  = cfgFull[STEP_NAME] or {}
RUN_CFG   = cfgFull['run'] or {}
EVAL_CFG  = cfgFull['eval'] or {}
DATA_CFG  = cfgFull['data'] or {}

EVAL_DIR  = path.resolve(EVAL_CFG.output_dir or 'eval_out')
RUN_DIR   = path.resolve(RUN_CFG.output_dir or 'run_out')
fs.mkdirSync(EVAL_DIR, {recursive:true})
fs.mkdirSync(RUN_DIR, {recursive:true})

ARTIFACTS = path.join(RUN_DIR, DATA_CFG.artifacts or 'artifacts.json')
ABL_JSONL = path.join(EVAL_DIR, (EVAL_CFG.ablations or 'ablations') + '.jsonl')
REPORT_MD = path.join(EVAL_DIR, EVAL_CFG.report or 'report.md')
POLICY_JS = path.join(EVAL_DIR, EVAL_CFG.policy or 'generation_policy.json')

unless fs.existsSync(ABL_JSONL)
  console.error "Missing eval_out/ablations.jsonl (run Step 12)."
  process.exit(1)

# --- Load JSONL ---
lines = fs.readFileSync(ABL_JSONL, 'utf8').split(/\r?\n/).filter (l)-> l.trim().length
rows = lines.map (l)-> JSON.parse(l)
df = new pd.DataFrame(rows)

# --- Metrics per (artifact, prompt_variant) ---
summarize = (subdf) ->
  n = subdf.shape[0]
  emptyRate = subdf['is_empty'].values.filter((x)->x).length / Math.max(1,n)
  sentEnd = subdf['generation'].values.map((g)-> String(g or '').trim().endsWith('.') or String(g).trim().endsWith('!') or String(g).trim().endsWith('?') or String(g).trim().endsWith('…')).filter((x)->x).length / Math.max(1,n)
  lenWords = subdf['len_words'].values.map((x)-> Number(x) or 0)
  avgLen = lenWords.reduce((a,b)->a+b,0) / Math.max(1,lenWords.length)
  medLen = lenWords.slice().sort((a,b)->a-b)[Math.floor(lenWords.length/2)] or 0
  {n, empty_rate: emptyRate, sent_end_rate: sentEnd, avg_len: Math.round(avgLen*1000)/1000, med_len: medLen}

# Group + aggregate manually
grouped = {}
for i in [0...df.shape[0]]
  row = df.row(i).toJSON()
  key = "#{row.model_id}::#{row.artifact}::#{row.prompt_variant}"
  grouped[key] ?= []
  grouped[key].push row

aggRows = []
for k, arr of grouped
  first = arr[0]
  stats = summarize(new pd.DataFrame(arr))
  aggRows.push {
    model_id: first.model_id
    artifact: first.artifact
    prompt_variant: first.prompt_variant
    n: stats.n
    empty_rate: stats.empty_rate
    sent_end_rate: stats.sent_end_rate
    avg_len: stats.avg_len
    med_len: stats.med_len
  }

agg = new pd.DataFrame(aggRows)

# --- Winner/Runner-up ---
aggSorted = agg.sortValues(['empty_rate','sent_end_rate','avg_len'], {ascending:[true,false,false]})
winner = aggSorted.row(0).toJSON()
runnerUp = if aggSorted.shape[0] > 1 then aggSorted.row(1).toJSON() else null

# --- Markdown report ---
pct = (x)-> "#{(x*100).toFixed(1)}%"
table = agg.copy()
table['empty_rate'] = table['empty_rate'].values.map(pct)
table['sent_end_rate'] = table['sent_end_rate'].values.map(pct)

ts = new Date().toISOString().replace(/\.\d+Z$/, 'Z')
linesMd = []
linesMd.push "# Learning Ablation Report  \n_#{ts}_\n"
linesMd.push "## Summary by artifact × prompt_variant"
linesMd.push "\n| model | artifact | prompt_variant | n | empty_rate | sent_end_rate | avg_len | med_len |"
linesMd.push "|-------|----------|----------------|---:|-----------:|--------------:|--------:|--------:|"

for i in [0...table.shape[0]]
  r = table.row(i).toJSON()
  linesMd.push "| #{r.model_id} | #{r.artifact} | #{r.prompt_variant} | #{r.n} | #{r.empty_rate} | #{r.sent_end_rate} | #{r.avg_len} | #{r.med_len} |"

linesMd.push "\n## Chosen policy"
linesMd.push "\n### Winner"
linesMd.push "- **artifact**: `#{winner.artifact}`"
linesMd.push "- **prompt_variant**: `#{winner.prompt_variant}`"
linesMd.push "- Rationale: lowest empty rate, then prefer sentence endings and adequate length."

if runnerUp?
  linesMd.push "\n### Runner-up"
  linesMd.push "- **artifact**: `#{runnerUp.artifact}`"
  linesMd.push "- **prompt_variant**: `#{runnerUp.prompt_variant}`"

# sample outputs for winner (budget=long)
sample = df.query("artifact == '#{winner.artifact}' and prompt_variant == '#{winner.prompt_variant}' and budget == 'long'")
linesMd.push "\n## Sample outputs (winner policy)"
for i in [0...Math.min(5, sample.shape[0])]
  r = sample.row(i).toJSON()
  gen = String(r.generation or '').replace(/\n/g, ' ⏎ ')
  genShort = if gen.length > 160 then gen.slice(0,157)+'…' else gen
  linesMd.push "- **#{r.prompt}** → #{genShort}"

fs.writeFileSync(REPORT_MD, linesMd.join("\n"), 'utf8')
console.log "[OK] Wrote #{REPORT_MD}"

# --- Policy JSON ---
POLICY =
  created_utc: ts
  artifact_preference: [winner.artifact, 'fused', 'adapter']
  prompt_policy:
    name: winner.prompt_variant
    fewshot:
      shots: [
        "The moon does not race the tide.",
        "A river carves stone by lingering."
      ]
      prefix: "Proverbs:\n- "
      joiner: "\n- "
      suffix: "\n\n{prompt}\n- "
    directive:
      suffix: "\n\nAnswer with a single important thought:"

fs.writeFileSync(POLICY_JS, JSON.stringify(POLICY, null, 2), 'utf8')
console.log "[OK] Wrote #{POLICY_JS}"

# --- Console Preview ---
console.log "\n=== WINNER ==="
console.log "model=#{winner.model_id} --- artifact=#{winner.artifact}  prompt_variant=#{winner.prompt_variant}"
if runnerUp?
  console.log "\n=== RUNNER-UP ==="
  console.log "model=#{runnerUp.model_id} --- artifact=#{runnerUp.artifact}  prompt_variant=#{runnerUp.prompt_variant}"

console.log "\n=== TABLE ==="
console.log agg.toString()

# Optional memo save
try
  if global.M? and typeof global.M.saveThis is 'function'
    global.M.saveThis 'eval:policy', POLICY
    global.M.saveThis 'eval:report', linesMd.join("\n")
catch e
  console.warn "(memo skip)", e.message