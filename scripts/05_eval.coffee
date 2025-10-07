#!/usr/bin/env coffee

###
STEP 13 — Comparative Report & Policy Lock-in (CoffeeScript version)

- Reads eval_out/ablations.jsonl + artifacts.json
- Computes metrics: empty_rate, sent_end_rate, avg_len, med_len
- Writes: eval_out/report.md and eval_out/generation_policy.json
###

fs       = require 'fs'
path     = require 'path'
_        = require 'lodash'

{load_config} = require './config_loader'
cfg       = load_config()
console.log "JIM PIPELINE",cfg.pipeline
STEP_NAME = process.env.STEP_NAME
STEP_CFG  = cfg.pipeline.steps[STEP_NAME]
PARAMS    = STEP_CFG?.params or {}

EVAL_DIR  = path.resolve PARAMS.eval_output_dir or cfg.eval.output_dir
RUN_DIR   = path.resolve PARAMS.run_dir or cfg.run.output_dir

fs.mkdirSync EVAL_DIR, {recursive:true}

ARTIFACTS = path.join RUN_DIR, PARAMS.artifacts or cfg.data.artifacts
ABL_JSONL = path.join EVAL_DIR, "#{PARAMS.ablations or cfg.eval.ablations}.jsonl"
REPORT_MD = path.join EVAL_DIR, PARAMS.report or cfg.eval.report
POLICY_JS = path.join EVAL_DIR, PARAMS.policy or cfg.eval.policy

unless fs.existsSync ABL_JSONL
  console.error "Missing ablations.jsonl (Step 12)"
  process.exit 1

# --- Load JSONL into array ---
rows = []
for line in fs.readFileSync(ABL_JSONL, 'utf8').split('\n')
  continue unless line.trim()
  rows.push JSON.parse line

# --- Helpers ---
pct = (x) -> "#{(x*100).toFixed(1)}%"

median = (arr) ->
  return 0 unless arr.length
  s = _.sortBy arr
  mid = Math.floor s.length/2
  if s.length % 2 then s[mid] else (s[mid-1]+s[mid]) / 2

# --- Group + summarize ---
groups = _.groupBy rows, (r) -> [r.model_id, r.artifact, r.prompt_variant].join('|')

agg = []
for key, g of groups
  n = g.length
  empty_rate    = _.sumBy(g, (x) -> if x.is_empty then 1 else 0) / n
  sent_end_rate = _.sumBy(g, (x) -> 
    gen = (x.generation or "").trim()
    if gen.match(/[.!?…]$/) then 1 else 0
  ) / n
  lens = g.map (x) -> x.len_words
  avg_len = _.mean(lens) or 0
  med_len = median(lens)

  [model_id, artifact, prompt_variant] = key.split('|')
  agg.push {model_id, artifact, prompt_variant, n, empty_rate, sent_end_rate, avg_len, med_len}

# --- Rank winner/runner-up ---
ranked = _.orderBy agg, ['empty_rate','sent_end_rate','avg_len'], ['asc','desc','desc']
winner    = ranked[0]
runner_up = if ranked.length > 1 then ranked[1] else null

# --- Markdown report ---
ts = new Date().toISOString().replace(/\.\d+Z$/,'Z')
lines = []
lines.push "# Learning Ablation Report\n_#{ts}_\n"
lines.push "## Summary by artifact × prompt_variant"
lines.push "| model | artifact | prompt_variant | n | empty_rate | sent_end_rate | avg_len | med_len |"
lines.push "|-------|----------|----------------|---:|-----------:|--------------:|--------:|--------:|"

for r in agg
  lines.push "| #{r.model_id} | #{r.artifact} | #{r.prompt_variant} | #{r.n} | #{pct r.empty_rate} | #{pct r.sent_end_rate} | #{r.avg_len.toFixed 3} | #{r.med_len} |"

lines.push "\n## Chosen policy"
lines.push "\n### Winner"
lines.push "- **artifact**: `#{winner.artifact}`"
lines.push "- **prompt_variant**: `#{winner.prompt_variant}`"
lines.push "- Rationale: lowest empty rate, then prefer sentence endings and adequate length."

if runner_up
  lines.push "\n### Runner-up"
  lines.push "- **artifact**: `#{runner_up.artifact}`"
  lines.push "- **prompt_variant**: `#{runner_up.prompt_variant}`"

# Sample outputs (winner, long budget)
samples = rows.filter (r) ->
  r.artifact is winner.artifact and r.prompt_variant is winner.prompt_variant and r.budget is 'long'

for s in _.uniqBy(samples, 'prompt')
  gen = (s.generation or "").replace(/\n/g,' ⏎ ')
  if gen.length > 160 then gen = gen.substring(0,157) + '…'
  lines.push "- **#{s.prompt}** → #{gen}"

fs.writeFileSync REPORT_MD, lines.join("\n"), 'utf8'
console.log "[OK] Wrote #{REPORT_MD}"

# --- Policy JSON ---
POLICY =
  created_utc: ts
  artifact_preference: [winner.artifact, "fused", "adapter"]
  prompt_policy:
    name: winner.prompt_variant
    fewshot:
      shots: [
        "The moon does not race the tide."
        "A river carves stone by lingering."
      ]
      prefix: "Proverbs:\n- "
      joiner: "\n- "
      suffix: "\n\n{prompt}\n- "
    directive:
      suffix: "\n\nAnswer with a single important thought:"

fs.writeFileSync POLICY_JS, JSON.stringify(POLICY,null,2), 'utf8'
console.log "[OK] Wrote #{POLICY_JS}"

# --- Console preview ---
console.log "\n=== WINNER ==="
console.log "model=#{winner.model_id} --- artifact=#{winner.artifact}  prompt_variant=#{winner.prompt_variant}"
if runner_up
  console.log "\n=== RUNNER-UP ==="
  console.log "model=#{runner_up.model_id} --- artifact=#{runner_up.artifact}  prompt_variant=#{runner_up.prompt_variant}"
console.log "\n=== TABLE ==="
for r in agg
  console.log "#{r.model_id} | #{r.artifact} | #{r.prompt_variant} | n=#{r.n} empty=#{pct r.empty_rate} sent_end=#{pct r.sent_end_rate} avg=#{r.avg_len.toFixed 3} med=#{r.med_len}"
