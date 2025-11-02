#!/usr/bin/env coffee
###
05_eval.coffee — strict memo-aware version (2025)
-------------------------------------------------
• Executes inside unified pipeline (shared @memo)
• Receives (M, stepName) directly — no env access
• Evaluates model outputs, selects best generation policy
• Produces report.md + generation_policy.json
###

fs  = require 'fs'
path = require 'path'
pd   = require 'danfojs-node'  # lightweight DataFrame lib

@step =
  desc: "Aggregate evaluation metrics and choose winner policy"

  action: (M, stepName) ->

    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    stepCfg = cfg[stepName]
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?
    runCfg  = cfg['run']
    throw new Error "Missing global 'run' section in experiment.yaml" unless runCfg?

    # --- Required keys ---
    for k in ['eval_dir','output_dir']
      throw new Error "Missing required run.#{k}" unless k of runCfg

    for k in ['ablations','report','policy']
      throw new Error "Missing required param '#{k}' in step '#{stepName}'" unless k of stepCfg

    EVAL_DIR = path.resolve(runCfg.eval_dir)
    OUT_DIR  = path.resolve(runCfg.output_dir)
    fs.mkdirSync(EVAL_DIR, {recursive:true})
    fs.mkdirSync(OUT_DIR, {recursive:true})

    ABL_JSONL = path.join(EVAL_DIR, "#{stepCfg.ablations}.jsonl")
    REPORT_MD = path.join(EVAL_DIR, stepCfg.report)
    POLICY_JS = path.join(EVAL_DIR, stepCfg.policy)

    unless fs.existsSync(ABL_JSONL)
      throw new Error "Missing #{ABL_JSONL} — ensure prior step produced ablations."

    # --- Load JSONL into DataFrame ---
    lines = fs.readFileSync(ABL_JSONL, 'utf8').split(/\r?\n/).filter (l)-> l.trim().length
    rows = lines.map (l)-> JSON.parse(l)
    df = new pd.DataFrame(rows)

    summarize = (subdf) ->
      n = subdf.shape[0]
      emptyRate = subdf['is_empty'].values.filter((x)->x).length / Math.max(1,n)
      sentEnd = subdf['generation'].values.map((g)->
        s = String(g or '').trim()
        s.endsWith('.') or s.endsWith('!') or s.endsWith('?') or s.endsWith('…')
      ).filter((x)->x).length / Math.max(1,n)
      lenWords = subdf['len_words'].values.map((x)-> Number(x) or 0)
      avgLen = lenWords.reduce((a,b)->a+b,0) / Math.max(1,lenWords.length)
      medLen = lenWords.slice().sort((a,b)->a-b)[Math.floor(lenWords.length/2)] or 0
      {n, empty_rate: emptyRate, sent_end_rate: sentEnd, avg_len: Math.round(avgLen*1000)/1000, med_len: medLen}

    # --- Group manually by (artifact, prompt_variant) ---
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
    aggSorted = agg.sortValues(['empty_rate','sent_end_rate','avg_len'], {ascending:[true,false,false]})
    winner = aggSorted.row(0).toJSON()
    runnerUp = if aggSorted.shape[0] > 1 then aggSorted.row(1).toJSON() else null

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

    fs.writeFileSync(REPORT_MD, linesMd.join("\n"), 'utf8')
    console.log "📘 Wrote #{REPORT_MD}"

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
    console.log "📗 Wrote #{POLICY_JS}"

    M.saveThis 'eval:policy', POLICY
    M.saveThis 'eval:report', linesMd.join("\n")
    M.saveThis "done:#{stepName}", true

    console.log "\n=== WINNER ==="
    console.log "model=#{winner.model_id} artifact=#{winner.artifact} prompt_variant=#{winner.prompt_variant}"
    if runnerUp?
      console.log "\n=== RUNNER-UP ==="
      console.log "model=#{runnerUp.model_id} artifact=#{runnerUp.artifact} prompt_variant=#{runnerUp.prompt_variant}"

    console.log "\n=== TABLE ==="
    console.log agg.toString()

    return