#!/usr/bin/env coffee
###
011_metrics_eos.coffee — strict memo-aware version (2025)
---------------------------------------------------------
STEP — EOS Behavior Probe & Metrics Summary
  • Reads eval_out/generations.jsonl
  • Computes token diversity, length, EOS usage, memorization checks
  • Aggregates by mode and writes summary CSV + JSON analysis
  • Logs key console summaries
###

fs       = require 'fs'
path     = require 'path'
yaml     = require 'js-yaml'
crypto   = require 'crypto'

@step =
  desc: "Analyze EOS and lexical diversity from JSONL generations"

  action: (M, stepName) ->

    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    stepCfg = cfg[stepName]
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?
    runCfg  = cfg['run']
    dataCfg = cfg['data']
    evalCfg = cfg['eval']
    throw new Error "Missing required 'run', 'data', or 'eval' sections." unless runCfg? and dataCfg? and evalCfg?

    DATA_DIR  = path.resolve(runCfg.data_dir)
    EVAL_DIR  = path.resolve(evalCfg.output_dir or path.join(DATA_DIR, 'eval_out'))
    CONTRACT  = path.join(DATA_DIR, dataCfg.contract or 'data_contract.json')
    GEN_JSONL = path.join(EVAL_DIR, "#{evalCfg.generations or 'generations'}.jsonl")
    OUT_SUM   = path.join(EVAL_DIR, "#{evalCfg.summary or 'summary'}.csv")
    OUT_JSON  = path.join(EVAL_DIR, "#{evalCfg.analysis or 'analysis'}.json")
    fs.mkdirSync(EVAL_DIR, {recursive:true})

    unless fs.existsSync(GEN_JSONL)
      throw new Error "Missing generations.jsonl (run snapshot step first)."
    unless fs.existsSync(CONTRACT)
      throw new Error "Missing data_contract.json (from prepare_data step)."

    # --- Helpers ---------------------------------------------------
    readJSONL = (p) ->
      rows = []
      for line in fs.readFileSync(p, 'utf8').split(/\r?\n/)
        if line.trim().length
          try
            rows.push JSON.parse(line)
          catch e
            console.warn "(skip bad JSONL)", e.message
      rows

    endsWithTerm = (s) -> /[.!?…]$/.test(s.trim())
    hasTrailingWS = (s) -> s.length > 0 and /\s$/.test(s)
    distinctN = (tokens, n=1) ->
      return 0.0 if tokens.length < n
      grams = new Set()
      for i in [0..tokens.length-n]
        grams.add tokens.slice(i, i+n).join(' ')
      grams.size / Math.max(1, tokens.length-n+1)

    wordCount = (s) -> (s.trim().split(/\s+/)).length

    # --- Load generations + contract + train corpus ----------------
    rows = readJSONL(GEN_JSONL)
    contract = JSON.parse(fs.readFileSync(CONTRACT, 'utf8'))
    trainPath = path.resolve(contract?.filenames?.train?.resolved or '')
    textField = Object.keys(contract?.schema?.fields or {}).find((k)-> String(contract.schema.fields[k]).toLowerCase() is 'string') or 'text'

    trainLines = []
    if fs.existsSync(trainPath)
      for line in fs.readFileSync(trainPath, 'utf8').split(/\r?\n/)
        try
          obj = JSON.parse(line)
          t = obj[textField]
          if typeof t is 'string' and t.trim().length
            trainLines.push t.trim()
        catch e then continue

    trainBlob = trainLines.join('\n\n')
    trainSet  = new Set(trainLines)

    # --- Per-row metrics -------------------------------------------
    perRow = []
    for r in rows
      g = String(r.generation or '')
      toks = g.trim().split(/\s+/)
      d1 = distinctN(toks,1); d2 = distinctN(toks,2)
      exactMem = trainSet.has(g.trim())
      substrMem = (not exactMem) and g.trim().length >= 20 and trainBlob.includes(g.trim())
      perRow.push Object.assign {}, r,
        len_chars: g.length
        len_words: wordCount(g)
        ends_sentence: if endsWithTerm(g) then 1 else 0
        ends_whitespace: if hasTrailingWS(g) then 1 else 0
        distinct1: Number(d1.toFixed(4))
        distinct2: Number(d2.toFixed(4))
        memorized_exact: if exactMem then 1 else 0
        memorized_substring: if substrMem then 1 else 0

    # --- Aggregate by mode -----------------------------------------
    groupByMode = {}
    for r in perRow
      m = r.mode or 'unknown'
      groupByMode[m] ?= []
      groupByMode[m].push(r)

    aggRows = []
    for mode, arr of Object.entries(groupByMode)
      n = arr.length
      avg = (key) -> arr.reduce(((a,b)->a+(b[key] or 0)),0)/Math.max(1,n)
      med = (key) ->
        vals = arr.map((r)->r[key]).sort((a,b)->a-b)
        vals[Math.floor(vals.length/2)] or 0
      aggRows.push
        mode: mode
        n: n
        avg_len_chars: Number(avg('len_chars').toFixed(2))
        med_len_chars: Number(med('len_chars').toFixed(2))
        avg_len_words: Number(avg('len_words').toFixed(2))
        sent_end_rate: Number(avg('ends_sentence').toFixed(4))
        trailing_ws_rate: Number(avg('ends_whitespace').toFixed(4))
        distinct1_mean: Number(avg('distinct1').toFixed(4))
        distinct2_mean: Number(avg('distinct2').toFixed(4))
        mem_exact_rate: Number(avg('memorized_exact').toFixed(4))
        mem_sub_rate: Number(avg('memorized_substring').toFixed(4))

    # --- Save summary CSV ------------------------------------------
    csvOut = ["mode,n,avg_len_chars,med_len_chars,avg_len_words,sent_end_rate,trailing_ws_rate,distinct1_mean,distinct2_mean,mem_exact_rate,mem_sub_rate"]
    for r in aggRows
      line = [r.mode,r.n,r.avg_len_chars,r.med_len_chars,r.avg_len_words,r.sent_end_rate,r.trailing_ws_rate,r.distinct1_mean,r.distinct2_mean,r.mem_exact_rate,r.mem_sub_rate].join(',')
      csvOut.push(line)
    fs.writeFileSync(OUT_SUM, csvOut.join('\n'), 'utf8')

    # --- JSON analysis ---------------------------------------------
    analysis =
      created_utc: new Date().toISOString().replace(/\.\d+Z$/,'Z')
      by_mode: aggRows
      notes: [
        "JSONL used as ground truth (avoids NaN coercion).",
        "distinct* = lexical diversity over whitespace tokens.",
        "memorized_* = match or substring from training corpus."
      ]
    fs.writeFileSync(OUT_JSON, JSON.stringify(analysis, null, 2), 'utf8')

    # --- Console preview -------------------------------------------
    console.log "=== EOS / OUTPUT ANALYSIS (by mode) ==="
    for r in aggRows
      console.log "#{r.mode}: len_words=#{r.avg_len_words}, EOS=#{r.sent_end_rate}, distinct2=#{r.distinct2_mean}, memorized=#{r.mem_exact_rate}"

    M.saveThis "metrics:analysis", analysis
    M.saveThis "done:#{stepName}", true
    return