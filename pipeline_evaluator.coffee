#!/usr/bin/env coffee
###
  courtroom_judge.coffee
  Top-level judging pipeline.

  What it does:
  - Given a "courtroom" directory containing several completed training runs (each with experiment.yaml),
    run the evaluation pipeline *inside each run directory* (so it uses that run's experiment.yaml),
    then collect metrics from eval_out/summary.csv and produce overall judgements in the courtroom dir.

  Requirements:
  - Your existing evaluation pipeline script: scripts/pipeline_evaluator.coffee
  - Each candidate run dir has:
      experiment.yaml
      eval_out/ (will be created/updated by evaluator)
  - Node + CoffeeScript installed.

  Usage:
    coffee scripts/courtroom_judge.coffee /path/to/courtroom

  Environment:
    EXEC must point to repo root (so evaluator can find recipes & scripts).
###
fs        = require 'fs'
path      = require 'path'
{ spawn } = require 'child_process'

# --------------------------------------
# Small helpers
# --------------------------------------
banner = (msg) -> console.log "\n=== #{msg} ==="
trimNL = (s) -> String(s ? '').replace(/\r?\n+$/,'')
toFixed4 = (x) -> Number.isFinite(+x) and (+x).toFixed(4) or ''

# Simple CSV reader (assumes no quoted commas in numeric files)
readCsv = (p) ->
  txt = fs.readFileSync(p, 'utf8')
  lines = txt.split(/\r?\n/).filter (l)-> l.trim().length
  return [] unless lines.length
  headers = lines[0].split(',').map (h)-> h.trim()
  rows = []
  for line in lines.slice(1)
    cols = line.split(',').map (c)-> c.trim()
    obj = {}
    for i in [0...headers.length]
      obj[headers[i]] = cols[i] ? ''
    rows.push obj
  rows

# Spawn helper (Promise)
spawnP = (cmd, args=[], opts={}) ->
  new Promise (resolve, reject) ->
    proc = spawn(cmd, args, opts)
    outbuf = []; errbuf = []
    proc.stdout?.on 'data', (b)-> outbuf.push String(b)
    proc.stderr?.on 'data', (b)-> errbuf.push String(b)
    proc.on 'error', (e)-> reject e
    proc.on 'exit', (code) ->
      stdout = trimNL(outbuf.join(''))
      stderr = trimNL(errbuf.join(''))
      if code is 0 then resolve {code,stdout,stderr} else reject new Error("#{cmd} #{args.join(' ')} failed (#{code})\n#{stderr}")

# --------------------------------------
# Core
# --------------------------------------
main = ->
  EXEC = process.env.EXEC
  unless EXEC? and fs.existsSync(path.join(EXEC, 'recipes'))
    console.error "❌ EXEC must be set to your repo root (and contain recipes/)."
    process.exit(1)

  courtroom = process.argv[2] ? process.cwd()
  courtroom = path.resolve(courtroom)
  unless fs.existsSync(courtroom)
    console.error "❌ Courtroom directory not found:", courtroom
    process.exit(1)

  banner "Courtroom: #{courtroom}"

  # discover candidate run dirs (must contain experiment.yaml)
  entries = fs.readdirSync(courtroom, {withFileTypes:true})
  candidates = entries
    .filter (d)-> d.isDirectory()
    .map (d)-> path.join(courtroom, d.name)
    .filter (dir)-> fs.existsSync(path.join(dir, 'experiment.yaml'))

  if candidates.length is 0
    console.log "No candidate run directories found (need subdirs with experiment.yaml)."
    process.exit(0)

  banner "Found #{candidates.length} candidate(s)"
  for c in candidates
    console.log " •", c

  # run evaluator per candidate, sequentially
  results = []
  for runDir in candidates
    banner "Evaluating: #{runDir}"

    # Always run evaluator inside the run directory so load_config() sees experiment.yaml
    # Uses your existing evaluation pipeline; it will write eval_out/* inside runDir
    try
      await spawnP 'coffee', [ path.join(EXEC, 'pipeline_evaluator2.coffee') ],
        cwd: runDir
        env: Object.assign({}, process.env, { EXEC })  # ensure evaluator can find repo files
      console.log "✅ Evaluation OK:", runDir
    catch e
      console.error "❌ Evaluation failed for", runDir
      console.error String(e?.message or e)
      # we still attempt to read any partial eval_out; continue

    # Collect metrics from eval_out/summary.csv (from 11_eos_analysis)
    sumCsv = path.join(runDir, 'eval_out', 'summary.csv')
    if not fs.existsSync(sumCsv)
      console.warn "⚠️  Missing eval_out/summary.csv in", runDir
      continue

    rows = readCsv(sumCsv)
    if rows.length is 0
      console.warn "⚠️  Empty summary.csv in", runDir
      continue

    # Pick the row with the largest n as the primary (if multiple modes)
    best = rows.slice().sort (a,b) ->
      nb = parseFloat(b.n ? '0'); na = parseFloat(a.n ? '0')
      nb - na
    [primary] = best

    # Parse numeric metrics we care about
    parseF = (x)-> parseFloat(x ? '0') or 0
    emptyRate = parseF(primary.empty_rate)
    sentEnd   = parseF(primary.sent_end_rate)
    avgLen    = parseF(primary.avg_len_words)

    # Rank key identical to your earlier heuristic:
    #   lower empty_rate, then higher sent_end_rate, then higher avg_len_words
    # We'll store components; sorting happens later.
    results.push
      run_dir: runDir
      name: path.basename(runDir)
      mode: primary.mode ? ''
      n: parseInt(primary.n ? '0') or 0
      empty_rate: +toFixed4(emptyRate)
      sent_end_rate: +toFixed4(sentEnd)
      avg_len_words: +toFixed4(avgLen)
      summary_path: sumCsv

  if results.length is 0
    console.log "No usable results found."
    process.exit(0)

  # Sort by heuristic (empty_rate ASC, sent_end_rate DESC, avg_len_words DESC)
  results.sort (a,b) ->
    if a.empty_rate isnt b.empty_rate then a.empty_rate - b.empty_rate \
    else if a.sent_end_rate isnt b.sent_end_rate then b.sent_end_rate - a.sent_end_rate \
    else b.avg_len_words - a.avg_len_words

  # Write courtroom-level judgement files
  outJson = path.join(courtroom, 'judgement_summary.json')
  outCsv  = path.join(courtroom, 'judgement_summary.csv')
  outMd   = path.join(courtroom, 'judgement_summary.md')

  fs.writeFileSync outJson, JSON.stringify(results, null, 2), 'utf8'

  # CSV
  csvLines = []
  csvLines.push "rank,name,run_dir,mode,n,empty_rate,sent_end_rate,avg_len_words,summary_path"
  for r, i in results
    csvLines.push [
      i+1
      r.name
      r.run_dir
      r.mode
      r.n
      r.empty_rate
      r.sent_end_rate
      r.avg_len_words
      r.summary_path
    ].join(',')
  fs.writeFileSync outCsv, csvLines.join("\n") + "\n", 'utf8'

  # Markdown
  md = []
  md.push "# Courtroom Judgement"
  md.push ""
  md.push "| rank | name | n | empty_rate | sent_end_rate | avg_len_words |"
  md.push "|-----:|:-----|--:|-----------:|--------------:|--------------:|"
  for r, i in results
    md.push "| #{i+1} | #{r.name} | #{r.n} | #{toFixed4(r.empty_rate)} | #{toFixed4(r.sent_end_rate)} | #{toFixed4(r.avg_len_words)} |"
  fs.writeFileSync outMd, md.join("\n") + "\n", 'utf8'

  banner "Judgement written:"
  console.log " •", outJson
  console.log " •", outCsv
  console.log " •", outMd
  console.log "\nTop candidate:", results[0].name

# Kick off
main().catch (e) ->
  console.error "Fatal error:", String(e?.message or e)
  process.exit(1)
