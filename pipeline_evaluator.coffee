#!/usr/bin/env coffee
###
  courtroom_judge.coffee
  Unified evaluator + judge.

  Modes:
  1) Single-run (inside a training directory with experiment.yaml):
     - Build evaluate.yaml (defaults + recipe + override + env CFG_*)
     - Parse pipeline, topo-sort, and execute CoffeeScript steps
     - Log step stdout/stderr to logs/eval.log and logs/eval.err

  2) Courtroom (no experiment.yaml in CWD):
     - For each subdir containing experiment.yaml:
         spawn this same script (single-run mode) with cwd=subdir
     - After all complete, aggregate ablation_generations_summary.csv
       from each run into judgement_summary.{json,csv,md} in courtroom.

  ENV:
    EXEC must point to repo root containing config/default.yaml and recipes/eval_pipeline.yaml
###

fs        = require 'fs'
path      = require 'path'
yaml      = require 'js-yaml'
{ spawn } = require 'child_process'

targetDir = process.argv[2]
unless targetDir?
  console.error "❌ Missing target directory argument."
  console.error "Usage: coffee $EXEC/pipeline_evaluator.coffee /path/to/run_dir/"
  process.exit 1

unless fs.existsSync(targetDir)
  console.error "❌ Target directory not found:", targetDir
  process.exit 1

process.chdir targetDir
console.log "📂 Evaluating:", targetDir
# ----------------------------
# Helpers (logging, CSV, misc)
# ----------------------------
banner = (msg) -> console.log "\n=== #{msg} ==="
trimNL = (s) -> String(s ? '').replace(/\r?\n+$/,'')
toFixed4 = (x) -> Number.isFinite(+x) and (+x).toFixed(4) or ''

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

# ----------------------------
# Memo kernel (as in runner)
# ----------------------------
class Memo
  constructor: (@evaluator) ->
    @MM = {}
    @regexListeners = []

  saveThis: (key, value) ->
    return @MM[key] if @MM[key]? and value == @MM[key].value
    oldResolver = @MM[key]?.resolver ? null
    breaker = null
    maybe = new Promise (resolve, reject) -> breaker = resolve
    @MM[key] = { value, notifier: maybe, resolver: breaker }
    oldResolver value if oldResolver
    maybe.then (newvalue) => @MM[key].value = newvalue
    for listener in @regexListeners when listener.regex.test(key)
      listener.callback key, value
    @MM[key]

  theLowdown: (key) ->
    return @MM[key] if @MM[key]?
    @saveThis key, undefined

  waitFor: (aList, andDo) ->
    dependants = ( @theLowdown(k).notifier for k in aList )
    Promise.all(dependants).then andDo

# ----------------------------
# Config flattening utilities
# ----------------------------
isPlainObject = (o) -> Object.prototype.toString.call(o) is '[object Object]'
deepMerge = (target, source) ->
  return target unless source?
  for own k, v of source
    if isPlainObject(v) and isPlainObject(target[k])
      target[k] = deepMerge Object.assign({}, target[k]), v
    else
      target[k] = v
  target

loadYamlSafe = (p) ->
  return {} unless p? and fs.existsSync(p)
  yaml.load fs.readFileSync(p, 'utf8') or {}

expandIncludes = (spec, baseDir) ->
  incs = spec.include
  return spec unless incs? and Array.isArray(incs) and incs.length > 0
  merged = JSON.parse(JSON.stringify(spec))
  for inc in incs
    incPath = path.isAbsolute(inc) and inc or path.join(baseDir, inc)
    sub = loadYamlSafe(incPath)
    merged = deepMerge merged, sub
  merged

buildEnvOverrides = (prefix='CFG_') ->
  out = {}
  for own k, v of process.env when k.indexOf(prefix) is 0
    parts = k.substring(prefix.length).split('__')
    val = v
    try val = JSON.parse(v) catch e then val = v
    node = out
    for i in [0...parts.length-1]
      p = parts[i]
      node[p] ?= {}
      node = node[p]
    node[parts[parts.length-1]] = val
  out

createEvaluateYaml = (EXEC, baseRecipePath, overridePath) ->
  banner "🔧 Building evaluate.yaml"
  defaultPath = path.join(EXEC, 'config', 'default.yaml')
  baseAbs     = path.resolve(baseRecipePath)
  baseDir     = path.dirname(baseAbs)

  defaults = loadYamlSafe(defaultPath)
  base     = loadYamlSafe(baseAbs)
  base     = expandIncludes(base, baseDir)
  override = loadYamlSafe(overridePath)
  envOv    = buildEnvOverrides('CFG_')

  merged = deepMerge {}, defaults
  merged = deepMerge merged, base
  merged = deepMerge merged, override
  merged = deepMerge merged, envOv

  # Sanity ping
  unless merged?.run?.output_dir?
    console.warn "⚠️  run.output_dir missing post-merge; check defaults/override."

  outPath = path.join(process.cwd(), 'evaluate.yaml')
  fs.writeFileSync outPath, yaml.dump(merged), 'utf8'
  console.log "✅ evaluate.yaml →", outPath
  outPath

# ----------------------------
# Pipeline graph (eval recipe)
# ----------------------------
normalizePipeline = (spec={}) ->
  steps = spec.pipeline?.steps
  unless steps?
    throw new Error "Spec missing pipeline.steps"

  for own name, def of steps when def?.depends_on? and def.depends_on.length is 1 and def.depends_on[0] is 'never'
    delete steps[name]

  if Array.isArray(steps)
    obj = {}
    for name in steps
      obj[name] = { run: null, depends_on: [] }
    steps = obj

  for own name, def of steps
    unless def?.run? then throw new Error "Step '#{name}' missing 'run:'"
    def.depends_on ?= []
    unless Array.isArray(def.depends_on)
      throw new Error "Step '#{name}'.depends_on must be an array"
  steps

toposort = (steps) ->
  indeg = {}; graph = {}
  for own n, _ of steps
    indeg[n] = 0; graph[n] = []
  for own n, def of steps
    for dep in def.depends_on
      unless steps[dep]? then throw new Error "Undefined dependency '#{dep}'"
      indeg[n] += 1; graph[dep].push n
  q = (n for own n, d of indeg when d is 0)
  order = []
  while q.length
    n = q.shift()
    order.push n
    for m in graph[n]
      indeg[m] -= 1
      if indeg[m] is 0 then q.push m
  if order.length isnt Object.keys(steps).length
    throw new Error "Cycle detected in pipeline graph"
  order

# ----------------------------
# Step runner (CoffeeScript)
# ----------------------------
runCoffeeStep = (stepName, scriptPath, env, logOutFd, logErrFd) ->
  new Promise (resolve, reject) ->
    # Always spawn coffee for the step; send output to logs
    proc = spawn('coffee', [scriptPath],
      cwd: process.cwd()
      env: env
      stdio: ['ignore', logOutFd, logErrFd]
    )
    proc.on 'error', (e) -> 
      console.log "BIGBAD",e
      reject e
    proc.on 'exit', (code) ->
      if code is 0 then resolve() else reject new Error("#{stepName} failed (#{code})")
      

# ----------------------------
# Single-run evaluator (in CWD)
# ----------------------------
evaluateCurrentRun = (EXEC) ->
  banner "Single-run mode: evaluating CWD"
  console.log "CWD is" , process.cwd()

  # Prepare logs
  fs.mkdirSync path.join(process.cwd(), 'logs'), {recursive:true}
  logOutFd = fs.openSync(path.join(process.cwd(), 'logs', 'eval.log'), 'a')
  logErrFd = fs.openSync(path.join(process.cwd(), 'logs', 'eval.err'), 'a')

  try
    recipe = path.join(EXEC, 'recipes', 'eval_pipeline.yaml')
    overridePath = path.join(process.cwd(), 'override.yaml')
    evalYaml = createEvaluateYaml(EXEC, recipe, overridePath)

    spec  = loadYamlSafe(evalYaml)
    steps = normalizePipeline(spec)
    order = toposort(steps)

    M = new Memo()

    # Run in strict topo order (deterministic), Memo marks done
    for name in order
      def = steps[name]
      scriptPath = path.join(EXEC, def.run)
      env = Object.assign({}, process.env,
        { CFG_OVERRIDE: evalYaml, STEP_NAME: name, EXEC }
      )
      await runCoffeeStep(name, scriptPath, env, logOutFd, logErrFd)
      M.saveThis "done:#{name}", true

    banner "🌟 Evaluation finished for current run."
  finally
    # Ensure fds closed even on errors
    try fs.closeSync logOutFd catch e then null
    try fs.closeSync logErrFd catch e then null

# ----------------------------
# Courtroom mode (iterate runs)
# ----------------------------
discoverCandidates = (courtroomDir) ->
  root = path.resolve(courtroomDir)
  entries = []
  try
    entries = fs.readdirSync(root, { withFileTypes: true })
  catch e
    console.error "Cannot read directory:", root, "-", e.message
    return []
  out = []
  for d in entries when d.isDirectory?() and d.isDirectory()
    full = path.join(root, d.name)
    if fs.existsSync(path.join(full, 'experiment.yaml'))
      out.push full
  out

spawnSelfSingleRun = (EXEC, runDir) ->
  logOutFd = fs.openSync(path.join(runDir, 'logs', 'eval.log'), 'a')
  logErrFd = fs.openSync(path.join(runDir, 'logs', 'eval.err'), 'a')
  new Promise (resolve, reject) ->
    # Spawn THIS script in the runDir; stdout/err piped to that run's logs via the child
    proc = spawn 'coffee', [
      path.join(EXEC, 'pipeline_evaluator.coffee'),
      runDir
    ],
      cwd: runDir   #courtroom        # parent context
      env: Object.assign({}, process.env, { EXEC })
      stdio: ['ignore', logOutFd, logErrFd]  # child writes logs to files

    proc.on 'error', (e) ->
      console.log "JIM spawn fail",runDir,e
      reject e
    proc.on 'exit', (code) ->
      if code is 0 then resolve() else reject new Error("Evaluator exited #{code}")

aggregateCourtroom = (courtroomDir) ->
  results = []
  for runDir in discoverCandidates(courtroomDir)
    sumCsv = path.join(runDir, 'eval_out', 'ablation_generations_summary.csv')
    continue unless fs.existsSync(sumCsv)
    rows = readCsv(sumCsv)
    continue unless rows.length
    # choose the row with largest n
    best = rows.slice().sort (a,b) ->
      (parseFloat(b.n ? '0') or 0) - (parseFloat(a.n ? '0') or 0)
    primary = best[0]
    parseF = (x)-> parseFloat(x ? '0') or 0
    results.push
      run_dir: runDir
      name: path.basename(runDir)
      n: parseInt(primary.n ? '0') or 0
      empty_rate: +toFixed4(parseF(primary.empty_rate))
      sent_end_rate: +toFixed4(parseF(primary.sent_end_rate))
      avg_len_words: +toFixed4(parseF(primary.avg_len_words))
  return results

writeJudgement = (courtroomDir, results) ->
  # Sort: empty_rate ASC, sent_end_rate DESC, avg_len_words DESC
  results.sort (a,b) ->
    if a.empty_rate isnt b.empty_rate then a.empty_rate - b.empty_rate \
    else if a.sent_end_rate isnt b.sent_end_rate then b.sent_end_rate - a.sent_end_rate \
    else b.avg_len_words - a.avg_len_words

  outJson = path.join(courtroomDir, 'judgement_summary.json')
  outCsv  = path.join(courtroomDir, 'judgement_summary.csv')
  outMd   = path.join(courtroomDir, 'judgement_summary.md')

  fs.writeFileSync outJson, JSON.stringify(results, null, 2), 'utf8'

  lines = []
  lines.push "rank,name,run_dir,n,empty_rate,sent_end_rate,avg_len_words"
  for r,i in results
    lines.push [i+1,r.name,r.run_dir,r.n,r.empty_rate,r.sent_end_rate,r.avg_len_words].join(',')
  fs.writeFileSync outCsv, lines.join("\n") + "\n", 'utf8'

  md = []
  md.push "# Courtroom Judgement"
  md.push ""
  md.push "| rank | name | n | empty_rate | sent_end_rate | avg_len_words |"
  md.push "|-----:|:-----|--:|-----------:|--------------:|--------------:|"
  for r,i in results
    md.push "| #{i+1} | #{r.name} | #{r.n} | #{toFixed4(r.empty_rate)} | #{toFixed4(r.sent_end_rate)} | #{toFixed4(r.avg_len_words)} |"
  fs.writeFileSync outMd, md.join("\n") + "\n", 'utf8'

  banner "Judgement written:"
  console.log " •", outJson
  console.log " •", outCsv
  console.log " •", outMd
  console.log "\nTop candidate:", results[0]?.name ? "(none)"

# ----------------------------
# Main
# ----------------------------
main = ->
  EXEC = process.env.EXEC
  unless EXEC? and fs.existsSync(path.join(EXEC, 'recipes', 'eval_pipeline.yaml'))
    console.error "❌ EXEC must point to repo root with recipes/eval_pipeline.yaml"
    process.exit(1)

  console.log "=== Eval started", new Date().toISOString(), "==="

  if fs.existsSync(path.join(process.cwd(), 'experiment.yaml'))
    # Single run: parse recipe + run steps with Memo + log to files
    await evaluateCurrentRun(EXEC)
    process.exit(0)

  # Courtroom mode
  courtroom = process.argv[2] ? process.cwd()
  courtroom = path.resolve(courtroom)
  unless fs.existsSync(courtroom)
    console.error "❌ Courtroom directory not found:", courtroom
    process.exit(1)
  banner "Courtroom mode: #{courtroom}"

  runDirs = discoverCandidates(courtroom)
  if runDirs.length is 0
    console.log "No candidate run directories found (need subdirs with experiment.yaml)."
    process.exit(0)

  # Sequentially spawn ourselves in each run directory
  for dir in runDirs
    banner "Evaluating: #{dir}"
    try
      console.log "Spawning",EXEC,dir
      await spawnSelfSingleRun(EXEC, dir)
      console.log "✅ OK:", dir
    catch e
      console.error "❌ Evaluation failed:", dir
      console.error String(e?.message or e)

  # After all complete, aggregate
  results = aggregateCourtroom(courtroom)
  if results.length is 0
    console.log "No usable results found."
    process.exit(0)
  writeJudgement(courtroom, results)

# Kickoff
main().catch (e) ->
  console.error "Fatal:", String(e?.message or e)
  process.exit(1)
