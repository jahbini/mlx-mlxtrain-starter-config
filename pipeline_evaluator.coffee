#!/usr/bin/env coffee
###
  pipeline_evaluator.coffee
  Unified evaluator + judge (flat-map schema).

  Modes:
  1) Single-run (inside a training directory with experiment.yaml):
     - Build evaluate.yaml (defaults + eval recipe + env CFG_* ONLY)
     - Parse flat-map steps, build DAG, execute CoffeeScript steps
     - Log stdout/stderr to logs/eval.log and logs/eval.err

  2) Courtroom (no experiment.yaml in CWD):
     - For each subdir containing experiment.yaml:
         spawn this same script (single-run mode) with cwd=subdir
     - Aggregate ablation_generations_summary.csv to judgement_summary.{json,csv,md}

  Guarantees:
    • No per-run override files are used (directory overrides ignored).
    • Only steps declared in the eval recipe become DAG nodes.
    • Flat-map schema (top-level keys with `run:`) is required.

  ENV:
    EXEC must point to repo root containing:
      - config/default.yaml        (for generic run.* defaults and params)
      - recipes/eval_pipeline.yaml (the evaluation recipe)
    Optional:
      - CFG_* prefixed env vars for last-mile param overrides
###

fs        = require 'fs'
path      = require 'path'
yaml      = require 'js-yaml'
{ spawn } = require 'child_process'

# ----------------------------
# CLI + CWD
# ----------------------------
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
# Memo (same as runner)
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
# Config utilities
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
    incPath = if path.isAbsolute(inc) then inc else path.join(baseDir, inc)
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

# ----------------------------
# Build evaluate.yaml (NO per-run overrides)
#   - defaults + eval recipe (+ includes) + CFG_*
#   - then RESTRICT steps to those declared in the recipe
# ----------------------------
createEvaluateYaml = (EXEC) ->
  banner "🔧 Building evaluate.yaml"
  defaultPath = path.join(EXEC, 'config', 'default.yaml')
  recipePath  = path.join(EXEC, 'recipes', 'eval_pipeline.yaml')

  defaults = loadYamlSafe(defaultPath)
  recipe   = loadYamlSafe(recipePath)
  envOv    = buildEnvOverrides('CFG_')

  # Merge only these three — no cwd overrides, no experiment.yaml
  merged = deepMerge {}, defaults
  merged = deepMerge merged, recipe
  merged = deepMerge merged, envOv

  # Sanity ping
  unless merged?.run?.output_dir?
    console.warn "⚠️ run.output_dir missing post-merge; check defaults/config:", defaultPath

  outPath = path.join(process.cwd(), 'evaluate.yaml')
  fs.writeFileSync outPath, yaml.dump(merged), 'utf8'
  console.log "✅ evaluate.yaml written →", outPath
  console.log "Includes steps:", (k for own k, v of recipe when v?.run?).join(', ')
  outPath

createEvaluateYamlOld = (EXEC, baseRecipePath) ->
  banner "🔧 Building evaluate.yaml"
  defaultPath = path.join(EXEC, 'config', 'default.yaml')
  baseAbs     = path.resolve(baseRecipePath)
  baseDir     = path.dirname(baseAbs)

  unless fs.existsSync(baseAbs)
    throw new Error "Eval recipe not found: #{baseAbs}"

  defaults = loadYamlSafe(defaultPath)
  base     = loadYamlSafe(baseAbs)
  base     = expandIncludes(base, baseDir)   # resolve includes relative to recipe dir
  envOv    = buildEnvOverrides('CFG_')

  # Merge: defaults → base recipe → env
  merged = deepMerge {}, defaults
  merged = deepMerge merged, base
  merged = deepMerge merged, envOv

  # Restrict DAG nodes to only those declared by the recipe (not defaults).
  # A step is a top-level key with a 'run:' field.
  recipeKeys = (k for own k, v of base when k isnt 'run' and v?.run?)
  for own k, v of merged
    continue if k is 'run'
    continue if k in recipeKeys
    if v?.run?
      # This is a runnable step present in defaults/env, but NOT in the eval recipe.
      delete merged[k]

  unless merged?.run?.output_dir?
    console.warn "⚠️  run.output_dir missing post-merge; check recipe:", baseRecipePath

  outPath = path.join(process.cwd(), 'evaluate.yaml')
  fs.writeFileSync outPath, yaml.dump(merged), 'utf8'
  console.log "✅ evaluate.yaml →", outPath
  console.log "Eval steps:", recipeKeys.join(', ')
  outPath

# ----------------------------
# Pipeline (flat-map) → steps map
# ----------------------------
###
  normalizeFlatPipeline
  ---------------------
  Interprets only those top-level keys that contain an explicit
  'depends_on' field as pipeline steps.
  Everything else (like 'run:' block, paths, configs) is ignored.
###
normalizeFlatPipeline = (spec = {}) ->
  steps = {}

  for own name, def of spec
    # Ignore the global 'run' block or any scalar/non-object entries
    continue if name is 'run'
    continue unless def? and typeof def is 'object'

    # Only accept entries that explicitly define 'depends_on'
    unless 'depends_on' of def
      console.log "⚙️  config-only section '#{name}' (no depends_on) — ignored"
      continue

    # Skip disabled steps
    if def.depends_on is 'never' or (Array.isArray(def.depends_on) and 'never' in def.depends_on)
      console.log "⏭️  skipping step #{name} (depends_on: never)"
      continue

    # Normalize dependency list
    deps = []
    if def.depends_on?
      deps = if Array.isArray(def.depends_on) then def.depends_on else [def.depends_on]

    # Register the step
    steps[name] =
      run: def.run ? null
      depends_on: deps
      params: Object.assign({}, def)
      desc: def.desc or ''

  unless Object.keys(steps).length
    throw new Error "No runnable steps found in evaluate.yaml"

  return steps

normalizeFlatPipelineOld = (spec = {}) ->
  steps = {}
  for own name, def of spec
    continue if name is 'run'   # global config

    # A DAG node must have 'run:'
    continue unless def?.run?

    # depends_on normalization / skipping
    deps = []
    if def.depends_on?
      if def.depends_on is 'never' or (Array.isArray(def.depends_on) and 'never' in def.depends_on)
        console.log "⏭️  skipping step #{name} (depends_on: never)"
        continue
      deps = if Array.isArray(def.depends_on) then def.depends_on else [def.depends_on]

    steps[name] =
      run: def.run
      depends_on: deps
      params: Object.assign({}, def)
      desc: def.desc or ''

  unless Object.keys(steps).length
    throw new Error "No runnable steps found in evaluate.yaml"

  steps

# ----------------------------
# DAG builder (defensive Kahn)
# ----------------------------
buildDag = (steps) ->
  indeg = {}
  graph = {}

  # init structures
  for own name, _ of steps
    indeg[name] = 0
    graph[name] = []

  # build edges, prune missing deps
  for own name, def of steps
    clean = []
    for dep in def.depends_on or []
      if steps[dep]?
        clean.push dep
      else
        console.warn "⚠️  #{name} depends on missing step '#{dep}' — ignoring."
    def.depends_on = clean

  for own name, def of steps
    for dep in def.depends_on
      indeg[name] += 1
      graph[dep].push name

  # seed queue with roots (indegree 0)
  q = (n for own n, d of indeg when d is 0)
  if q.length is 0
    # try explicit [] deps as roots
    for own n, def of steps when Array.isArray(def.depends_on) and def.depends_on.length is 0
      q.push n unless n in q

  order = []
  while q.length
    n = q.shift()
    order.push n
    for m in graph[n]
      indeg[m] -= 1
      if indeg[m] is 0
        q.push m

  # Don't hard-fail if graph changed due to pruning; just warn
  if order.length isnt Object.keys(steps).length
    missing = Object.keys(steps).filter (k)-> order.indexOf(k) is -1
    console.error "⚠️  DAG anomaly: expected #{Object.keys(steps).length}, got #{order.length}. Missing:", missing.join(', ')

  order

# ----------------------------
# Step runner (CoffeeScript-only eval steps)
# ----------------------------
runCoffeeStep = (stepName, scriptPath, env, logOutFd, logErrFd) ->
  new Promise (resolve, reject) ->
    proc = spawn('coffee', [scriptPath],
      cwd: process.cwd()
      env: env
      stdio: ['ignore', logOutFd, logErrFd]
    )
    proc.on 'error', (e) ->
      reject e
    proc.on 'exit', (code) ->
      if code is 0 then resolve() else reject new Error("#{stepName} failed (#{code})")

# ----------------------------
# Single-run evaluator
# ----------------------------
evaluateCurrentRun = (EXEC) ->
  banner "Single-run mode: evaluating CWD"
  console.log "CWD is", process.cwd()

  # Prepare logs
  fs.mkdirSync path.join(process.cwd(), 'logs'), {recursive:true}
  logOutFd = fs.openSync(path.join(process.cwd(), 'logs', 'eval.log'), 'a')
  logErrFd = fs.openSync(path.join(process.cwd(), 'logs', 'eval.err'), 'a')

  try
    recipe = path.join(EXEC, 'recipes', 'eval_pipeline.yaml')
    console.log "Recipe:", recipe
    evalYaml = createEvaluateYaml(EXEC, recipe)

    spec  = loadYamlSafe(evalYaml)
    steps = normalizeFlatPipeline(spec)
    order = buildDag(steps)

    console.log "Topo order:", order.join(' → ')
    for own n, d of steps
      console.log "#{n} depends_on: #{(d.depends_on or []).join(', ')}"

    M = new Memo()

    # Execute in topo order
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
  fs.mkdirSync path.join(runDir, 'logs'), {recursive:true}
  logOutFd = fs.openSync(path.join(runDir, 'logs', 'eval.log'), 'a')
  logErrFd = fs.openSync(path.join(runDir, 'logs', 'eval.err'), 'a')
  new Promise (resolve, reject) ->
    proc = spawn 'coffee', [
      path.join(EXEC, 'pipeline_evaluator.coffee'),
      runDir
    ],
      cwd: runDir
      env: Object.assign({}, process.env, { EXEC })
      stdio: ['ignore', logOutFd, logErrFd]

    proc.on 'error', (e) -> reject e
    proc.on 'exit', (code) ->
      if code is 0 then resolve() else reject new Error("Evaluator exited #{code}")

aggregateCourtroom = (courtroomDir) ->
  results = []
  for runDir in discoverCandidates(courtroomDir)
    sumCsv = path.join(runDir, 'eval_out', 'ablation_generations_summary.csv')
    continue unless fs.existsSync(sumCsv)
    rows = readCsv(sumCsv)
    continue unless rows.length
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
  results

writeJudgement = (courtroomDir, results) ->
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

  # Single-run mode if the target has a training experiment.yaml
  if fs.existsSync(path.join(process.cwd(), 'experiment.yaml'))
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

  for dir in runDirs
    banner "Evaluating: #{dir}"
    try
      await spawnSelfSingleRun(EXEC, dir)
      console.log "✅ OK:", dir
    catch e
      console.error "❌ Evaluation failed:", dir
      console.error String(e?.message or e)

  results = aggregateCourtroom(courtroom)
  if results.length is 0
    console.log "No usable results found."
    process.exit(0)
  writeJudgement(courtroom, results)

# Kickoff
main().catch (e) ->
  console.error "Fatal:", String(e?.message or e)
  process.exit(1)
