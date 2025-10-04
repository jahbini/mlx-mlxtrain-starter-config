###
  pipeline_runner.coffee
  A lightweight Petri-net/DAG recipe runner using your Memo kernel.

  Changes:
  - Pre-flatten config into PWD/experiment.yaml (base recipe + sub-recipes + override.yaml).
  - DEBUG mode: per-step behavior.
      * If any declared input is missing → log paths and EXIT immediately.
      * If all inputs exist → touch/create declared outputs, mark step done, DO NOT run the script.
###

fs        = require 'fs'
path      = require 'path'
yaml      = require 'js-yaml'
{ spawn } = require 'child_process'
{ execSync } = require 'child_process'

# --------------------------------------
# Memo kernel (kept consistent with your version)
# --------------------------------------
class Memo
  constructor: (@evaluator) ->
    @MM = {}
    @regexListeners = []

  memoLog: (key) ->
    console.log "Snapping #{key}", @MM[key]

  saveThis: (key, value) ->
    return @MM[key] if @MM[key]? and value == @MM[key].value

    console.log "saving key,value", key, value
    oldResolver = @MM[key]?.resolver ? null
    breaker = null
    maybe = new Promise (resolve, reject) ->
      breaker = resolve

    @MM[key] =
      value: value
      notifier: maybe
      resolver: breaker

    oldResolver value if oldResolver
    maybe.then (newvalue) => @MM[key].value = newvalue

    for listener in @regexListeners
      if listener.regex.test(key)
        listener.callback(key, value)

    @MM[key]

  theLowdown: (key) ->
    return @MM[key] if @MM[key]?
    @saveThis key, undefined

  waitFor: (aList, andDo) ->
    dependants = for key in aList
      d = @theLowdown key
      d.notifier
    Promise.all(dependants).then andDo

  notifyMe: (n, andDo) ->
    newValue = (@theLowdown n).value
    while true
      currentValue = newValue
      andDo newValue
      while currentValue == newValue
        newValue = (await @MM[n].notifier).value

  waitForRegex: (regex, callback) ->
    matched = []
    for key, memoObj of @MM
      if regex.test(key)
        matched.push(memoObj.notifier)
    @regexListeners.push({ regex, callback })
    if matched.length > 0
      Promise.any(matched).then(callback)

# --------------------------------------
# Utilities
# --------------------------------------
banner = (msg) -> console.log "\n=== #{msg} ==="
prefixLines = (pfx, s) -> (s ? '').split(/\r?\n/).map((l)-> (pfx + l)).join("\n")

# Deep merge (for config flattening)
isPlainObject = (o) -> Object.prototype.toString.call(o) is '[object Object]'
deepMerge = (target, source) ->
  return target unless source?
  for own k, v of source
    if isPlainObject(v) and isPlainObject(target[k])
      target[k] = deepMerge Object.assign({}, target[k]), v
    else
      target[k] = v
  target

# --------------------------------------
# Petri/DAG execution
# --------------------------------------
M = new Memo()

# --------------------------------------
# Spawn a python script, prefixing logs with the step name
# --------------------------------------
runPythonScript = (stepName, scriptPath, envOverrides={}) ->
  new Promise (resolve, reject) ->
    console.log "▶️  #{stepName}: running #{scriptPath}"
    proc = spawn("python", [scriptPath], {
      stdio: ['ignore', 'pipe', 'pipe']
      env: Object.assign({}, process.env, envOverrides)
    })
    proc.stdout.on 'data', (buf) -> process.stdout.write prefixLines("┆ #{stepName} | ", buf.toString()) + "\n"
    proc.stderr.on 'data', (buf) -> process.stderr.write prefixLines("! #{stepName} | ", buf.toString()) + "\n"
    proc.on 'error', (err) ->
      console.error "! #{stepName}: spawn error", err
      reject err
    proc.on 'exit', (code) ->
      if code is 0
        console.log "✅ #{stepName}: done"
        M.saveThis "done:#{stepName}", true
        resolve "done: #{stepName}"
      else
        err = new Error "#{stepName} failed (exit #{code})"
        console.error "! #{stepName}: #{err.message}"
        reject err

# --------------------------------------
# Validate and normalize the pipeline spec
# --------------------------------------
normalizePipeline = (spec = {"pipeline":[]}  ) ->
  steps = spec.pipeline?.steps
  unless steps?
    throw new Error "Spec missing pipeline.steps"

  # prune steps with depends_on: [never]
  for own name, def of steps
    if def.depends_on? and def.depends_on.length is 1 and def.depends_on[0] is "never"
      console.log "⏭️ removing step #{name} (depends_on: never)"
      delete steps[name]

  if Array.isArray(steps)
    obj = {}
    for name in steps
      obj[name] = { run: null, depends_on: [] }
    steps = obj

  for own name, def of steps
    unless def?.run?
      throw new Error "Step '#{name}' missing 'run:' script path"
    def.depends_on ?= []
    unless Array.isArray(def.depends_on)
      throw new Error "Step '#{name}'.depends_on must be an array"
    # inputs/outputs are optional; leave as-is so DEBUG can use them

  steps

# --------------------------------------
# Detect cycles using Kahn's algorithm; return topological order
# --------------------------------------
toposort = (steps) ->
  indeg = {}
  graph = {}
  for own name, def of steps
    indeg[name] = 0
    graph[name] = []
  for own name, def of steps
    for dep in def.depends_on
      unless steps[dep]? then throw new Error "Undefined dependency '#{dep}' (referenced by '#{name}')"
      indeg[name] += 1
      graph[dep].push name
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

# --------------------------------------
# Compute terminal steps (no one depends on them)
# --------------------------------------
terminalSteps = (steps) ->
  dependents = new Set()
  for own name, def of steps
    for dep in def.depends_on
      dependents.add dep
  (n for own n, _ of steps when not dependents.has(n))

# --------------------------------------
# Optional: emit Graphviz DOT
# --------------------------------------
emitDot = (steps, outPath) ->
  try
    lines = ['digraph pipeline {', '  rankdir=LR;']
    for own name, def of steps
      lines.push "  \"#{name}\" [shape=box];"
    for own name, def of steps
      for dep in def.depends_on
        lines.push "  \"#{dep}\" -> \"#{name}\";"
    lines.push '}'
    fs.writeFileSync outPath, lines.join("\n"), "utf8"
    console.log "🖼  Wrote DOT graph:", outPath
  catch e
    console.error "Failed to write DOT:", e.message

# --------------------------------------
# Single-instance guard
# --------------------------------------
ensureSingleInstance = ->
  try
    scriptPath = path.resolve(__filename)
    out = execSync("ps -Ao pid,command | grep 'coffee' | grep '#{scriptPath}' | grep -v grep || true").toString()
    lines = out.trim().split("\n").filter (l) -> l.length > 0
    others = lines.filter (l) -> not l.startsWith(process.pid.toString())
    if others.length > 0
      process.exit(0)
  catch err
    console.error "Error checking processes:", err.message

# --------------------------------------
# Pre-flatten into experiment.yaml (base + sub-recipes + override)
# --------------------------------------
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

createExperimentYaml = (basePath, overridePath) ->
  banner "🔧 Creating experiment.yaml"
  baseAbs = path.resolve(basePath)
  baseDir = path.dirname(baseAbs)

# Always merge defaults → base → override
  defaults = loadYamlSafe("config/default.yaml")
  base     = loadYamlSafe(baseAbs)
  base     = expandIncludes(base, baseDir)
  override = loadYamlSafe(overridePath)

  merged = deepMerge(JSON.parse(JSON.stringify(defaults)), base)
  merged = deepMerge(merged, override)

  expPath = path.join(process.cwd(), "experiment.yaml")
  fs.writeFileSync(expPath, yaml.dump(merged), "utf8")
  console.log "✅ Wrote experiment.yaml:", expPath
  expPath

# --------------------------------------
# Per-step DEBUG behavior
# --------------------------------------
DEBUG_TOUCH_DIR = (p) ->
  try
    fs.mkdirSync(p, {recursive:true})
    true
  catch e
    console.error "! DEBUG mkdir failed:", p, e.message
    false

DEBUG_TOUCH_FILE = (p) ->
  try
    dir = path.dirname(p)
    fs.mkdirSync(dir, {recursive:true})
    fd = fs.openSync(p, 'a')
    fs.closeSync(fd)
    true
  catch e
    console.error "! DEBUG touch failed:", p, e.message
    false

debugHandleStep = (stepName, def) ->
  ins  = def.inputs or []
  outs = def.outputs or []

  missing = (f for f in ins when not fs.existsSync(f))
  if missing.length > 0
    console.error "🐞 DEBUG: missing inputs for step '#{stepName}':"
    for f in missing
      console.error "   - #{f}"
    console.error "Exiting due to DEBUG missing inputs."
    process.exit(0)

  # All inputs exist → touch outputs and mark as done
  for f in outs
    # treat trailing slash as directory; otherwise create file
    if /[\/\\]$/.test(f)
      DEBUG_TOUCH_DIR(f)
    else
      # crude heuristic: if it has an extension, treat as file; else still create dir if ends with '/'
      if path.extname(f) then DEBUG_TOUCH_FILE(f) else DEBUG_TOUCH_DIR(f)

  console.log "🐞 DEBUG: step '#{stepName}' outputs touched; skipping script."
  M.saveThis "done:#{stepName}", true

# --------------------------------------
# Main
# --------------------------------------
main = ->
  ensureSingleInstance()

  baseRecipe = process.argv[2] ? 'recipes/full_pipeline.yaml'
  dotOut     = process.env.DOT_OUT ? process.argv[3] ? null
  DEBUG      = !!(process.env.DEBUG? and String(process.env.DEBUG).toLowerCase() in ['1','true','yes'])

  console.log "CWD:", process.cwd()
  banner "Recipe (base): #{baseRecipe}"

  # Pre-flatten experiment config
  overridePath = path.join(process.cwd(), 'override.yaml')
  expPath = createExperimentYaml(baseRecipe, overridePath)

  # Load flattened spec
  spec = loadYamlSafe(expPath)

  steps = normalizePipeline spec
  order = toposort steps
  console.log "Topo order:", order.join(" → ")
  if dotOut? then emitDot steps, dotOut

  M.waitForRegex /^done:/, (k,v) ->
    console.log "DEBUG done-signal:", k

  # Set up step firing rules
  for own name, def of steps
    do (name, def) ->
      fire = ->
        if DEBUG
          # Per-step DEBUG: check inputs; if ok, touch outputs; do not spawn
          return debugHandleStep(name, def)

        runPythonScript(name, process.env.EXEC + "/" + def.run, { CFG_OVERRIDE: expPath, STEP_NAME: name })
          .then ->
            M.saveThis "done:#{name}", true
          .catch (err) ->
            console.error "! #{name}: step failed, continuing"
            console.error err.stack or err
            M.saveThis "done:#{name}", false

      if def.depends_on.length is 0
        console.log "▶️ starting root step #{name}"
        fire()
      else
        console.log "⏳ waiting for deps of #{name}: #{def.depends_on.join ', '}"
        M.waitFor (def.depends_on.map (d) -> "done:#{d}"), ->
          fire()

  finals = terminalSteps steps
  Promise.all( finals.map((s)-> M.theLowdown(s).notifier) ).then ->
    banner "🌟 Pipeline finished (final steps: #{finals.join(', ')})"
    process.exit(0)
  .catch (e) ->
    console.error "Pipeline failed:", e.message
    process.exit(1)

# Handle Ctrl+C nicely
process.on 'SIGINT', ->
  console.log "\n(CTRL+C) Shutting down…"
  process.exit(130)

# Kickoff
main()
