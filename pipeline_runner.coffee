#!/usr/bin/env coffee
###
  pipeline_runner.coffee  — Flat-Step Runner
  -----------------------------------------
  New model:
    - No "pipeline: steps:" block.
    - Each top-level key that has a "run:" is a step (except "run" global).
    - "depends_on" is just another key on the step (string or array).
    - Precedence: recipe < config/default.yaml < override.yaml
    - Experiment is pre-flattened and written to PWD/experiment.yaml

  Extras kept from prior runner:
    - depends_on: "never" or ["never"] → step skipped
    - DEBUG mode: touch outputs, don't execute
    - Auto-interpreter: .py -> python -u; .coffee -> coffee
    - STEP_NAME and STEP_PARAMS_JSON exported to scripts
    - Graphviz DOT optional via DOT_OUT
###

fs        = require 'fs'
path      = require 'path'
yaml      = require 'js-yaml'
{ spawn } = require 'child_process'
{ execSync } = require 'child_process'

EXEC = process.env.EXEC

# --------------------------------------
# Memo kernel (unchanged semantics)
# --------------------------------------
class Memo
  constructor: (@evaluator) ->
    @MM = {}
    @regexListeners = []

  memoLog: (key) -> console.log "Snapping #{key}", @MM[key]

  saveThis: (key, value) ->
    return @MM[key] if @MM[key]? and value == @MM[key].value
    oldResolver = @MM[key]?.resolver ? null
    breaker = null
    maybe = new Promise (resolve, reject) -> breaker = resolve
    @MM[key] = { value, notifier: maybe, resolver: breaker }
    oldResolver value if oldResolver
    maybe.then (newvalue) => @MM[key].value = newvalue
    for listener in @regexListeners
      if listener.regex.test(key) then listener.callback(key, value)
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
      if regex.test(key) then matched.push(memoObj.notifier)
    @regexListeners.push({ regex, callback })
    if matched.length > 0 then Promise.any(matched).then(callback)

# --------------------------------------
# Utilities
# --------------------------------------
banner = (msg) -> console.log "\n=== #{msg} ==="
prefixLines = (pfx, s) -> (s ? '').split(/\r?\n/).map((l)-> (pfx + l)).join("\n")

isPlainObject = (o) -> Object.prototype.toString.call(o) is '[object Object]'

deepMerge = (target, source) ->
  # Straightforward, predictable deep merge:
  # - objects merge by key (source overwrites target values)
  # - arrays REPLACE (no concat magic)
  # - null deletes key
  return target unless source?
  for own k, v of source
    if v is null
      delete target[k]
      continue
    if isPlainObject(v) and isPlainObject(target[k])
      deepMerge target[k], v
    else
      target[k] = Array.isArray(v) and v.slice() or v
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

# --------------------------------------
# Build experiment.yaml (recipe < config < override)
# --------------------------------------
createExperimentYaml = (basePath, defaultConfig, overridePath) ->
  banner "🔧 Creating experiment.yaml"
  baseAbs  = path.resolve(basePath)
  baseDir  = path.dirname(baseAbs)

  recipe   = loadYamlSafe(baseAbs)
  recipe   = expandIncludes(recipe, baseDir)

  defaults = loadYamlSafe(defaultConfig)
  override = loadYamlSafe(overridePath)

  # Precedence: recipe < defaults < override
  merged = deepMerge {}, recipe
  merged = deepMerge merged, defaults
  merged = deepMerge merged, override

  expPath = path.join(process.cwd(), 'experiment.yaml')
  fs.writeFileSync expPath, yaml.dump(merged), 'utf8'
  console.log "✅ Wrote experiment.yaml:", expPath
  expPath

# --------------------------------------
# Step discovery from flat spec
# --------------------------------------
discoverSteps = (spec) ->
  steps = {}
  for own key, val of spec
    continue if key is 'run' # global section
    continue unless isPlainObject(val)
    if val.run?
      # Normalize depends_on
      deps = []
      if val.depends_on?
        if Array.isArray(val.depends_on)
          deps = val.depends_on.slice()
        else if typeof val.depends_on is 'string'
          deps = [val.depends_on]
      # Handle "never"
      if deps.length is 1 and String(deps[0]).toLowerCase() is 'never'
        console.log "⏭️  skipping step #{key} (depends_on: never)"
        continue
      # Shallow clone for safety
      def = {}
      for own k2, v2 of val
        def[k2] = v2
      # Ensure deps array
      def.depends_on = deps
      steps[key] = def
  if Object.keys(steps).length is 0
    throw new Error "No steps discovered in experiment.yaml (flat model expects top-level keys with 'run:')"
  steps

# --------------------------------------
# Topological sort
# --------------------------------------
toposort = (steps) ->
  indeg = {}; graph = {}
  for own name, def of steps
    indeg[name] = 0; graph[name] = []
  for own name, def of steps
    for dep in def.depends_on or []
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

terminalSteps = (steps) ->
  dependents = new Set()
  for own name, def of steps
    for dep in def.depends_on or [] then dependents.add dep
  (n for own n, _ of steps when not dependents.has(n))

emitDot = (steps, outPath) ->
  try
    lines = ['digraph pipeline {','  rankdir=LR;']
    for own name, def of steps
      lines.push "  \"#{name}\" [shape=box];"
    for own name, def of steps
      for dep in def.depends_on or []
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
    lines = out.trim().split("\n").filter (l)-> l.length>0
    others = lines.filter (l)-> not l.startsWith(process.pid.toString())
    if others.length>0 then process.exit(0)
  catch err
    console.error "Error checking processes:", err.message

# --------------------------------------
# DEBUG / touch behavior
# --------------------------------------
DEBUG_TOUCH_DIR = (p) ->
  try fs.mkdirSync(p,{recursive:true}); true catch e then console.error "! DEBUG mkdir failed:",p,e.message; false

DEBUG_TOUCH_FILE = (p) ->
  try dir=path.dirname(p); fs.mkdirSync(dir,{recursive:true}); fd=fs.openSync(p,'a'); fs.closeSync(fd); true catch e then console.error "! DEBUG touch failed:",p,e.message; false

debugHandleStep = (stepName,def) ->
  ins=def.inputs or []; outs=def.outputs or []
  missing=(f for f in ins when not fs.existsSync(f))
  if missing.length>0
    console.error "🐞 DEBUG: missing inputs for '#{stepName}':"
    for f in missing then console.error "  - #{f}"
    console.error "Exiting due to DEBUG missing inputs."; process.exit(0)
  for f in outs
    if /[\/\\]$/.test(f) then DEBUG_TOUCH_DIR(f)
    else if path.extname(f) then DEBUG_TOUCH_FILE(f) else DEBUG_TOUCH_DIR(f)
  console.log "🐞 DEBUG: step '#{stepName}' outputs touched; skipping script."
  M.saveThis "done:#{stepName}", true

# --------------------------------------
# Spawn a step with clear logging
# --------------------------------------
runStepScript = (stepName, scriptPath, envOverrides={}) ->
  new Promise (resolve, reject) ->
    interp = null
    args = []
    if /\.py$/i.test(scriptPath)
      interp = 'python'
      args = ['-u', scriptPath]  # unbuffered
    else if /\.coffee$/i.test(scriptPath)
      interp = 'coffee'
      args = [scriptPath]
    else
      return reject new Error "Unknown script type for #{stepName}: #{scriptPath}"

    console.log "▶️  #{stepName}: #{interp} #{args.join(' ')}"
    proc = spawn(interp, args,
      stdio: ['ignore','pipe','pipe']
      env: Object.assign({}, process.env, envOverrides)
    )
    proc.stdout.on 'data', (buf) -> process.stdout.write prefixLines("┆ #{stepName} | ", buf.toString())
    proc.stderr.on 'data', (buf) -> process.stderr.write prefixLines("! #{stepName} | ", buf.toString())
    proc.on 'error', (err) ->
      console.error "! #{stepName}: spawn error", err
      reject err
    proc.on 'exit', (code, signal) ->
      if code is 0
        console.log "✅ #{stepName}: done"
        resolve true
      else
        msg = if signal then "#{stepName} terminated by #{signal}" else "#{stepName} failed (exit #{code})"
        console.error "! #{stepName}: #{msg}"
        reject new Error msg

# --------------------------------------
# Main
# --------------------------------------
M = new Memo()

main = ->
  ensureSingleInstance()

  baseRecipe = process.argv[2] or path.join(EXEC, 'recipes', 'full_pipeline.yaml')
  dotOut     = process.env.DOT_OUT or process.argv[3] or null
  DEBUG      = !!(process.env.DEBUG? and String(process.env.DEBUG).toLowerCase() in ['1','true','yes'])

  console.log "CWD:", process.cwd()
  console.log "EXEC:", EXEC
  banner "Recipe (base): #{baseRecipe}"

  defaultConfig = path.join(EXEC, 'config', 'default.yaml')
  overridePath  = path.join(process.cwd(), 'override.yaml')

  expPath = createExperimentYaml(baseRecipe, defaultConfig, overridePath)
  spec    = loadYamlSafe(expPath)

  # --- Discover steps from flat top-level map ---
  steps = discoverSteps(spec)
  console.log "Discovered steps:", Object.keys(steps).join(', ') or '(none)'
  order = toposort(steps)
  console.log "Topo order:", order.join(' → ')
  if dotOut? then emitDot steps, dotOut

  # Watch for step finishes (debug)
  M.waitForRegex /^done:/, (k,v) -> console.log "DEBUG done-signal:", k

  # --- Fire rules ---
  for own name, def of steps
    do (name, def) ->
      fire = ->
        if DEBUG then return debugHandleStep(name, def)

        # Build STEP_PARAMS_JSON from def minus run/depends_on
        paramsObj = {}
        for own k, v of def
          continue if k is 'run' or k is 'depends_on'
          paramsObj[k] = v

        stepEnv =
          CFG_OVERRIDE: expPath
          STEP_NAME: name
          STEP_PARAMS_JSON: JSON.stringify(paramsObj)

        scriptAbs = path.join(EXEC, def.run)
        runStepScript(name, scriptAbs, stepEnv)
          .then -> M.saveThis "done:#{name}", true
          .catch (err) ->
            console.error "! #{name}: step failed, continuing"
            console.error err.stack or err
            M.saveThis "done:#{name}", false

      deps = def.depends_on or []
      if deps.length is 0
        console.log "▶️ starting root step #{name}"
        fire()
      else
        console.log "⏳ waiting for deps of #{name}: #{deps.join(', ')}"
        M.waitFor (deps.map (d)-> "done:#{d}"), -> fire()

  finals = terminalSteps(steps)
  Promise.all( finals.map((s)-> M.theLowdown(s).notifier) ).then ->
    banner "🌟 Pipeline finished (final steps: #{finals.join(', ')})"
    process.exit(0)
  .catch (e) ->
    console.error "Pipeline failed:", e.message
    process.exit(1)

process.on 'SIGINT', ->
  console.log "\n(CTRL+C) Shutting down…"
  process.exit(130)

main()
