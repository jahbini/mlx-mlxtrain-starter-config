###
  pipeline_evaluator.coffee
  Runs evaluation-only pipelines (sanity checks, comparative reporting, etc.)
  Modeled on pipeline_runner.coffee but executes CoffeeScript evaluation steps.
###

fs        = require 'fs'
path      = require 'path'
yaml      = require 'js-yaml'
{ spawn } = require 'child_process'
{ execSync } = require 'child_process'

EXEC = process.env.EXEC 
# --- import Memo and utilities (identical to runner) ---
# [ copy/paste Memo class, deepMerge, normalizePipeline, toposort, etc. from runner ]

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
# Spawn a CoffeeScript script, prefixing logs with the step name
# --------------------------------------
runCoffeeScript = (stepName, scriptPath, envOverrides={}) ->
  new Promise (resolve, reject) ->
    console.log "▶️  #{stepName}: running #{scriptPath}"
    proc = spawn("coffee", [scriptPath],
      stdio: ['ignore', 'pipe', 'pipe']
      env: Object.assign({}, process.env, envOverrides)
    )
    proc.stdout.on 'data', (buf) -> process.stdout.write "┆ #{stepName} | #{buf}"
    proc.stderr.on 'data', (buf) -> process.stderr.write "! #{stepName} | #{buf}"
    proc.on 'error', (err) ->
      console.error "! #{stepName}: spawn error", err
      reject err
    proc.on 'exit', (code) ->
      if code is 0
        console.log "✅ #{stepName}: done"
        M.saveThis "done:#{stepName}", true
        resolve()
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

# --- add this helper once ---
buildEnvOverrides = (prefix = 'CFG_') ->
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

# --- replace createExperimentYaml with this version ---
createExperimentYaml = (basePath, overridePath) ->
  banner "🔧 Creating evaluate.yaml"

  defaultPath = path.join(EXEC, 'config', 'default.yaml')

  baseAbs  = path.resolve(basePath)
  baseDir  = path.dirname(baseAbs)

  defaults = loadYamlSafe(defaultPath)            # 1) global defaults (run/data/eval/etc.)
  base     = loadYamlSafe(basePath)                # 2) recipe
  base     = expandIncludes(base, baseDir)        #    + sub-recipes/includes
  override = loadYamlSafe(overridePath)           # 3) local override.yaml
  envOv    = buildEnvOverrides('CFG_')            # 4) CFG_* environment

  # precedence: defaults < recipe(+includes) < override.yaml < env
  merged = deepMerge {}, defaults
  merged = deepMerge merged, base
  merged = deepMerge merged, override
  merged = deepMerge merged, envOv

  # tiny sanity signal (optional)
  if not (merged?.run? and merged.run.output_dir?)
    console.warn "⚠️  run.output_dir missing after merge; check defaults/override."

  expPath = path.join(process.cwd(), 'evaluate.yaml')
  fs.writeFileSync expPath, yaml.dump(merged), 'utf8'
  console.log "✅ Wrote evaluate.yaml (defaults+recipe+override+env):", expPath

  expPath

# --------------------------------------
# Main
# --------------------------------------
main = ->

  baseRecipe = process.env.EXEC +  '/recipes/eval_pipeline.yaml'
  dotOut     = process.env.DOT_OUT ? process.argv[3] ? null
  DEBUG      = !!(process.env.DEBUG? and String(process.env.DEBUG).toLowerCase() in ['1','true','yes'])

  console.log "CWD:", process.cwd()
  banner "Eval Recipe (base): #{baseRecipe}"

  # Flatten config same way
  overridePath = path.join(process.cwd(), 'override.yaml')
  expPath = createExperimentYaml(baseRecipe, overridePath)

  spec  = loadYamlSafe(expPath)
  steps = normalizePipeline spec
  order = toposort steps
  console.log "Topo order:", order.join(" → ")
  if dotOut? then emitDot steps, dotOut

  for own name, def of steps
    do (name, def) ->
      fire = ->
        if DEBUG
          return debugHandleStep(name, def)

        console.log "JIM", process.cwd(),"and EXEC",EXEC
        runCoffeeScript(name, process.env.EXEC + "/" + def.run,
          CFG_OVERRIDE: expPath
          STEP_NAME: name
        ).then ->
          M.saveThis "done:#{name}", true
        .catch (err) ->
          console.error "! #{name}: step failed"
          console.error err.stack or err
          M.saveThis "done:#{name}", false

      if def.depends_on.length is 0
        fire()
      else
        M.waitFor (def.depends_on.map (d) -> "done:#{d}"), -> fire()

  finals = terminalSteps steps
  Promise.all(finals.map((s)-> M.theLowdown(s).notifier)).then ->
    banner "🌟 Evaluation pipeline finished (final: #{finals.join(', ')})"
    process.exit(0)
  .catch (e) ->
    console.error "Evaluation failed:", e.message
    process.exit(1)

process.on 'SIGINT', ->
  console.log "\n(CTRL+C) Stopping evaluator…"
  process.exit(130)

main()
