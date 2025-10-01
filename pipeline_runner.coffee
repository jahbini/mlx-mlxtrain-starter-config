###
    pipeline_runner.coffee
    A lightweight Petri‑net/DAG recipe runner using your Memo kernel.
    - Reads a YAML recipe (override) with `pipeline.steps`
    - Each step has: run: <script.py>, depends_on: [ ... ]
    - Spawns `python <script.py>` with CFG_OVERRIDE env set to the recipe file
    - Parallelizes independent steps; waits for terminal steps to finish
    - Emits optional Graphviz DOT if DOT_OUT env/arg is set
    Usage:
      coffee pipeline_runner.coffee [recipes/full_pipeline.yaml] [dot_out.dot]
###

fs       = require 'fs'
path     = require 'path'
yaml     = require 'js-yaml'
{ spawn }= require 'child_process'

# --------------------------------------
# Memo kernel (kept close to your version)
# --------------------------------------
class Memo
  constructor: (@evaluator) ->
    @MM = {}
    @regexListeners = []  # Store listeners for regex matches

  memoLog: (key) ->
    console.log "Snapping #{key}", @MM[key]

  saveThis: (key, value) ->
    return @MM[key] if @MM[key]? and value == @MM[key].value

    console.log "saving key,value",key,value
    oldResolver = @MM[key]?.resolver ? null
    breaker = null

    maybe = new Promise (resolve, reject) ->
      breaker = resolve

    @MM[key] =
      value: value
      notifier: maybe
      resolver: breaker

    oldResolver value if oldResolver  # Notify subscribers of the new cache value
    maybe.then (newvalue) => @MM[key].value = newvalue

    # Check regex listeners for any match on the new key
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
    # Wait for ALL deps to settle before andDo
    Promise.all(dependants).then andDo

  notifyMe: (n, andDo) ->
    newValue = (@theLowdown n).value
    while true
      currentValue = newValue
      andDo newValue
      while currentValue == newValue
        newValue = (await @MM[n].notifier).value

  # Wait for regex matches on existing and future keys
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

# Petri/DAG execution
M = new Memo()

# Spawn a python script, prefixing logs with the step name
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

# Validate and normalize the pipeline spec
normalizePipeline = (spec = {"pipeline":[]}  ) ->
  console.log "Pipeline", spec
  steps = spec.pipeline.steps
  # prune steps with depends_on: [never]
  for own name, def of steps
    if def.depends_on? and def.depends_on.length is 1 and def.depends_on[0] is "never"
      console.log "⏭️ removing step #{name} (depends_on: never)"
      delete steps[name]

  # steps can be a mapping {name: {run, depends_on}} or an array of names (legacy)
  if Array.isArray(steps)
    obj = {}
    for name in steps
      obj[name] = { run: null, depends_on: [] }
    steps = obj

  # fill defaults, validate fields
  for own name, def of steps
    unless def?.run?
      throw new Error "Step '#{name}' missing 'run:' script path"
    def.depends_on ?= []
    unless Array.isArray(def.depends_on)
      throw new Error "Step '#{name}'.depends_on must be an array"
  steps

# Detect cycles using Kahn's algorithm; return topological order (names array)
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

# Compute terminal steps (no one depends on them)
terminalSteps = (steps) ->
  dependents = new Set()
  for own name, def of steps
    for dep in def.depends_on
      dependents.add dep
  (n for own n, _ of steps when not dependents.has(n))

# Optional: emit Graphviz DOT
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
# Main
# --------------------------------------
main = ->
  overridePath = process.env.CFG_OVERRIDE ? process.argv[1] ? 'recipes/full_pipeline.yaml'
  dotOut      = process.env.DOT_OUT ? process.argv[3] ? null

  console.log "CWD is", process.cwd()
  console.log "JIM main yaml",process.argv[2]
  console.log "JIM cfg EXEC",process.env.EXEC
  console.log "JIM cfg override",process.env.CFG_OVERRIDE
  banner "Recipe: #{overridePath}"
  console.log "LOADING DEFAULT"
  spec = yaml.load fs.readFileSync( process.argv[2], 'utf8')
  console.log "LOADING OVeRRIDE"
  local = yaml.load fs.readFileSync("override.yaml" , 'utf8')
  console.log "JIM", overridePath,spec || overridePath
  steps = normalizePipeline spec 
  console.log "JIM STEPS", steps

  order = toposort steps
  console.log "Topo order:", order.join(" → ")

  if dotOut? then emitDot steps, dotOut


  M.waitForRegex /^done:/, (k,v) ->
    console.log "DEBUG done-signal:", k

  # For each step, set up its firing rule
  for own name, def of steps
    do (name, def) ->
      fire = ->
        runPythonScript(name, process.env.EXEC + "/" + def.run, { CFG_OVERRIDE: overridePath })
          .then ->
            M.saveThis "done:#{name}", true

      if def.depends_on.length is 0
        console.log "▶️ starting root step #{name}"
        fire()
      else
        console.log "⏳ waiting for deps of #{name}: #{def.depends_on.join ', '}"
        M.waitFor (def.depends_on.map (d) -> "done:#{d}"), ->
          fire()
  # Wait for all terminal steps to complete
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
