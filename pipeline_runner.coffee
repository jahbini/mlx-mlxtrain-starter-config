#!/usr/bin/env coffee
###
  pipeline_runner.coffee  — Flat-Step Runner (Memo + MLX)
  -------------------------------------------------------
  Future-adoptable runner with:
    • Unified Memo across steps
    • Reactive file persistence for *.json / *.csv memo keys
    • Centralized MLX runner exposed as M.mlx_runner(params)
    • Optional declarative MLX steps via run_mlx: true + mlx: { ... }

  Flat-step model (unchanged):
    - Each top-level key that has a "run:" is a step (except "run" global).
    - "depends_on" is a string or array.
    - Precedence: recipe < config/default.yaml < override.yaml
    - experiment.yaml is written to PWD and used by steps.

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
# Memo kernel with reactive persistence
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

  # --- Reactive persistence for *.json / *.csv ------------------------------
  enableFilePersistence: (baseDir = path.join(process.cwd(), 'eval_out')) ->
    writeJSON = (p, obj) ->
      fs.mkdirSync(path.dirname(p), {recursive:true})
      fs.writeFileSync(p, JSON.stringify(obj, null, 2), 'utf8')
    writeCSV = (p, rows) ->
      return unless rows?.length
      keys = Object.keys(rows[0])
      buf = [keys.join(',')]
      for r in rows
        vals = (String(r[k] or '').replace(/,/g, ';') for k in keys)
        buf.push vals.join(',')
      fs.mkdirSync(path.dirname(p), {recursive:true})
      fs.writeFileSync(p, buf.join('\n'), 'utf8')

    @regexListeners.push
      regex: /\.json$/i
      callback: (key, value) ->
        dest = path.join(baseDir, key)
        try
          writeJSON dest, value
          console.log "💾 Memo→JSON:", dest
        catch e
          console.error "❌ JSON write failed:", dest, e.message

    @regexListeners.push
      regex: /\.csv$/i
      callback: (key, value) ->
        dest = path.join(baseDir, key)
        try
          writeCSV dest, value
          console.log "💾 Memo→CSV:", dest
        catch e
          console.error "❌ CSV write failed:", dest, e.message

# --------------------------------------
# Utilities
# --------------------------------------
banner = (msg) -> console.log "\n=== #{msg} ==="
prefixLines = (pfx, s) -> (s ? '').split(/\r?\n/).map((l)-> (pfx + l)).join("\n")
isPlainObject = (o) -> Object.prototype.toString.call(o) is '[object Object]'

deepMerge = (target, source) ->
  # Predictable deep merge:
  # - objects merge by key (source overwrites target values)
  # - arrays REPLACE (no concat)
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
  merged = deepMerge {}, defaults
  merged = deepMerge merged, recipe
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
    # A step must define either 'run:' or 'run_mlx: true'
    if val.run? or val.run_mlx is true
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
      def.depends_on = deps
      steps[key] = def
  if Object.keys(steps).length is 0
    throw new Error "No steps discovered in experiment.yaml (expect top-level keys with 'run:' or 'run_mlx: true')"
  steps

# --------------------------------------
# Topological sort (+ DOT/terminal helpers)
# --------------------------------------
toposort = (steps) ->
  indeg = {}; graph = {}
  for own name, def of steps
    indeg[name] = 0; graph[name] = []
  for own name, def of steps
    for dep in def.depends_on or []
      unless steps[dep]?
        throw new Error "Undefined dependency '#{dep}' (referenced by '#{name}')"
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
# Single-instance guard (unchanged)
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
# DEBUG / touch behavior (unchanged)
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
# Centralized MLX runner (exposed to steps via M.mlx_runner)
# params example:
#   {
#     module: "mlx_lm",            # default "mlx_lm"
#     entry:  "generate",          # e.g., "generate" subcommand or model name
#     args:   ["--input", "in"],   # argv array after entry
#     cwd:    "/path/dir",         # optional cwd
#     env:    { MODEL: "..." },    # optional env
#     capture_stdout_key: "foo.txt" or "memo:key" # optional memo key for stdout
#   }
# Returns a Promise<string> (stdout)
# --------------------------------------
runMLX = (stepName, params={}) ->
  new Promise (resolve, reject) ->
    mod   = params.module ? 'mlx_lm'
    entry = params.entry  ? 'generate'
    args  = params.args   ? []
    cmd   = 'python'
    argv  = ['-m', mod, entry].concat args

    console.log "⚙️  #{stepName}: mlx #{argv.join(' ')}"
    proc = spawn cmd, argv,
      cwd: params.cwd ? process.cwd()
      env: Object.assign({}, process.env, params.env or {})
      stdio: ['ignore','pipe','pipe']

    out = ''
    proc.stdout.on 'data', (d) ->
      s = d.toString()
      out += s
      process.stdout.write prefixLines("mlx| #{stepName} | ", s)

    proc.stderr.on 'data', (d) ->
      process.stderr.write prefixLines("! mlx #{stepName} | ", d.toString())

    proc.on 'error', (e) -> reject e
    proc.on 'exit', (code) ->
      if code is 0
        resolve out
      else
        reject new Error "mlx failed #{code}"

# --------------------------------------
# Spawn a step with clear logging
# • If def.run_mlx: true → use runMLX with def.mlx params
# • Else spawn external .py / .coffee script
# --------------------------------------
runStep = (stepName, def, expPath) ->
  new Promise (resolve, reject) ->
    # Declarative MLX step path (no external script)
    if def.run_mlx is true
      params = def.mlx ? {}
      runMLX(stepName, params)
        .then (stdout) ->
          # Publish raw stdout if caller requested a memo key
          if typeof params.capture_stdout_key is 'string'
            M.saveThis params.capture_stdout_key, stdout
          # Always publish a generic memo slot for traceability
          M.saveThis "#{stepName}:mlx_stdout", stdout
          resolve true
        .catch (e) -> reject e
      return

    # External script path (CoffeeScript/Python)
    unless def.run?
      return reject new Error "Step '#{stepName}' missing 'run' (and not run_mlx)"

    scriptAbs = path.join(EXEC, def.run)
    interp = null
    args = []
    if /\.py$/i.test(scriptAbs)
      interp = 'python'
      args = ['-u', scriptAbs]
    else if /\.coffee$/i.test(scriptAbs)
      interp = 'coffee'
      args = [scriptAbs]
    else
      return reject new Error "Unknown script type for #{stepName}: #{scriptAbs}"

    console.log "▶️  #{stepName}: #{interp} #{args.join(' ')}"
    proc = spawn(interp, args,
      stdio: ['ignore','pipe','pipe']
      env: Object.assign({}, process.env,
        CFG_OVERRIDE: expPath
        STEP_NAME: stepName
        STEP_PARAMS_JSON: JSON.stringify(def)
      )
    )
    proc.stdout.on 'data', (buf) -> process.stdout.write prefixLines("┆ #{stepName} | ", buf.toString())
    proc.stderr.on 'data', (buf) -> process.stderr.write prefixLines("! #{stepName} | ", buf.toString())
    proc.on 'error', (err) -> reject err
    proc.on 'exit', (code, signal) ->
      if code is 0 then resolve true \
      else
        msg = if signal then "#{stepName} terminated by #{signal}" else "#{stepName} failed (exit #{code})"
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

  # --- Memo wiring ---
  M.enableFilePersistence path.join(process.cwd(), 'eval_out')
  M.saveThis "experiment.yaml", spec

  # Expose MLX runner to steps (for new-style CoffeeScript modules)
  M.mlx_runner = (params={}) -> runMLX("mlx", params)

  # --- Discover steps from flat top-level map ---
  steps = discoverSteps(spec)
  console.log "Discovered steps:", Object.keys(steps).join(', ') or '(none)'
  order = toposort(steps)
  console.log "Topo order:", order.join(' → ')
  if dotOut? then emitDot steps, dotOut

  # Watch for step finishes (debug)
  M.waitForRegex /^done:/, (k,v) -> console.log "DEBUG done-signal:", k

  # --- Fire rules (respect depends_on) ---
  for own name, def of steps
    do (name, def) ->
      fire = ->
        if DEBUG then return debugHandleStep(name, def)

        runStep(name, def, expPath)
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
  Promise.all( finals.map((s)-> M.theLowdown("done:#{s}").notifier) ).then ->
    banner "🌟 Pipeline finished (final steps: #{finals.join(', ')})"
    process.exit(0)
  .catch (e) ->
    console.error "Pipeline failed:", e.message
    process.exit(1)

process.on 'SIGINT', ->
  console.log "\n(CTRL+C) Shutting down…"
  process.exit(130)

main().catch (e) ->
  console.error "Fatal:", String(e?.message or e)
  process.exit(1)