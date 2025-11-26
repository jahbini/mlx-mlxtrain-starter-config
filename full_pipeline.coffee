#!/usr/bin/env coffee
###
  pipeline_runner.coffee — Flat-Step Runner (Evaluator-Compatible)
  ---------------------------------------------------------------
  Unified runtime with:
    • Single Memo shared across steps
    • Reactive file persistence for *.json / *.csv / any path-like memo keys
    • In-process execution for CoffeeScript steps defining @step = { action }
    • Centralized MLX runner via M.mlx_runner(params)
    • Declarative MLX steps supported via run_mlx: true + mlx: { ... }
    • depends_on DAG
    • Restart-resume via state/done-*.json

  NEW:
    • NO recipes directory
    • Stage selection via config/<stage>.yaml only
    • experiment.yaml = config < override
###

fs        = require 'fs'
path      = require 'path'
yaml      = require 'js-yaml'
{ spawn, execSync } = require 'child_process'
CoffeeScript = require 'coffeescript'
CoffeeScript.register()

EXEC = process.env.EXEC
CWD  = process.cwd()

# -------------------------------------------------------------------
# Memo with Meta-Dispatcher
# -------------------------------------------------------------------
class Memo
  constructor: ->
    @MM = {}
    @metaRules = []
    @currentStep = null
    @initializeMetaRules CWD

  saveThis: (key, value) ->
    entry = @MM[key]

    unless entry?
      breaker = null
      maybe = new Promise (resolve) -> breaker = resolve
      entry =
        value: value
        notifier: maybe
        resolver: breaker
        meta: null
      @MM[key] = entry

      entry.meta = @selectMetaHandler(key)

      try v = entry.meta(key, value) catch e then console.error "Meta init error:", e.message
      entry.value = v if v
      return entry

    oldResolver = entry.resolver ? null
    breaker = null
    maybe = new Promise (resolve) -> breaker = resolve

    entry.resolver = breaker
    entry.notifier = maybe
    entry.value = value

    oldResolver value if oldResolver

    try entry.meta(key, value) catch e then console.error "Meta update error:", key, e.message

    maybe.then (newval) -> entry.value = newval
    entry

  theLowdown: (key) ->
    return @MM[key] if @MM[key]?
    @saveThis key, undefined

  waitFor: (keys, andDo) ->
    unsatisfied = []
    for key in keys
      entry = @theLowdown(key)
      if entry.value is true then continue
      unsatisfied.push entry.notifier

    if unsatisfied.length is 0
      try andDo() catch e then console.error "waitFor immediate:", e.message
      return

    Promise.all(unsatisfied).then ->
      try andDo() catch e then console.error "waitFor deferred:", e.message

  selectMetaHandler: (key) ->
    for rule in @metaRules
      if rule.regex.test(key)
        return rule.handler
    (k,v)-> return

  addMetaRule: (name, regex, handler) ->
    @metaRules.push {name, regex, handler}

  initializeMetaRules: (baseDir) ->
    fs = require 'fs'
    path = require 'path'

    writeJSON = (dest, obj) ->
      fs.mkdirSync(path.dirname(dest), {recursive:true})
      fs.writeFileSync(dest, JSON.stringify(obj, null, 2), 'utf8')

    writeCSV = (dest, rows) ->
      fs.mkdirSync(path.dirname(dest), {recursive:true})
      if typeof rows is 'string'
        fs.writeFileSync(dest, rows, 'utf8'); return
      unless Array.isArray(rows)
        throw new Error "CSV expects array or string"
      return unless rows.length and typeof rows[0] is 'object'
      keys = Object.keys(rows[0])
      buf = [keys.join(',')]
      for r in rows
        vals = (String(r[k] ? '').replace(/,/g,';') for k in keys)
        buf.push vals.join(',')
      fs.writeFileSync(dest, buf.join('\n') + '\n', 'utf8')

    writeJSONL = (dest, arr) ->
      fs.mkdirSync(path.dirname(dest), {recursive:true})
      fs.writeFileSync dest, ''
      for t in arr
        fs.appendFileSync dest, JSON.stringify(t) + "\n"

    # 1) MLX dispatcher
    @addMetaRule "mlx-lm agent",
      /^donkeyButt mlx-lm:(train|generate|fuse|convert|lora)$/
      (key, payload) =>
        return unless payload?
        cmdType = key.split(":")[1]
        @runMlxCommand key, cmdType, payload

    # 2) JSONL
    @addMetaRule "jsonl-writer",
      /\.jsonl$/i,
      (key, value) ->
        return unless value?
        dest = path.join(baseDir, key)
        writeJSONL(dest, value)
        console.log "💾 JSONL:", dest

    # 3) JSON
    @addMetaRule "json-writer",
      /\.json$/i,
      (key, value) ->
        return unless value?
        dest = path.join(baseDir, key)
        writeJSON(dest, value)
        console.log "💾 JSON:", dest

    # 4) CSV
    @addMetaRule "csv-writer",
      /\.csv$/i,
      (key, value) ->
        return unless value?
        dest = path.join(baseDir, key)
        writeCSV(dest, value)
        console.log "💾 CSV:", dest

    # 5) Slash paths
    @addMetaRule "slash-path-writer",
      /^(?=.*\/)(?!.*\.[A-Za-z0-9]{1,8}$).+$/,
      (key, value) ->
        return unless value?
        dest = path.join(baseDir, key)
        fs.mkdirSync(path.dirname(dest), {recursive:true})
        data = if Buffer.isBuffer(value) then value else JSON.stringify(value, null, 2)
        fs.writeFileSync(dest, data, 'utf8')
        console.log "💾 FILE:", dest

    @addMetaRule "noop",
      /.^/,
      (k,v)-> return

  demand: (key) ->
    return @MM[key] if @MM[key]?

    fs   = require 'fs'
    path = require 'path'
    abs  = path.join(process.cwd(), key)

    unless fs.existsSync(abs)
      return undefined

    raw = null
    try raw = fs.readFileSync(abs, 'utf8')
    catch e then return undefined

    value = null
    if /\.jsonl$/i.test(key)
      lines = raw.split(/\r?\n/).filter (l)-> l.trim().length
      objs = []
      for l in lines
        try objs.push JSON.parse(l)
        catch e then continue
      value = objs
    else
      value = raw

    entry =
      value: value
      notifier: null
      resolver: null
      meta: (-> return)

    @MM[key] = entry
    entry

  callMLX: (cmdType, payload) ->
    child = require 'child_process'

    buildArgs = (cmdType, params) ->
      args = []
      args.push "-m", "mlx_lm", cmdType
      for k,v of params
        args.push "--#{k}"
        args.push v if v
      args

    args = buildArgs(cmdType, payload)
    cmd = "python"

    try
      proc = child.spawnSync(cmd, args, {encoding:'utf8'})
    catch e
      console.error "MLX spawn failed:", e.message
      throw e

    if proc.status isnt 0
      console.error "MLX stderr:\n", proc.stderr
      throw new Error "MLX command failed"

    proc.stdout

# -------------------------------------------------------------------
# Utilities
# -------------------------------------------------------------------

banner = (msg) -> console.log "\n=== #{msg} ==="
prefixLines = (pfx, s) -> (s ? '').split(/\r?\n/).map((l)-> pfx + l).join("\n")
isPlainObject = (o) -> Object.prototype.toString.call(o) is '[object Object]'

deepMerge = (target, source) ->
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

# -------------------------------------------------------------------
# Restart helpers
# -------------------------------------------------------------------
bootstrapDoneFlags = (M, spec) ->
  stateDir = path.join(process.cwd(), 'state')
  return unless fs.existsSync(stateDir)

  for own name, val of spec
    continue unless isPlainObject(val)
    if val.run? or val.run_mlx is true
      fp = path.join(stateDir, "done-#{name}.json")
      continue unless fs.existsSync(fp)
      try
        st = JSON.parse fs.readFileSync(fp, 'utf8')
        if st?.done is true
          console.log "↺ restoring done:#{name} from state/"
          M.saveThis "done:#{name}", true
      catch e
        console.error "state restore failed for #{name}:", e.message

persistDone = (stepName) ->
  try
    stateDir = path.join(process.cwd(), 'state')
    fs.mkdirSync stateDir, {recursive:true}
    fp = path.join(stateDir, "done-#{stepName}.json")
    payload =
      done: true
      finished_at: new Date().toISOString()
    fs.writeFileSync fp, JSON.stringify(payload, null, 2), 'utf8'
  catch e
    console.error "persistDone failed:", e.message

# -------------------------------------------------------------------
# Create experiment.yaml (config < override)
# -------------------------------------------------------------------
createExperimentYaml = (configPath, overridePath) ->
  banner "🔧 Creating experiment.yaml"

  config   = loadYamlSafe(configPath)
  override = loadYamlSafe(overridePath)

  merged = deepMerge {}, config
  merged = deepMerge merged, override

  expPath = path.join(process.cwd(), 'experiment.yaml')
  fs.writeFileSync expPath, yaml.dump(merged), 'utf8'
  console.log "✅ Wrote experiment.yaml:", expPath
  expPath

# -------------------------------------------------------------------
# Step discovery
# -------------------------------------------------------------------
discoverSteps = (spec) ->
  steps = {}
  for own key, val of spec
    continue if key is 'run'
    continue unless isPlainObject(val)
    if val.run? or val.run_mlx is true
      deps = []
      if val.depends_on?
        if Array.isArray(val.depends_on)
          deps = val.depends_on.slice()
        else if typeof val.depends_on is 'string'
          deps = [val.depends_on]
        if deps.length is 1 and String(deps[0]).toLowerCase() is 'never'
          console.log "⏭️ skipping #{key}"
          continue
      def = {}
      for own k2,v2 of val then def[k2] = v2
      def.depends_on = deps unless deps.length == 0
      steps[key] = def
  if Object.keys(steps).length is 0
    throw new Error "No steps found in experiment.yaml"
  steps

# -------------------------------------------------------------------
# DAG helpers
# -------------------------------------------------------------------
toposort = (steps) ->
  indeg = {}; graph = {}
  for own name, def of steps
    indeg[name] = 0; graph[name] = []
  for own name, def of steps
    for dep in def.depends_on or []
      unless steps[dep]?
        throw new Error "Undefined dependency '#{dep}'"
      indeg[name] += 1
      graph[dep].push name

  q = (n for own n,d of indeg when d is 0)
  order = []
  while q.length
    n = q.shift()
    order.push n
    for m in graph[n]
      indeg[m] -= 1
      q.push m if indeg[m] is 0

  if order.length isnt Object.keys(steps).length
    missing = Object.keys(steps).filter (k)-> order.indexOf(k) is -1
    console.error "⚠️ DAG anomaly missing:", missing.join(', ')
  order

terminalSteps = (steps) ->
  dependents = new Set()
  for own name, def of steps
    for dep in def.depends_on or [] then dependents.add dep
  (n for own n,_ of steps when not dependents.has(n))

emitDot = (steps, outPath) ->
  try
    lines = ['digraph pipeline {','  rankdir=LR;']
    for own n,_ of steps
      lines.push "  \"#{n}\" [shape=box];"
    for own n, d of steps
      for dep in d.depends_on or []
        lines.push "  \"#{dep}\" -> \"#{n}\";"
    lines.push '}'
    fs.writeFileSync outPath, lines.join("\n"), "utf8"
    console.log "🖼 DOT:", outPath
  catch e
    console.error "DOT write failed:", e.message

# -------------------------------------------------------------------
# Single instance guard
# -------------------------------------------------------------------
ensureSingleInstance = ->
  try
    scriptPath = path.resolve(__filename)
    out = execSync("ps -Ao pid,command | grep 'coffee' | grep '#{scriptPath}' | grep -v grep || true").toString()
    lines = out.trim().split("\n").filter (l)-> l.length>0
    others = lines.filter (l)-> not l.startsWith(String(process.pid))
    process.exit(0) if others.length>0
  catch err
    console.error "Instance check error:", err.message

# -------------------------------------------------------------------
# MLX Runner
# -------------------------------------------------------------------
runMLX = (stepName, params={}) ->
  new Promise (resolve, reject) ->
    mod   = params.module ? 'mlx_lm'
    entry = params.entry  ? 'generate'
    args  = params.args   ? []
    cmd   = 'python'
    argv  = ['-m', mod, entry].concat args

    console.log "⚙️ #{stepName}: mlx #{argv.join(' ')}"
    proc = spawn cmd, argv,
      cwd: params.cwd ? process.cwd()
      env: Object.assign({}, process.env, params.env or {})
      stdio: ['ignore','pipe','pipe']

    out = ''
    proc.stdout.on 'data', (d) ->
      s = d.toString(); out += s
      process.stdout.write prefixLines("mlx| #{stepName} | ", s)
    proc.stderr.on 'data', (d) ->
      process.stderr.write prefixLines("! mlx #{stepName} | ", d.toString())
    proc.on 'error', (e) -> reject e
    proc.on 'exit', (code) ->
      if code is 0 then resolve out else reject new Error "mlx failed #{code}"

# -------------------------------------------------------------------
# Step Runner
# -------------------------------------------------------------------
isNewStyleStep = (scriptPath) ->
  try src = fs.readFileSync(scriptPath, 'utf8'); /\@step\s*=/.test(src)
  catch e then false

runStep = (stepName, def, expPath, M) ->
  new Promise (resolve, reject) ->
    # Declarative MLX
    if def.run_mlx is true
      params = def.mlx ? {}
      runMLX(stepName, params)
        .then (stdout) ->
          if typeof params.capture_stdout_key is 'string'
            M.saveThis params.capture_stdout_key, stdout
          M.saveThis "#{stepName}:mlx_stdout", stdout
          M.saveThis "done:#{stepName}", true
          persistDone stepName
          resolve true
        .catch (e) ->
          console.error "! #{stepName} mlx:", e.message
          M.saveThis "done:#{stepName}", false
          reject e
      return

    unless def.run?
      return reject new Error "Step '#{stepName}' missing 'run'"

    scriptAbs = path.join(EXEC, def.run)

    # Inline @step
    if /\.coffee$/i.test(scriptAbs) and isNewStyleStep(scriptAbs)
      console.log "⚙️ inline @step:", stepName
      stepModule = require scriptAbs
      step = stepModule?.step or global?.step
      unless step?.action?
        return reject new Error "Missing @step.action in #{stepName}"

      Promise.resolve(step.action(M,stepName))
        .then ->
          M.saveThis "done:#{stepName}", true
          persistDone stepName
          resolve true
        .catch (e) ->
          console.error "! #{stepName}:", e.message
          M.saveThis "done:#{stepName}", false
          reject e
      return

    # Spawn Python or Coffee
    interp = null; args = []
    if /\.py$/i.test(scriptAbs)
      interp = 'python'; args = ['-u', scriptAbs]
    else if /\.coffee$/i.test(scriptAbs)
      interp = 'coffee'; args = [scriptAbs]
    else
      return reject new Error "Unknown script type: #{scriptAbs}"

    console.log "▶️ #{stepName}: #{interp} #{args.join(' ')}"
    proc = spawn interp, args,
      stdio: ['ignore','pipe','pipe']
      env: Object.assign({}, process.env,
        CFG_OVERRIDE: expPath
        STEP_NAME: stepName
        STEP_PARAMS_JSON: JSON.stringify(def)
      )

    proc.stdout.on 'data', (buf) ->
      process.stdout.write prefixLines("┆ #{stepName} | ", buf.toString())
    proc.stderr.on 'data', (buf) ->
      process.stderr.write prefixLines("! #{stepName} | ", buf.toString())
    proc.on 'error', (err) -> reject err
    proc.on 'exit', (code) ->
      if code is 0
        M.saveThis "done:#{stepName}", true
        persistDone stepName
        resolve true
      else
        console.error "! #{stepName} failed:", code
        M.saveThis "done:#{stepName}", false
        reject new Error "#{stepName} failed #{code}"

# -------------------------------------------------------------------
# MAIN
# -------------------------------------------------------------------
main = ->
  ensureSingleInstance()

  stage = process.argv[2] or 'daily'
  dotOut = process.env.DOT_OUT or process.argv[3] or null

  console.log "CWD:", CWD
  console.log "EXEC:", EXEC
  banner "Stage: #{stage}"

  configPath   = path.join(EXEC, 'config', stage+'.yaml')
  overridePath = path.join(CWD, 'override.yaml')

  expPath = createExperimentYaml(configPath, overridePath)
  spec    = loadYamlSafe(expPath)

  M = new Memo()
  M.saveThis "experiment.yaml", spec
  M.mlx_runner = (params={}) -> runMLX("mlx", params)

  bootstrapDoneFlags(M, spec)

  steps = discoverSteps(spec)
  order = toposort(steps)

  console.log "Steps:", Object.keys(steps).join(', ')
  console.log "Topo:", order.join(' → ')
  emitDot steps, dotOut if dotOut?

  for own n,d of steps
    M.saveThis "params/#{n}.json", d

  for own name, def of steps
    do (name, def) ->
      deps = def.depends_on or []
      fire = ->
        runStep(name, def, expPath, M)
          .catch (e) -> console.error "! #{name}:", e.message
          .then ->
            console.log "✓ fini", name
            M.saveThis "done:#{name}", true
            persistDone name

      if deps.length is 0
        console.log "▶️ starting #{name}"
        fire()
      else
        console.log "⏳ waiting for #{name}: #{deps.join(', ')}"
        M.waitFor (deps.map (d)-> "done:#{d}"), -> fire()

  finals = terminalSteps(steps)
  Promise.all( finals.map((s)-> M.theLowdown("done:#{s}").notifier) )
    .then ->
      banner "🌟 Pipeline finished (#{finals.join(', ')})"
      process.exit(0)
    .catch (e) ->
      console.error "Pipeline failed:", e.message
      process.exit(1)

process.on 'SIGINT', ->
  console.log "\n(CTRL+C) Exiting..."
  process.exit(130)

main().catch (e) ->
  console.error "Fatal:", String(e?.message or e)
  process.exit(1)
