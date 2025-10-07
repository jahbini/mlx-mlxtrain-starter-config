###
  pipeline_evaluator.coffee
  Evaluation-only pipeline runner.
  Uses the *frozen* run/experiment.yaml from each training directory.
###

fs        = require 'fs'
path      = require 'path'
yaml      = require 'js-yaml'
{ spawn } = require 'child_process'

EXEC = process.env.EXEC or process.cwd()

# --------------------------------------
# Memo kernel (minimal, same as runner)
# --------------------------------------
class Memo
  constructor: -> @MM = {}

  saveThis: (key, value) ->
    @MM[key] =
      value: value
      notifier: Promise.resolve(value)
    @MM[key]

  theLowdown: (key) ->
    return @MM[key] if @MM[key]?
    @saveThis key, undefined

  waitFor: (aList, andDo) ->
    dependants = for key in aList
      d = @theLowdown key
      d.notifier
    Promise.all(dependants).then andDo

M = new Memo()

# --------------------------------------
# Utilities
# --------------------------------------
banner = (msg) -> console.log "\n=== #{msg} ==="

normalizePipeline = (spec) ->
  steps = spec.pipeline?.steps
  unless steps?
    throw new Error "Spec missing pipeline.steps"

  for own name, def of steps
    unless def?.run?
      throw new Error "Step '#{name}' missing run: path"
    def.depends_on ?= []
  steps

toposort = (steps) ->
  indeg = {}; graph = {}
  for own n, def of steps
    indeg[n] = 0; graph[n] = []
  for own n, def of steps
    for dep in def.depends_on
      throw new Error "Undefined dep '#{dep}'" unless steps[dep]?
      indeg[n] += 1
      graph[dep].push n
  q = (n for own n, d of indeg when d is 0)
  order = []
  while q.length
    n = q.shift()
    order.push n
    for m in graph[n]
      indeg[m] -= 1
      q.push m if indeg[m] is 0
  if order.length isnt Object.keys(steps).length
    throw new Error "Cycle in pipeline"
  order

terminalSteps = (steps) ->
  dependents = new Set()
  for own n, def of steps
    for dep in def.depends_on
      dependents.add dep
  (n for own n, _ of steps when not dependents.has(n))

# --------------------------------------
# Spawn eval script
# --------------------------------------
runCoffeeScript = (stepName, scriptPath, envOverrides={}) ->
  new Promise (resolve, reject) ->
    console.log "▶️  #{stepName}: running #{scriptPath}"
    proc = spawn "coffee", [scriptPath],
      stdio: ['ignore','pipe','pipe']
      env: Object.assign {}, process.env, envOverrides

    proc.stdout.on 'data', (b) -> process.stdout.write "┆ #{stepName} | #{b}"
    proc.stderr.on 'data', (b) -> process.stderr.write "! #{stepName} | #{b}"

    proc.on 'exit', (code) ->
      if code is 0
        console.log "✅ #{stepName}: done"
        M.saveThis "done:#{stepName}", true
        resolve()
      else
        reject new Error "#{stepName} failed (#{code})"

# --------------------------------------
# Main
# --------------------------------------
main = ->
  targetDir = process.argv[2]
  unless targetDir?
    console.error "Usage: coffee pipeline_evaluator.coffee /path/to/training_dir"
    process.exit(1)

  runDir = path.join targetDir, 'run'
  expPath = path.join runDir, 'experiment.yaml'
  unless fs.existsSync(expPath)
    console.error "Missing experiment.yaml at #{expPath}"
    process.exit(1)

  # Load eval pipeline recipe (always from EXEC/recipes)
  baseRecipe = path.join EXEC, 'recipes', 'eval_pipeline.yaml'
  spec = yaml.load fs.readFileSync(baseRecipe, 'utf8')
  steps = normalizePipeline spec
  order = toposort steps
  banner "Eval pipeline order: #{order.join ' → '}"

  for own name, def of steps
    do (name, def) ->
      fire = ->
        runCoffeeScript name, path.join(EXEC, def.run),
          CFG_OVERRIDE: expPath
          STEP_NAME: name
        .then -> M.saveThis "done:#{name}", true
        .catch (err) ->
          console.error "! #{name}: #{err.message}"
          M.saveThis "done:#{name}", false

      if def.depends_on.length is 0
        fire()
      else
        M.waitFor (def.depends_on.map (d) -> "done:#{d}"), -> fire()

  finals = terminalSteps steps
  Promise.all(finals.map((s)-> M.theLowdown(s).notifier)).then ->
    banner "🌟 Evaluation finished for #{targetDir}"
    process.exit(0)
  .catch (e) ->
    console.error "Evaluation failed:", e.message
    process.exit(1)

main()