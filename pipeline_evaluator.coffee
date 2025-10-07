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

# --- import Memo and utilities (identical to runner) ---
# [ copy/paste Memo class, deepMerge, normalizePipeline, toposort, etc. from runner ]

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
# Main
# --------------------------------------
main = ->

  baseRecipe = process.argv[2] ? 'recipes/eval_pipeline.yaml'
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
