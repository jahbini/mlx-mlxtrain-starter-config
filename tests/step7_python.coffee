###
  step7_python.coffee
  -------------------
  Example of a memo-aware step that spawns a Python process.
  Demonstrates in-process spawning while sharing @memo.
###

{ spawnSync } = require 'child_process'

@step =
  desc: "Run a Python interpreter and capture its version."
  action: (M) ->

    console.log "🐍 Running Python via @step notation..."

    cmd = 'python'
    args = ['-V']

    result = spawnSync(cmd, args, encoding: 'utf8')

    if result.error
      console.error "❌ Python failed:", result.error
      M.saveThis 'python_info.json',
        { status: 'failed', error: String(result.error) }
      return

    output = (result.stdout or result.stderr).trim()
    console.log "✅ Python responded:", output

    # Memo result as JSON
    M.saveThis 'python_info.json',
      { status: 'ok', version: output }

    M.saveThis 'done:step7_python', true
    console.log "💾 Memo updated: python_info.json"
    return
