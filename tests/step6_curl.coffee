###
  step6_curl.coffee
  -----------------
  Example of a memo-aware step that spawns a bash curl request.
  Demonstrates how external commands can run within the @step model.
###

{ spawnSync } = require 'child_process'

@step =
  desc: "Spawn a curl request and memoize its result."
  action: (M) ->

    console.log "🌐 Running curl via @step notation..."

    # Example: fetch a simple HTTP header from example.com
    cmd = 'curl'
    args = ['-sI', 'https://example.com']

    result = spawnSync(cmd, args, encoding: 'utf8')

    if result.error
      console.error "❌ CURL failed:", result.error
      M.saveThis 'curl_result.json',
        { status: 'failed', error: String(result.error) }
      return

    output = result.stdout.trim()
    console.log "✅ CURL completed; length:", output.length

    # Memo the output to JSON file automatically
    M.saveThis 'curl_result.json',
      { status: 'ok', output }

    # Simulate chaining data
    M.saveThis 'done:step6_curl', true

    console.log "💾 Memo updated: curl_result.json"
    return
