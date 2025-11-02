#!/usr/bin/env coffee
###
Step 4 — wait: simulate asynchronous work
###
@step =
  name: 'step4_wait'
  action: (M) ->
    console.log "🕐 [step4_wait] working..."
    new Promise (resolve) ->
      setTimeout ->
        M.saveThis "state/wait.json", { done: true, timestamp: new Date().toISOString() }
        console.log "⏰ [step4_wait] done"
        resolve()
      , 1000
