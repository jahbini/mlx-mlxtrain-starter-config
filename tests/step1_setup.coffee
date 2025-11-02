#!/usr/bin/env coffee
###
Step 1 — setup: create dummy input data
###
@step =
  name: 'step1_setup'
  action: (M) ->
    console.log "🔧 [step1_setup] creating inputs..."
    params = M.theLowdown("params/step1_setup.json").value or {}
    data =
      greeting: params?.greeting ? "Hello"
      value: Math.floor(Math.random() * 100)
    M.saveThis "data/input.json", data
    console.log "✅ [step1_setup] wrote data/input.json"
