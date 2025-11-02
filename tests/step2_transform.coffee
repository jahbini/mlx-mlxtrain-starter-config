#!/usr/bin/env coffee
###
Step 2 — transform: read input.json and write derived output
###
@step =
  name: 'step2_transform'
  action: (M) ->
    input = M.theLowdown("data/input.json").value
    unless input?
      throw new Error "Missing input.json"
    transformed =
      greeting: "#{input.greeting}, world!"
      doubled: input.value * 2
    M.saveThis "data/transformed.json", transformed
    console.log "✨ [step2_transform] doubled value =", transformed.doubled
