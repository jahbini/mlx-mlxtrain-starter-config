#!/usr/bin/env coffee
###
Step 3 — table: generate CSV summary
###
@step =
  name: 'step3_table'
  action: (M) ->
    t = M.theLowdown("data/transformed.json").value
    unless t?
      throw new Error "Missing transformed.json"
    rows = [
      { key: "greeting", val: t.greeting }
      { key: "doubled",  val: t.doubled }
    ]
    M.saveThis "reports/summary.csv", rows
    console.log "📊 [step3_table] wrote reports/summary.csv"
