#!/usr/bin/env coffee
###
Step 5 — finalize: aggregate results
###
@step =
  name: 'step5_finalize'
  action: (M) ->
    input  = M.theLowdown("data/input.json").value
    trans  = M.theLowdown("data/transformed.json").value
    waited = M.theLowdown("state/wait.json").value
    summary =
      original: input?.value
      doubled:  trans?.doubled
      waited:   waited?.done
      timestamp: new Date().toISOString()
    M.saveThis "results/final_summary.json", summary
    console.log "🏁 [step5_finalize] wrote results/final_summary.json"
