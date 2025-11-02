#!/usr/bin/env coffee
###
042_prepare_outmd_kag.coffee — strict memo-aware version (2025)
----------------------------------------------------------------
STEP — Convert Markdown stories into KAG fine-tuning JSONL

• Executes inside unified pipeline (shared @memo)
• Receives (M, stepName) directly — no env vars
• Reads Markdown stories separated by "# " headers
• Writes run/data/out_kag.jsonl
###

fs   = require 'fs'
path = require 'path'
os   = require 'os'

@step =
  desc: "Convert Markdown stories into KAG-style JSONL entries"

  action: (M, stepName) ->

    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    stepCfg = cfg[stepName]
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?
    runCfg = cfg['run']
    throw new Error "Missing global 'run' section in experiment.yaml" unless runCfg?

    # --- Required keys ---
    for k in ['data_dir']
      throw new Error "Missing required run.#{k}" unless k of runCfg

    for k in ['input_md','output_jsonl']
      throw new Error "Missing required param '#{k}' in step '#{stepName}'" unless k of stepCfg

    DATA_DIR  = path.resolve(runCfg.data_dir)
    INPUT_MD  = path.resolve(stepCfg.input_md)
    OUTPUT_JSONL = path.join(DATA_DIR, stepCfg.output_jsonl)

    fs.mkdirSync(DATA_DIR, {recursive:true})

    log = (msg) ->
      stamp = new Date().toISOString().replace('T',' ').replace('Z','')
      line  = "[#{stamp}] #{msg}"
      console.log line
      try M.logThis?(stepName, line) catch e then null

    PROMPT_TEMPLATE = [
      "You are St. John's Jim — a myth-weaving, bar-stool Buddha of the Pacific Northwest.",
      "Tell a new short story in your own voice, using this idea as inspiration:\n"
    ].join '\n'

    extract_snippets = (mdText) ->
      chunks = mdText.split('# ')
      chunks = (c.trim() for c in chunks when c.trim().length > 0)
      chunks

    make_entry = (chunk) ->
      prompt = PROMPT_TEMPLATE + chunk.slice(0, 200) + '...'
      response = chunk.trim()
      {prompt, response}

    log "Starting step: #{stepName}"
    log "Input markdown: #{INPUT_MD}"
    log "Output JSONL:   #{OUTPUT_JSONL}"

    unless fs.existsSync(INPUT_MD)
      throw new Error "Missing input markdown: #{INPUT_MD}"

    mdText = fs.readFileSync(INPUT_MD, 'utf8')
    chunks = extract_snippets(mdText)
    entries = (make_entry c for c in chunks)

    fout = fs.createWriteStream(OUTPUT_JSONL, {encoding:'utf8'})
    for e in entries
      fout.write JSON.stringify(e) + '\n'
    fout.end()

    log "[OK] Wrote #{entries.length} entries to #{OUTPUT_JSONL}"
    M.saveThis "out_kag:entries", entries
    M.saveThis "done:#{stepName}", true
    log "Completed successfully."
    return