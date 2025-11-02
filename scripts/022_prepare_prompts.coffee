#!/usr/bin/env coffee
###
022_prepare_prompts.coffee — strict memo-aware version (2025)
-------------------------------------------------------------
STEP — Prepare prompt-formatting policy for fine-tuning data.

• Executes inside unified pipeline (shared @memo)
• Receives (M, stepName) directly — no env or defaults
• Reads dataset contract, samples examples, applies formatter
• Writes prompt_policy.json
###

fs = require 'fs'
path = require 'path'

@step =
  desc: "Generate prompt-formatting policy JSON from dataset contract"

  action: (M, stepName) ->

    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    stepCfg = cfg[stepName]
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?
    runCfg = cfg['run']
    throw new Error "Missing global 'run' section in experiment.yaml" unless runCfg?

    # --- Required keys ---
    for k in ['data_dir','contract','policy']
      throw new Error "Missing required run.#{k}" unless k of runCfg

    for k in ['template_name','stop_strings','use_eos_token']
      throw new Error "Missing required param '#{k}' in step '#{stepName}'" unless k of stepCfg

    DATA_DIR  = path.resolve(runCfg.data_dir)
    CONTRACT  = path.join(DATA_DIR, runCfg.contract)
    POLICY    = path.join(DATA_DIR, runCfg.prompt_policy)

    TEMPLATE_NAME = stepCfg.template_name
    STOP_STRINGS  = stepCfg.stop_strings
    USE_EOS_TOKEN = stepCfg.use_eos_token

    readJSON = (p) -> JSON.parse(fs.readFileSync(p, 'utf8'))

    loadContract = (p) ->
      c = readJSON(p)
      dataDir = path.resolve(c.data_dir)
      files = {}
      for k,v of c.filenames when v.resolved?
        files[k] = v.resolved
      fields = c.schema?.fields or {}
      textField = Object.keys(fields).find((k)-> String(fields[k]).toLowerCase() is 'string') or 'text'
      {dataDir, files, textField}

    readFirstNTexts = (p, n=3, field='text') ->
      out = []
      lines = fs.readFileSync(p, 'utf8').split(/\r?\n/)
      for line in lines
        break if out.length >= n
        try
          obj = JSON.parse(line)
        catch e
          continue
        val = obj[field]
        if typeof val is 'string'
          out.push(val)
      out

    # --- Load contract + samples ---
    {dataDir, files, textField} = loadContract(CONTRACT)
    trainPath = files.train or path.join(dataDir, 'train.jsonl')
    samples = readFirstNTexts(trainPath, 3, textField)

    # --- Formatters ---
    fmtPlain = (text) -> text

    fmtIclMinimal = (text) ->
      "### Instruction\nShare an important thought.\n\n### Response\n" + text.trim()

    fmtLlama3Style = (text) ->
      "<s>[INSTRUCTION]\nShare an important thought.\n[/INSTRUCTION]\n[RESPONSE]\n" + text.trim() + "\n[/RESPONSE]</s>"

    FORMATTERS =
      plain_text_passthrough: fmtPlain
      icl_minimal: fmtIclMinimal
      llama3_style: fmtLlama3Style

    unless FORMATTERS[TEMPLATE_NAME]?
      throw new Error "Unknown TEMPLATE_NAME: #{TEMPLATE_NAME}"

    formatter = FORMATTERS[TEMPLATE_NAME]

    console.log "=== FORMAT PREVIEW ==="
    console.log "Template: #{TEMPLATE_NAME}"
    for i in [0...samples.length]
      txt = samples[i]
      before = txt.replace(/\n/g, ' \\n ')
      after  = formatter(txt).replace(/\n/g, ' \\n ')
      console.log "\n--- Example #{i+1}: BEFORE ---"
      console.log before.slice(0,220) + (if before.length>220 then '…' else '')
      console.log "--- Example #{i+1}: AFTER  ---"
      console.log after.slice(0,220) + (if after.length>220 then '…' else '')

    # --- Persist policy ---
    policy =
      template_name: TEMPLATE_NAME
      text_field: textField
      stop_strings: STOP_STRINGS
      use_eos_token: USE_EOS_TOKEN
      notes: [
        "This policy defines how to format examples when generating or materializing new data.",
        "It does not modify existing JSONL files; it informs downstream processing."
      ]
      preview: for i in [0...Math.min(2, samples.length)]
        before: samples[i]
        after: formatter(samples[i])

    fs.writeFileSync(POLICY, JSON.stringify(policy, null, 2), 'utf8')
    console.log "\n📘 Wrote #{POLICY}"

    M.saveThis "prepare_prompts:policy", policy
    M.saveThis "done:#{stepName}", true
    return