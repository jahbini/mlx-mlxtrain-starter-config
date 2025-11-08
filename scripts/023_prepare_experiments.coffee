#!/usr/bin/env coffee
###
022_prepare_experiments.coffee — memo-native (single-model)
- Reads contract/catalog/report from memo keys declared in run.*
- Builds experiments.csv compatible with 03_train.coffee
- Uses run.model (single model id), not a list
- Keeps iters floor = 10000 to match existing behavior
###

path  = require 'path'
fs    = require 'fs'

@step =
  desc: "Materialize experiments.csv for MLX LoRA training (single-model)"

  action: (M, stepName) ->

    throw new Error "Missing stepName" unless stepName?
    cfg = M.theLowdown('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    run = cfg.run
    throw new Error "Missing run section" unless run?

    stepCfg = cfg[stepName]
    throw new Error "Missing step config for #{stepName}" unless stepCfg?

    # Required run keys
    for k in ['contract','catalog','report','output_dir','experiments_csv','model']
      throw new Error "Missing run.#{k}" unless run[k]?

    CONTRACT_KEY = run.contract
    CATALOG_KEY  = run.catalog
    REPORT_KEY   = run.report
    OUT_DIR      = path.resolve(run.output_dir)
    EXP_CSV_PATH = path.resolve(run.experiments_csv)
    MODEL_ID     = run.model

    contract = M.theLowdown(CONTRACT_KEY)?.value
    throw new Error "Missing contract in memo: #{CONTRACT_KEY}" unless contract?

    catalog  = M.theLowdown(CATALOG_KEY)?.value
    report   = M.theLowdown(REPORT_KEY)?.value
    throw new Error "Missing data_report.json in memo: #{REPORT_KEY}" unless report?

    # Resolve file paths from contract
    files = {}
    for split, info of contract.filenames when info?.resolved?
      files[split] = info.resolved
    if files.valid? then files.validation = files.valid
    if files.val?   then files.validation = files.val

    dataDir = path.resolve(contract.data_dir ? path.dirname(files.train ? OUT_DIR))

    # Counts: prefer catalog if available; otherwise use report
    trainCount = null
    validCount = null

    if catalog?.entries?.train?.stats?.num_valid_examples?
      trainCount = parseInt(catalog.entries.train.stats.num_valid_examples)
      ventry = catalog.entries.valid ? catalog.entries.val
      validCount = ventry?.stats?.num_valid_examples ? 0
      validCount = parseInt(validCount)
    else
      # fallback to report
      rtrain = report?.splits?.train?.valid_examples
      throw new Error "No train count in report" unless Number.isFinite(rtrain)
      trainCount = parseInt(rtrain)
      vrep = report?.splits?.valid ? report?.splits?.val
      validCount = vrep?.valid_examples ? 0
      validCount = parseInt(validCount)

    # Required step keys (no defaults)
    for k in ['epochs','batch_size','grad_accum','max_seq_length','learning_rate','bf16','iters_override']
      throw new Error "Missing #{k} in step '#{stepName}'" unless stepCfg[k]?

    EPOCHS         = parseInt(stepCfg.epochs)
    BATCH_SIZE     = parseInt(stepCfg.batch_size)
    GRAD_ACCUM     = parseInt(stepCfg.grad_accum)
    MAX_SEQ_LENGTH = parseInt(stepCfg.max_seq_length)
    LEARNING_RATE  = parseFloat(stepCfg.learning_rate)
    BF16           = if String(stepCfg.bf16) in ['1','true','True'] then 1 else 0
    ITERS_OVERRIDE = parseInt(stepCfg.iters_override)

    estIters = ->
      steps = Math.ceil( (EPOCHS * Math.max(1, trainCount)) / Math.max(1, BATCH_SIZE * GRAD_ACCUM) )
      Math.max(10000, steps)

    iters = if ITERS_OVERRIDE and ITERS_OVERRIDE > 0 then ITERS_OVERRIDE else estIters()
    estTokens = MAX_SEQ_LENGTH * BATCH_SIZE * GRAD_ACCUM * iters

    modelTag = MODEL_ID.replace(/\//g, '--')
    adapterPath = path.join(OUT_DIR, modelTag, 'adapter')
    logsDir     = path.join(OUT_DIR, modelTag, 'logs')

    row =
      created_utc: new Date().toISOString().replace(/\.\d+Z$/, 'Z')
      model_id: MODEL_ID
      data_dir: dataDir
      train_file: files.train ? ''
      valid_file: files.validation ? ''
      train_examples: trainCount
      valid_examples: validCount
      epochs: EPOCHS
      iters: iters
      batch_size: BATCH_SIZE
      grad_accum: GRAD_ACCUM
      max_seq_length: MAX_SEQ_LENGTH
      learning_rate: LEARNING_RATE
      bf16: BF16
      adapter_path: adapterPath
      log_dir: logsDir
      est_tokens: estTokens

    # Write CSV to disk and memo for compatibility/debug
    headers = Object.keys(row)
    csv = headers.join(',') + '\n' + (headers.map((k)-> String(row[k])).join(',')) + '\n'
    fs.mkdirSync(path.dirname(EXP_CSV_PATH), {recursive:true})
    fs.writeFileSync(EXP_CSV_PATH, csv, 'utf8')

    M.saveThis run.experiments_csv, csv
    M.saveThis "prepare_experiments:last_row", row
    M.saveThis "done:#{stepName}", true

    console.log "experiments.csv →", EXP_CSV_PATH
    return
