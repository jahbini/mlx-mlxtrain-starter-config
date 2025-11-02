#!/usr/bin/env coffee
###
03_train.coffee — strict memo-aware version (2025)
---------------------------------------------------
• Executes inside unified pipeline (shared @memo)
• Receives (M, stepName) directly — no env lookup
• Reads experiments.csv, runs MLX LoRA trainings
• Aborts on missing config keys, supports DRY_RUN
###

fs      = require 'fs'
path    = require 'path'
child   = require 'child_process'
shlex   = require 'shell-quote'

@step =
  desc: "Run MLX LoRA training runs as defined in experiments.csv"

  action: (M, stepName) ->

    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    stepCfg = cfg[stepName]
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?
    runCfg = cfg['run']
    throw new Error "Missing global 'run' section in experiment.yaml" unless runCfg?

    # --- Required keys ---
    for k in ['data_dir','experiments_csv']
      throw new Error "Missing required run.#{k}" unless k of runCfg

    requiredStep = ['dry_run','only_model_id','only_row','steps_per_report','steps_per_eval','val_batches']
    for k in requiredStep
      throw new Error "Missing required param '#{k}' in step '#{stepName}'" unless k of stepCfg

    OUT_DIR = path.resolve(runCfg.data_dir)
    EXPERIMENTS_CSV = path.join(OUT_DIR, runCfg.experiments_csv)

    fs.mkdirSync(OUT_DIR, {recursive:true})

    DRY_RUN          = stepCfg.dry_run
    ONLY_MODEL_ID    = stepCfg.only_model_id
    ONLY_ROW         = stepCfg.only_row
    STEPS_PER_REPORT = stepCfg.steps_per_report
    STEPS_PER_EVAL   = stepCfg.steps_per_eval
    VAL_BATCHES      = stepCfg.val_batches

    readCSV = (p) ->
      text = fs.readFileSync(p, 'utf8')
      lines = text.split(/\r?\n/).filter (l)-> l.trim().length
      return [] unless lines.length
      headers = lines[0].split(',').map (h)-> h.trim()
      rows = []
      for line in lines.slice(1)
        cols = line.split(',').map (c)-> c.trim()
        row = {}
        for i in [0...headers.length]
          row[headers[i]] = cols[i] ? ''
        # numeric coercions
        for k in ['epochs','iters','batch_size','grad_accum','max_seq_length','bf16']
          if row[k]? and row[k] isnt ''
            row[k] = parseInt(parseFloat(row[k]))
        for k in ['learning_rate']
          if row[k]? and row[k] isnt ''
            row[k] = parseFloat(row[k])
        rows.push row
      rows

    selectRows = (rows, onlyModel, onlyRowIdx) ->
      if onlyRowIdx? and onlyRowIdx isnt 'None'
        idx = parseInt(onlyRowIdx)
        return if rows[idx]? then [rows[idx]] else []
      if onlyModel
        return rows.filter (r)-> r.model_id is onlyModel
      rows

    ensureDirs = (row) ->
      fs.mkdirSync(path.resolve(row.adapter_path), {recursive:true})
      fs.mkdirSync(path.resolve(row.log_dir), {recursive:true})

    buildCmd = (row) ->
      py = shlex.quote(process.env.PYTHON_EXECUTABLE or 'python')
      model = shlex.quote(row.model_id)
      data_dir = shlex.quote(row.data_dir)
      iters = parseInt(row.iters)
      bs = parseInt(row.batch_size)
      maxlen = parseInt(row.max_seq_length)
      lr = parseFloat(row.learning_rate)
      adapter = shlex.quote(row.adapter_path)

      parts = [
        "#{py} -m mlx_lm lora"
        "--model #{model}"
        "--data #{data_dir}"
        "--train"
        "--fine-tune-type lora"
        "--batch-size #{bs}"
        "--iters #{iters}"
        "--learning-rate #{lr}"
        "--max-seq-length #{maxlen}"
        "--adapter-path #{adapter}"
        "--num-layers -1"
      ]

      if VAL_BATCHES then parts.push "--val-batches #{parseInt(VAL_BATCHES)}"
      if STEPS_PER_REPORT then parts.push "--steps-per-report #{parseInt(STEPS_PER_REPORT)}"
      if STEPS_PER_EVAL then parts.push "--steps-per-eval #{parseInt(STEPS_PER_EVAL)}"

      parts.join(' ')

    runCmd = (cmd, logPath="run/lora_last.log") ->
      console.log "\n[MLX train]", cmd
      if DRY_RUN
        console.log "DRY_RUN=True → not executing."
        return 0

      fs.mkdirSync(path.dirname(logPath), {recursive:true})
      proc = child.spawnSync(cmd, {shell:true, encoding:'utf8'})
      fs.writeFileSync(logPath, proc.stdout or '', 'utf8')

      if proc.status isnt 0
        console.error "❌ Training failed. See log:", logPath
      else
        console.log "✅ Training completed. Log:", logPath
      proc.status

    rows = readCSV(EXPERIMENTS_CSV)
    todo = selectRows(rows, ONLY_MODEL_ID, ONLY_ROW)

    console.log "Found #{rows.length} rows; running #{todo.length} row(s). DRY_RUN=#{DRY_RUN}"

    for i in [0...todo.length]
      row = todo[i]
      console.log "\n=== RUN #{i+1}/#{todo.length} ==="
      ensureDirs(row)
      rc = runCmd(buildCmd(row), path.join(row.log_dir or 'logs', 'lora_last.log'))
      if rc isnt 0
        console.error "❌ Training failed with returncode=#{rc}"
        break
      console.log "✅ Training launched."

    M.saveThis "train:last_row", todo[todo.length-1]
    M.saveThis "train:status", "done"
    console.log "📗 Recorded training completion in memo."
    return