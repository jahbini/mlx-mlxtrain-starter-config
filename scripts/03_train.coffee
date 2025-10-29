###
  03_train.coffee
  ----------------
  Direct CoffeeScript port of 03_train.py
  ✅ Could later delegate its MLX execution to @mlx_runner inside the pipeline.

  Function:
    - Reads experiments.csv for LoRA runs
    - Builds MLX LoRA training commands
    - Executes each, logs to file
    - Supports DRY_RUN, filtering by model or row index
###

fs        = require 'fs'
path      = require 'path'
yaml      = require 'js-yaml'
child     = require 'child_process'
shlex     = require 'shell-quote'

# --- Config ---
CFG_PATH  = process.env.CFG_OVERRIDE or path.join(process.cwd(), 'experiment.yaml')
STEP_NAME = process.env.STEP_NAME or 'train'
cfgFull   = yaml.load(fs.readFileSync(CFG_PATH, 'utf8'))
STEP_CFG  = cfgFull[STEP_NAME] or {}
RUN_CFG   = cfgFull['run'] or {}

OUT_DIR   = path.resolve(RUN_CFG.data_dir or 'data')
fs.mkdirSync(OUT_DIR, {recursive:true})
EXPERIMENTS_CSV = path.join(OUT_DIR, RUN_CFG.experiments_csv or 'experiments.csv')

DRY_RUN          = STEP_CFG.dry_run or false
ONLY_MODEL_ID    = STEP_CFG.only_model_id or ''
ONLY_ROW         = STEP_CFG.only_row or 'None'
STEPS_PER_REPORT = STEP_CFG.steps_per_report or 0
STEPS_PER_EVAL   = STEP_CFG.steps_per_eval or 0
VAL_BATCHES      = STEP_CFG.val_batches or 0

# --- Helpers ---
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
  logFile = fs.openSync(logPath, 'w')

  proc = child.spawnSync(cmd, {shell:true, encoding:'utf8'})
  fs.writeFileSync(logPath, proc.stdout or '', 'utf8')

  if proc.status isnt 0
    console.error "❌ Training failed. See log:", logPath
  else
    console.log "✅ Training completed. Log:", logPath
  proc.status

# --- Main ---
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

# Optional memo save
try
  if global.M? and typeof global.M.saveThis is 'function'
    global.M.saveThis "train:last_row", todo[todo.length-1]
    global.M.saveThis "train:status", "done"
catch e
  console.warn "(memo skip)", e.message