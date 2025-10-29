###
  031_register.coffee
  -------------------
  Direct CoffeeScript port of 031_register.py
  ✅ Builds a detailed artifact registry after LoRA training runs.
  ✅ Ready for memo integration and declarative execution in the pipeline.

  Function:
    - Reads experiments.csv
    - Computes SHA256 for adapter and log files
    - Creates symlinks for latest runs
    - Writes artifacts.json
###

fs   = require 'fs'
path = require 'path'
yaml = require 'js-yaml'
crypto = require 'crypto'

# --- STEP-AWARE CONFIG ---
CFG_PATH  = process.env.CFG_OVERRIDE or path.join(process.cwd(), 'experiment.yaml')
STEP_NAME = process.env.STEP_NAME or 'register'
cfgFull   = yaml.load(fs.readFileSync(CFG_PATH, 'utf8'))
STEP_CFG  = cfgFull[STEP_NAME] or {}
RUN_CFG   = cfgFull['run'] or {}

OUT_DIR  = path.resolve(RUN_CFG.output_dir or 'run_out')
DATA_DIR = path.resolve(RUN_CFG.data_dir or 'data')
fs.mkdirSync(OUT_DIR, {recursive:true})
fs.mkdirSync(DATA_DIR, {recursive:true})

EXPERIMENTS_CSV = path.join(DATA_DIR, RUN_CFG.experiments_csv or 'experiments.csv')
ARTIFACTS_JSON  = path.join(DATA_DIR, RUN_CFG.artifacts or 'artifacts.json')

# --- Utilities ---
sha256File = (p) ->
  h = crypto.createHash('sha256')
  f = fs.openSync(p, 'r')
  buf = Buffer.alloc(1024*1024)
  loop
    bytes = fs.readSync(f, buf, 0, buf.length, null)
    break if bytes is 0
    h.update buf.subarray(0, bytes)
  fs.closeSync(f)
  h.digest('hex')

gatherDirFiles = (root) ->
  out = []
  return out unless fs.existsSync(root)
  for relPath of fs.readdirSync(root)
    full = path.join(root, relPath)
    stats = fs.statSync(full)
    if stats.isDirectory()
      sub = gatherDirFiles(full)
      out = out.concat(sub)
    else
      out.push
        path: path.resolve(full)
        rel: path.relative(root, full)
        bytes: stats.size
        sha256: sha256File(full)
        mtime_utc: new Date(stats.mtime).toISOString().replace(/\.\d+Z$/,'Z')
  out

loadRows = (p) ->
  rows = []
  if not fs.existsSync(p)
    console.error "experiments.csv not found (run Step 6)."
    process.exit(1)
  txt = fs.readFileSync(p, 'utf8').split(/\r?\n/)
  hdr = null
  for line in txt when line.trim().length
    cols = line.split(',')
    if not hdr then hdr = cols; continue
    row = {}
    for i in [0...hdr.length]
      row[hdr[i].trim()] = cols[i]?.trim() or ''
    rows.push(row)
  rows

# --- Main ---
rows = loadRows(EXPERIMENTS_CSV)
registry =
  created_utc: new Date().toISOString().replace(/\.\d+Z$/,'Z')
  runs: []

for r in rows
  modelId = r.model_id
  modelTag = modelId.replace(/\//g, '--')
  outRoot = path.join(DATA_DIR, modelTag)
  adapterDir = path.resolve(r.adapter_path)
  logsDir = path.resolve(r.log_dir)

  fusedDir = path.join(outRoot, 'fused', 'model')
  quantDir = path.join(outRoot, 'quantized', 'model')
  fs.mkdirSync(path.dirname(fusedDir), {recursive:true})
  fs.mkdirSync(path.dirname(quantDir), {recursive:true})

  # Symlinks
  try
    latestAdapter = path.join(outRoot, 'latest_adapter')
    if fs.existsSync(latestAdapter) then fs.unlinkSync(latestAdapter)
    fs.symlinkSync(path.basename(adapterDir), latestAdapter)
  catch e
    console.warn "(symlink adapter)", e.message

  try
    latestLogs = path.join(outRoot, 'latest_logs')
    if fs.existsSync(latestLogs) then fs.unlinkSync(latestLogs)
    fs.symlinkSync(path.basename(logsDir), latestLogs)
  catch e
    console.warn "(symlink logs)", e.message

  entry =
    model_id: modelId
    output_root: path.resolve(outRoot)
    adapter_dir: path.resolve(adapterDir)
    logs_dir: path.resolve(logsDir)
    fused_dir: path.resolve(fusedDir)
    quantized_dir: path.resolve(quantDir)
    files:
      adapter: gatherDirFiles(adapterDir)
      logs: gatherDirFiles(logsDir)
    training_params:
      iters: parseInt(r.iters or 0)
      batch_size: parseInt(r.batch_size or 0)
      max_seq_length: parseInt(r.max_seq_length or 0)

  registry.runs.push(entry)

fs.writeFileSync(ARTIFACTS_JSON, JSON.stringify(registry, null, 2), 'utf8')
console.log "[OK] Wrote artifact registry: #{ARTIFACTS_JSON}"

# Optional memo save
try
  if global.M? and typeof global.M.saveThis is 'function'
    global.M.saveThis 'register:artifacts', registry
catch e
  console.warn "(memo skip)", e.message