###
  032_fuse.coffee
  ----------------
  Fuse and Quantize Models (MLX pipeline)
  ✅ Fully deterministic, idempotent
  ✅ Logs to <data_dir>/logs/032_fuse.log
  ✅ Updates artifacts.json with fused and quantized directories
###

fs        = require 'fs'
path      = require 'path'
yaml      = require 'js-yaml'
crypto    = require 'crypto'
shlex     = require 'shell-quote'
child     = require 'child_process'
shutil    = require 'fs-extra'

# --- STEP-AWARE CONFIG ---
CFG_PATH  = process.env.CFG_OVERRIDE or path.join(process.cwd(), 'experiment.yaml')
STEP_NAME = process.env.STEP_NAME or 'fuse'
cfgFull   = yaml.load(fs.readFileSync(CFG_PATH, 'utf8'))
STEP_CFG  = cfgFull[STEP_NAME] or {}
RUN_CFG   = cfgFull['run'] or {}

RUN_DIR   = path.resolve(RUN_CFG.output_dir or 'run')
DATA_DIR  = path.resolve(RUN_CFG.data_dir or 'data')
ARTIFACTS = path.join(DATA_DIR, RUN_CFG.artifacts or 'artifacts.json')
LOG_DIR   = path.join(DATA_DIR, 'logs')
fs.mkdirSync(LOG_DIR, {recursive:true})
LOG_PATH  = path.join(LOG_DIR, "#{STEP_NAME}.log")

# --- Logging ---
log = (msg) ->
  stamp = new Date().toISOString().replace('T',' ').replace(/\.\d+Z$/,'')
  line  = "[#{stamp}] #{msg}"
  fs.appendFileSync(LOG_PATH, line + "\n")
  console.log(line)

# --- Controls ---
DO_FUSE = !!STEP_CFG.do_fuse
Q_BITS  = parseInt(STEP_CFG.q_bits or 4)
Q_GROUP = parseInt(STEP_CFG.q_group or 32)
DTYPE   = STEP_CFG.dtype or 'float16'
DRY_RUN = !!STEP_CFG.dry_run

# --- Utilities ---
sha256File = (p) ->
  h = crypto.createHash('sha256')
  fd = fs.openSync(p, 'r')
  buf = Buffer.alloc(1024*1024)
  loop
    bytes = fs.readSync(fd, buf, 0, buf.length, null)
    break if bytes is 0
    h.update(buf.subarray(0, bytes))
  fs.closeSync(fd)
  h.digest('hex')

listFiles = (root) ->
  out = []
  return out unless fs.existsSync(root)
  for file of fs.readdirSync(root)
    full = path.join(root, file)
    stats = fs.statSync(full)
    if stats.isDirectory()
      out = out.concat(listFiles(full))
    else
      out.push
        path: path.resolve(full)
        rel: path.relative(root, full)
        bytes: stats.size
        sha256: sha256File(full)
        mtime_utc: new Date(stats.mtime).toISOString().replace(/\.\d+Z$/,'Z')
  out

runCmd = (cmd) ->
  log "[MLX] #{cmd}"
  if DRY_RUN
    log "DRY_RUN=True → not executing."
    return 0
  try
    child.execSync(cmd, {stdio:'inherit'})
    return 0
  catch e
    return e.status or 1

# --- Load artifacts registry ---
unless fs.existsSync(ARTIFACTS)
  console.error "artifacts.json not found. Run training first."
  process.exit(1)

registry = JSON.parse(fs.readFileSync(ARTIFACTS, 'utf8'))
runs = registry.runs or []
if runs.length is 0
  console.error "No runs found in artifacts.json."
  process.exit(1)

updated = false
py = shlex.quote(process.argv[0])

# --- Main Loop ---
for entry in runs
  modelId = entry.model_id
  outputDir  = path.resolve(entry.output_root)
  adapterDir = path.resolve(entry.adapter_dir)
  fusedDir   = entry.fused_dir or path.join(outputDir, 'fused', 'model')

  # 1) FUSE ----------------------------------------------------------
  if DO_FUSE and not fs.existsSync(fusedDir)
    fs.mkdirSync(path.dirname(fusedDir), {recursive:true})
    cmdFuse = "#{py} -m mlx_lm fuse --model #{shlex.quote(modelId)} " +
              "--adapter-path #{shlex.quote(adapterDir)} " +
              "--save-path #{shlex.quote(fusedDir)}"
    log "=== FUSE ==="
    rc = runCmd(cmdFuse)
    if rc isnt 0
      log "❌ Fuse failed for #{modelId}"
      continue
    entry.fused_dir = path.resolve(fusedDir)
    entry.files ?= {}
    entry.files.fused = listFiles(fusedDir)
    updated = true
  else if fs.existsSync(fusedDir)
    entry.fused_dir = path.resolve(fusedDir)
    entry.files ?= {}
    entry.files.fused = listFiles(fusedDir)

  unless fs.existsSync(fusedDir)
    log "Skipping quantize for #{modelId}: fused_dir missing."
    continue

  # 2) QUANTIZE ------------------------------------------------------
  qDir = path.join(outputDir, 'quantized')
  if fs.existsSync(qDir)
    log "Removing pre-existing quantized dir: #{qDir}"
    shutil.removeSync(qDir)

  cmdQ = "#{py} -m mlx_lm convert --hf-path #{shlex.quote(fusedDir)} " +
         "--mlx-path #{shlex.quote(qDir)} " +
         "--q-bits #{Q_BITS} --q-group-size #{Q_GROUP} " +
         "--dtype #{shlex.quote(DTYPE)} -q"
  log "=== QUANTIZE ==="
  rc = runCmd(cmdQ)
  if rc isnt 0
    log "❌ Quantize failed for #{modelId}"
    continue

  entry.quantized_dir = path.resolve(qDir)
  entry.quantize_bits = Q_BITS
  entry.q_group_size  = Q_GROUP
  entry.files ?= {}
  entry.files.quantized = listFiles(qDir)
  updated = true

# --- Save Updated Artifacts ---
if updated
  registry.updated_utc = new Date().toISOString().replace(/\.\d+Z$/,'Z')
  fs.writeFileSync(ARTIFACTS, JSON.stringify(registry, null, 2), 'utf8')

log "=== FUSE/QUANTIZE SUMMARY ==="
log "Wrote: #{ARTIFACTS}"
for entry in registry.runs or []
  log "- #{entry.model_id}"
  if entry.fused_dir?
    log "   fused_dir: #{entry.fused_dir} (#{(entry.files?.fused?.length) or 0} files)"
  if entry.quantized_dir?
    log "   quantized_dir: #{entry.quantized_dir} (q#{entry.quantize_bits}, group=#{entry.q_group_size}) " +
        "files=#{(entry.files?.quantized?.length) or 0}"

# Optional memo save
try
  if global.M? and typeof global.M.saveThis is 'function'
    global.M.saveThis 'fuse:artifacts', registry
catch e
  console.warn "(memo skip)", e.message