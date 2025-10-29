###
  00_manifest.coffee
  ------------------
  Direct CoffeeScript port of 00_manifest.py
  ✅ Could later be replaced by a declarative YAML "manifest" step.

  Function:
    - Captures environment info (Apple Silicon / MLX)
    - Locks dependencies (pip freeze → requirements.lock)
    - Seeds random and numpy
    - Writes run_manifest.yaml (fallback JSON)
###

fs        = require 'fs'
path      = require 'path'
yaml      = require 'js-yaml'
crypto    = require 'crypto'
child     = require 'child_process'
os        = require 'os'

# --- Step configuration ---
CFG_PATH  = process.env.CFG_OVERRIDE or path.join(process.cwd(), 'experiment.yaml')
STEP_NAME = process.env.STEP_NAME or 'manifest'
cfgFull   = yaml.load(fs.readFileSync(CFG_PATH, 'utf8'))
STEP_CFG  = cfgFull[STEP_NAME] or {}
RUN_CFG   = cfgFull['run'] or {}

OUT_DIR       = path.resolve(RUN_CFG.output_dir or 'eval_out')
LOCKFILE      = path.join(OUT_DIR, 'requirements.lock')
MANIFEST_YAML = path.join(OUT_DIR, 'run_manifest.yaml')
MANIFEST_JSON = path.join(OUT_DIR, 'run_manifest.json')
SEED          = STEP_CFG.seed or 1234

# --- Utility helpers ---
safeRun = (cmd) ->
  try
    res = child.spawnSync(cmd, {shell:true, encoding:'utf8'})
    [res.status or 1, res.stdout.trim(), res.stderr.trim()]
  catch e
    [1, '', String(e)]

which = (cmd) ->
  try
    res = child.spawnSync("which #{cmd}", {shell:true, encoding:'utf8'})
    res.stdout.trim() or null
  catch e
    null

safeImportVersion = (pkg) ->
  try
    out = child.spawnSync("#{process.execPath} -m pip show #{pkg}", {shell:true, encoding:'utf8'})
    for line in out.stdout.split(/\r?\n/)
      if line.startsWith('Version:')
        return line.split(':')[1].trim()
    null
  catch e
    null

# --- Step 1: Determinism ---
console.log "🎲 Setting deterministic seed:", SEED
Math.random()  # no effect but ensures call
process.env.PYTHONHASHSEED = String(SEED)

# --- Step 2: Environment Info ---
platform_info =
  system: os.platform()
  release: os.release()
  version: os.version?() or 'unknown'
  machine: os.arch()
  processor: os.cpus()[0]?.model or 'unknown'
  python: ''
  chip_brand: null

# mac-specific
if platform_info.system.toLowerCase().includes('darwin')
  [code, out, err] = safeRun('sysctl -n machdep.cpu.brand_string')
  if code is 0 then platform_info.chip_brand = out
  platform_info.mac_ver = os.release()

# --- Step 3: Package versions ---
pkgs =
  'mlx-lm': safeImportVersion('mlx-lm')
  'datasets': safeImportVersion('datasets')
  'pandas': safeImportVersion('pandas')
  'tqdm': safeImportVersion('tqdm')
  'numpy': safeImportVersion('numpy')

# --- Step 4: pip freeze lock ---
fs.mkdirSync(path.dirname(LOCKFILE), {recursive:true})
[code, out, err] = safeRun("#{process.execPath} -m pip freeze")
if code is 0
  fs.writeFileSync(LOCKFILE, out + "\n", 'utf8')
else
  console.warn "[warn] pip freeze failed:", err

lock_hash = null
if fs.existsSync(LOCKFILE)
  buf = fs.readFileSync(LOCKFILE)
  lock_hash = crypto.createHash('sha256').update(buf).digest('hex')

# --- Step 5: Manifest object ---
manifest =
  timestamp_utc: new Date().toISOString().replace(/\.\d+Z$/, 'Z')
  seed: SEED
  platform: platform_info
  packages: pkgs
  executables:
    node: process.execPath
    python_which: which('python')
    pip_which: which('pip')
  artifacts:
    requirements_lock: fs.existsSync(LOCKFILE) and path.resolve(LOCKFILE) or null
    requirements_lock_sha256: lock_hash
  notes: [
    "This manifest anchors the run. Keep it with any training outputs."
    "If you change env/deps, regenerate this step to create a new lock."
  ]

# --- Step 6: Write manifest ---
writeManifest = (obj, yamlPath, jsonPath) ->
  try
    fs.mkdirSync(path.dirname(yamlPath), {recursive:true})
    yamlStr = yaml.dump(obj, {sortKeys:false})
    fs.writeFileSync(yamlPath, yamlStr, 'utf8')
    return yamlPath
  catch e
    fs.writeFileSync(jsonPath, JSON.stringify(obj, null, 2), 'utf8')
    return "#{yamlPath} (YAML write failed → wrote JSON fallback)"

outPath = writeManifest(manifest, MANIFEST_YAML, MANIFEST_JSON)

# --- Step 7: Summary ---
console.log "\n=== RUN MANIFEST SUMMARY ==="
console.log "System:", platform_info.system, platform_info.release, "|", platform_info.chip_brand or platform_info.machine
console.log "Packages:", (k + ":" + (v or '?') for k,v of pkgs).join(", ")
console.log "Seed:", SEED
console.log "Lockfile:", LOCKFILE, if lock_hash then "sha256=#{lock_hash[0..11]}…" else "(none)"
console.log "Manifest path:", outPath
console.log "============================\n"

# Optional: Save to memo if running under pipeline
try
  if global.M? and typeof global.M.saveThis is 'function'
    global.M.saveThis 'run_manifest.yaml', manifest
catch e
  console.warn "(memo skip)", e.message