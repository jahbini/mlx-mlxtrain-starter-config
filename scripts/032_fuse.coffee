#!/usr/bin/env coffee
###
STEP 9 — Fuse & Quantize (final, clean/idempotent)

- Reuses experiments.csv + artifacts.json from Steps 6–8
- If needed, fuses adapter -> fused/model  (mlx_lm fuse)
- Quantizes fused -> quantized/ (mlx_lm convert with explicit flags)
- Removes any pre-existing quantized dir to avoid MLX "already exists" error
- Updates artifacts.json

All configuration is read from cfg.quantize.* (no hardcoded constants).
###

fs    = require 'fs'
path  = require 'path'
crypto = require 'crypto'
shlex  = require 'shell-quote'
child  = require 'child_process'

{load_config} = require './config_loader'

# --- Step-aware config ---
CFG       = load_config()
STEP_NAME = process.env.STEP_NAME or "032_fuse"
STEP_CFG  = CFG.pipeline.steps[STEP_NAME]

RUN_DIR   = path.resolve CFG.run.output_dir
ARTIFACTS = path.join RUN_DIR, CFG.data.artifacts

# ---- Controls (from config) ----
DO_FUSE = Boolean CFG.quantize?.do_fuse
Q_BITS  = parseInt(CFG.quantize?.bits or 4)
Q_GROUP = parseInt(CFG.quantize?.group_size or 64)
DTYPE   = CFG.quantize?.dtype or 'float16'
DRY_RUN = Boolean CFG.quantize?.dry_run
# --------------------------------

# --- Helpers ---
runCmd = (cmd) ->
  console.log "[MLX]", cmd
  if DRY_RUN
    console.log "DRY_RUN=True -> not executing."
    return 0
  try
    child.execSync cmd, stdio: 'inherit'
    return 0
  catch err
    return err.status or 1

sha256File = (p) ->
  h = crypto.createHash 'sha256'
  buf = fs.readFileSync p
  h.update buf
  h.digest 'hex'

listFiles = (root) ->
  out = []
  return out unless fs.existsSync root
  walk = (dir) ->
    for f in fs.readdirSync dir
      fp = path.join dir, f
      stat = fs.statSync fp
      if stat.isDirectory()
        walk fp
      else
        out.push
          path: path.resolve fp
          rel: path.relative root, fp
          bytes: stat.size
          sha256: sha256File fp
          mtime_utc: new Date(stat.mtime).toISOString().replace(/\.\d+Z$/, 'Z')
  walk root
  out

# --- Main ---
unless fs.existsSync ARTIFACTS
  console.error "[FATAL] artifacts.json not found. Run Steps 6/7/8 first."
  process.exit 1

registry = JSON.parse fs.readFileSync ARTIFACTS, 'utf8'
runs = registry.runs or []
unless runs.length
  console.error "[FATAL] No runs found in artifacts.json."
  process.exit 1

py = shlex.quote process.execPath
updated = false

for entry in runs
  modelId    = entry.model_id
  outputDir  = entry.output_root
  adapterDir = if entry.adapter_dir? then entry.adapter_dir else null
  fusedDir   = entry.fused_dir or path.join outputDir, 'fused', 'model'

  # 1) Fuse (optional / idempotent)
  if DO_FUSE and not fs.existsSync fusedDir
    fs.mkdirSync path.dirname(fusedDir), recursive: true
    if adapterDir? and fs.existsSync adapterDir
      cmdFuse = "#{py} -m mlx_lm.fuse --model #{shlex.quote modelId} --adapter-path #{shlex.quote adapterDir} --save-path #{shlex.quote fusedDir}"
    else
      cmdFuse = "#{py} -m mlx_lm.fuse --model #{shlex.quote modelId} --save-path #{shlex.quote fusedDir}"

    console.log "\n=== FUSE ==="
    rc = runCmd cmdFuse
    if rc isnt 0
      console.error "❌ Fuse failed for #{modelId}"
      continue
    entry.fused_dir = path.resolve fusedDir
    entry.files ?= {}
    entry.files.fused = listFiles fusedDir
    updated = true
  else if fs.existsSync fusedDir
    entry.fused_dir = path.resolve fusedDir
    entry.files ?= {}
    entry.files.fused = listFiles fusedDir

  unless fs.existsSync fusedDir
    console.log "Skipping quantize for #{modelId}: fused_dir missing."
    continue

  # 2) Quantize (idempotent + clean)
  qDir = path.join outputDir, 'quantized'
  if fs.existsSync qDir
    if DRY_RUN
      console.log "[INFO] Would remove pre-existing quantized dir: #{qDir}"
    else
      console.log "[INFO] Removing pre-existing quantized dir: #{qDir}"
      fs.rmSync qDir, recursive: true, force: true

  cmdQ = """
    #{py} -m mlx_lm.convert \
    --hf-path #{shlex.quote fusedDir} \
    --mlx-path #{shlex.quote qDir} \
    --q-bits #{Q_BITS} \
    --q-group-size #{Q_GROUP} \
    --dtype #{shlex.quote DTYPE} \
    -q
  """.trim()

  console.log "\n=== QUANTIZE ==="
  rc = runCmd cmdQ
  if rc isnt 0
    console.error "❌ Quantize failed for #{modelId}"
    continue

  entry.quantized_dir = path.resolve qDir
  entry.quantize_config =
    bits: Q_BITS
    group_size: Q_GROUP
    dtype: DTYPE
    timestamp_utc: new Date().toISOString().replace(/\.\d+Z$/, 'Z')
  entry.files ?= {}
  entry.files.quantized = listFiles qDir
  updated = true

# Save updated artifacts
if updated
  registry.updated_utc = new Date().toISOString().replace(/\.\d+Z$/, 'Z')
  fs.writeFileSync ARTIFACTS, JSON.stringify(registry, null, 2), 'utf8'

# Summary
console.log "\n=== FUSE/QUANTIZE SUMMARY ==="
console.log "Wrote:", ARTIFACTS
for entry in registry.runs or []
  console.log "- #{entry.model_id}"
  if entry.fused_dir?
    console.log "   fused_dir:    #{entry.fused_dir} (#{(entry.files?.fused?.length) or 0} files)"
  if entry.quantized_dir?
    qc = entry.quantize_config or {}
    console.log "   quantized_dir: #{entry.quantized_dir} (q#{qc.bits}, group=#{qc.group_size}, dtype=#{qc.dtype}) files=#{(entry.files?.quantized?.length) or 0}"
