#!/usr/bin/env coffee
###
00_manifest.coffee — Pipeline-native
------------------------------------
Captures environment info (Apple Silicon / MLX),
locks dependencies (pip freeze → requirements.lock),
and writes run_manifest.yaml / run_manifest.json.

All configuration comes from @memo['experiment.yaml'].
Never uses process.env.  Expects the runner to call
step.action(M, stepName).
###

fs      = require 'fs'
path    = require 'path'
yaml    = require 'js-yaml'
crypto  = require 'crypto'
child   = require 'child_process'
os      = require 'os'

@step =
  desc: "Capture environment info and create manifest"

  action: (M, stepName) ->

    exp = M.theLowdown('experiment.yaml').value
    run = exp?.run or {}
    stepCfg = exp?[stepName]

    unless run?
      throw new Error "Missing 'run' section in experiment.yaml"

    OUT_DIR       = path.resolve(run.output_dir)
    LOCKFILE      = path.join(OUT_DIR, 'requirements.lock')
    MANIFEST_YAML = path.join(OUT_DIR, 'run_manifest.yaml')
    MANIFEST_JSON = path.join(OUT_DIR, 'run_manifest.json')
    SEED          = stepCfg?.seed

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
    process.env.PYTHONHASHSEED = String(SEED)
    Math.random()  # ensure call

    # --- Step 2: Environment Info ---
    platform_info =
      system: os.platform()
      release: os.release()
      version: os.version?() or 'unknown'
      machine: os.arch()
      processor: os.cpus()[0]?.model or 'unknown'
      python: ''
      chip_brand: null

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
    fs.mkdirSync path.dirname(LOCKFILE), {recursive:true}
    [code, out, err] = safeRun("#{process.execPath} -m pip freeze")
    if code is 0
      fs.writeFileSync LOCKFILE, out + "\n", 'utf8'
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
        fs.mkdirSync path.dirname(yamlPath), {recursive:true}
        yamlStr = yaml.dump(obj, {sortKeys:false})
        fs.writeFileSync yamlPath, yamlStr, 'utf8'
        return yamlPath
      catch e
        fs.writeFileSync jsonPath, JSON.stringify(obj, null, 2), 'utf8'
        return "#{yamlPath} (YAML write failed → wrote JSON fallback)"

    outPath = writeManifest manifest, MANIFEST_YAML, MANIFEST_JSON

    # --- Step 7: Summary ---
    console.log "\n=== RUN MANIFEST SUMMARY ==="
    console.log "System:", platform_info.system, platform_info.release, "|", platform_info.chip_brand or platform_info.machine
    console.log "Packages:", (k + ":" + (v or '?') for k,v of pkgs).join(", ")
    console.log "Seed:", SEED
    console.log "Lockfile:", LOCKFILE, if lock_hash then "sha256=#{lock_hash[0..11]}…" else "(none)"
    console.log "Manifest path:", outPath
    console.log "============================\n"

    # --- Step 8: Memo update ---
    M.saveThis 'run_manifest.yaml', manifest
    M.saveThis "done:#{stepName}", true
    console.log "💾 Saved run_manifest.yaml to memo"

    return