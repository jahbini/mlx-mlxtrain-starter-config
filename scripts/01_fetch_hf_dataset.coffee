#!/usr/bin/env coffee
###
01_fetch_hf_dataset.coffee — strict memo-aware version (final)
---------------------------------------------------------------
• Executes inside unified pipeline (shared @memo).
• Receives (M, stepName) directly — no env access.
• Aborts on any missing config key.
• Produces train.jsonl / valid.jsonl, data_contract.json, data_catalog.json.
###

fs     = require 'fs'
path   = require 'path'
crypto = require 'crypto'
child  = require 'child_process'
rand   = require 'seedrandom'

@step =
  desc: "Fetch and preprocess a HuggingFace dataset into train/valid splits (strict, no defaults)"

  action: (M, stepName) ->

    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    stepCfg = cfg[stepName]
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?
    runCfg = cfg['run']
    throw new Error "Missing global 'run' section in experiment.yaml" unless runCfg?

    # --- Required keys (no defaults allowed) ---
    requiredStepKeys = [
      'hf_dataset','subset','mode','valid_fract',
      'min_words','max_words','seed'
    ]
    for k in requiredStepKeys
      throw new Error "Missing required param '#{k}' in step '#{stepName}'" unless k of stepCfg

    requiredRunKeys = ['data_dir','contract','catalog']
    for k in requiredRunKeys
      throw new Error "Missing required run.#{k}" unless k of runCfg

    DATA_DIR  = path.resolve(runCfg.data_dir)
    CONTRACT  = path.join(DATA_DIR, runCfg.contract)
    CATALOG   = path.join(DATA_DIR, runCfg.catalog)

    HF_DATASET  = stepCfg.hf_dataset
    SUBSET      = stepCfg.subset
    MODE        = stepCfg.mode
    VALID_FRACT = stepCfg.valid_fract
    MIN_WORDS   = stepCfg.min_words
    MAX_WORDS   = stepCfg.max_words
    SEED        = stepCfg.seed

    fs.mkdirSync(DATA_DIR, {recursive:true})
    console.log "📦 Fetching:", HF_DATASET, "subset:", SUBSET

    rng = rand(SEED)
    Math.random = rng

    wc  = (s) -> String(s).split(/\s+/).length
    sha = (s) -> crypto.createHash('sha256').update(String(s)).digest('hex')
    timestampUTC = -> new Date().toISOString().replace(/\.\d+Z$/, 'Z')

    writeJSONL = (file, arr) ->
      fout = fs.createWriteStream(file, {flags:'w', encoding:'utf8'})
      for t in arr
        fout.write JSON.stringify({text:t}) + "\n"
      fout.close()

    countLinesBytes = (p) ->
      data = fs.readFileSync(p)
      n = (data.toString('utf8').match(/\n/g) or []).length
      [n, data.length]

    sha256File = (p) ->
      crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex')

    # --- Load dataset via Python subprocess ---
    script = """
from datasets import load_dataset
import json
ds = load_dataset(#{JSON.stringify(HF_DATASET)}, name=#{JSON.stringify(SUBSET)}, split='train')
for r in ds:
  print(json.dumps(r))
"""
    res = child.spawnSync('python', ['-u', '-c', script], {encoding:'utf8'})
    if res.error? or res.status isnt 0
      console.error "❌ datasets.load_dataset failed"
      console.error res.stderr
      throw new Error "HF dataset load failed"

    lines = res.stdout.trim().split(/\\r?\\n/)
    rawRows = []
    for l in lines when l.trim().length
      try rawRows.push JSON.parse(l)
      catch e then console.warn "⚠️ bad JSON row", e.message

    console.log "Fetched #{rawRows.length} records"

    rows = []
    for r in rawRows
      quote  = (r.quote or '').trim()
      author = (r.author or '').trim()
      continue unless quote.length
      text = if MODE is 'plain'
        quote
      else
        instr = if author then "Write a short motivational quote in the style of #{author}." else "Write a short motivational quote."
        "Instruction:\\n#{instr}\\n\\nResponse:\\n#{quote}"
      continue unless MIN_WORDS <= wc(text) <= MAX_WORDS
      rows.push text

    seen = new Set()
    uniq = []
    for t in rows
      h = sha(t)
      unless seen.has(h)
        seen.add(h)
        uniq.push(t)

    uniq.sort -> rng() - 0.5
    valid_n = Math.max(1, Math.floor(uniq.length * VALID_FRACT))
    valid = uniq.slice(0, valid_n)
    train = uniq.slice(valid_n)

    trainPath = path.join(DATA_DIR, 'train.jsonl')
    validPath = path.join(DATA_DIR, 'valid.jsonl')

    writeJSONL(trainPath, train)
    writeJSONL(validPath, valid)
    console.log "✅ Wrote #{train.length} train, #{valid.length} valid"

    created = timestampUTC()

    data_contract =
      created_utc: created
      data_dir: DATA_DIR
      filenames:
        train: { chosen: path.basename(trainPath), resolved: path.resolve(trainPath) }
        valid: { chosen: path.basename(validPath), resolved: path.resolve(validPath) }
      schema:
        format: 'jsonl'
        fields: { text: 'string' }

    [t_lines, t_bytes] = countLinesBytes(trainPath)
    [v_lines, v_bytes] = countLinesBytes(validPath)
    t_sha = sha256File(trainPath)
    v_sha = sha256File(validPath)

    data_catalog =
      created_utc: created
      files:
        train: { path: path.resolve(trainPath), lines: t_lines, bytes: t_bytes, sha256: t_sha }
        valid: { path: path.resolve(validPath), lines: v_lines, bytes: v_bytes, sha256: v_sha }
      entries:
        train:
          path: path.resolve(trainPath)
          stats:
            num_valid_examples: t_lines
            num_bytes: t_bytes
            sha256: t_sha
        valid:
          path: path.resolve(validPath)
          stats:
            num_valid_examples: v_lines
            num_bytes: v_bytes
            sha256: v_sha

    M.saveThis 'data_contract.json', data_contract
    M.saveThis 'data_catalog.json', data_catalog
    M.saveThis "done:#{stepName}", true
    console.log "📗 Recorded data_contract.json and data_catalog.json"

    return