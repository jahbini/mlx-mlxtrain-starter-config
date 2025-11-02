#!/usr/bin/env coffee
###
04_snapshot.coffee — strict memo-aware version (2025)
-----------------------------------------------------
• Executes inside unified pipeline (shared @memo)
• Receives (M, stepName) directly — no env access
• Loads LoRA models, generates few-shot text samples
• Writes JSONL + CSV outputs
###

fs      = require 'fs'
path    = require 'path'
crypto  = require 'crypto'
child   = require 'child_process'

@step =
  desc: "Run MLX inference snapshot with few-shot prompts"

  action: (M, stepName) ->

    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    stepCfg = cfg[stepName]
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?
    runCfg = cfg['run']
    throw new Error "Missing global 'run' section in experiment.yaml" unless runCfg?

    # --- Required keys ---
    for k in ['data_dir','contract','experiments_csv','snapshot_dir','output_dir','eval_dir','generations','tokmeta']
      throw new Error "Missing required run.#{k}" unless k of runCfg

    for k in ['prompts','max_new','alt_seed','n_shots','min_words','retries']
      throw new Error "Missing required param '#{k}' in step '#{stepName}'" unless k of stepCfg

    DATA_DIR  = path.resolve(runCfg.data_dir)
    EXPERIMENTS_CSV = path.join(DATA_DIR, runCfg.experiments_csv)
    SNAPSHOT_DIR    = path.join(runCfg.snapshot_dir)
    CONTRACT_PATH   = path.join(DATA_DIR, runCfg.contract)
    OUT_DIR    = path.resolve(runCfg.output_dir)
    EVAL_DIR   = path.resolve(runCfg.eval_dir)
    JSONL_PATH = path.join(EVAL_DIR, "#{runCfg.generations}.jsonl")
    CSV_PATH   = path.join(EVAL_DIR, "#{runCfg.generations}.csv")
    TOKMETA    = path.join(OUT_DIR, "#{runCfg.tokmeta}.json")

    PROMPTS   = stepCfg.prompts
    MAX_NEW   = stepCfg.max_new
    SEED      = stepCfg.alt_seed
    N_SHOTS   = stepCfg.n_shots
    MIN_WORDS = stepCfg.min_words
    RETRIES   = stepCfg.retries

    fs.mkdirSync OUT_DIR, {recursive:true}
    fs.mkdirSync EVAL_DIR, {recursive:true}
    fs.mkdirSync SNAPSHOT_DIR, {recursive:true}

    sha = (s) -> crypto.createHash('sha256').update(String(s), 'utf8').digest('hex')
    wc  = (s) -> String(s).trim().split(/\s+/).length
    readJSON = (p) -> JSON.parse(fs.readFileSync(p, 'utf8'))
    timestampUTC = -> new Date().toISOString().replace(/\.\d+Z$/, 'Z')

    contract = readJSON(CONTRACT_PATH)
    fields = contract?.schema?.fields or {}
    text_field = Object.keys(fields).find((k)->String(fields[k]).toLowerCase() is 'string') or 'text'
    train_path = contract?.filenames?.train?.resolved or path.join(DATA_DIR, 'train.jsonl')

    # --- Load & dedupe training corpus ---
    train_lines = []
    for line in fs.readFileSync(train_path, 'utf8').split(/\r?\n/)
      try
        obj = JSON.parse(line)
        t = obj[text_field]
        if typeof t is 'string' and t.trim().length
          train_lines.push t.trim()
      catch e then continue

    seen = new Set()
    unique = []
    for t in train_lines
      h = sha(t)
      unless seen.has(h)
        seen.add(h)
        unique.push(t)

    short  = unique.filter (t)-> wc(t) <= 4
    medium = unique.filter (t)-> wc(t) >= 5 and wc(t) <= 12
    longer = unique.filter (t)-> wc(t) > 12
    train_blob = unique.join('\n\n')
    train_set  = new Set(unique)

    pickDiverseShots = (k) ->
      pool = []
      pool.push short[Math.floor(Math.random()*short.length)] if short.length
      pool.push medium[Math.floor(Math.random()*medium.length)] if medium.length
      pool.push longer[Math.floor(Math.random()*longer.length)] if longer.length
      rest = unique.filter (t)-> not pool.includes(t)
      shuffled = rest.sort(-> Math.random()-0.5)
      (pool.concat(shuffled)).slice(0,k)

    formatFewshot = (prompt, shots) ->
      "Some Proverbs:\n- " + shots.join("\n- ") + "\n\n#{prompt}\n- "

    trimOnCustomStop = (text, stop) ->
      i = text.indexOf(stop)
      if i is -1 then text else text.slice(0, i)

    isBad = (gen) ->
      g = gen.trim()
      return true if wc(g) < MIN_WORDS
      return true if train_set.has(g)
      return true if g.length >= 24 and train_blob.includes(g)
      false

    runMLXGenerate = (prompt, modelPath, adapterPath, maxNew) ->
      script = """
from mlx_lm import load, generate
m,t = load(#{JSON.stringify(modelPath)}, adapter_path=#{JSON.stringify(adapterPath)})
out = generate(model=m, tokenizer=t, prompt=#{JSON.stringify(prompt)}, max_tokens=#{maxNew})
print(out)
"""
      res = child.spawnSync('python', ['-u', '-c', script], {encoding:'utf8'})
      if res.error? or res.status isnt 0
        console.error "❌ MLX generate failed", res.stderr
        return ''
      res.stdout.trim()

    generateOnce = (prompt, modelPath, adapterPath) ->
      tries = 0
      loop
        shots = pickDiverseShots(N_SHOTS)
        fp = formatFewshot(prompt, shots)
        txt = runMLXGenerate(fp, modelPath, adapterPath, MAX_NEW)
        gen = if txt.startsWith(fp) then txt.slice(fp.length).trim() else txt.trim()
        return [fp, gen, shots] unless isBad(gen) and tries < RETRIES
        tries++
        if tries >= RETRIES then return [fp, gen, shots]

    ts = timestampUTC()
    textCSV = fs.readFileSync(EXPERIMENTS_CSV, 'utf8').split(/\r?\n/)
    header = textCSV[0].split(',')
    rows = []
    for l in textCSV.slice(1) when l.trim().length
      cols = l.split(',')
      row = {}
      for i in [0...header.length]
        row[header[i]] = cols[i]
      rows.push(row)

    allRows = []
    for i in [0...rows.length]
      row = rows[i]
      base = (row.model_id or '').trim()
      adapter = (row.adapter_path or '').trim()
      artifactLabel = ''
      modelPath = null
      adapterPath = null

      if adapter and fs.existsSync(adapter)
        modelPath = base
        adapterPath = adapter
        artifactLabel = 'base+adapter'
      else
        console.warn "[WARN] Skipping row #{i} — no valid model found"
        continue

      tokmeta =
        eos_token: null
        eos_token_id: null
        pad_token: null
        pad_token_id: null
      fs.writeFileSync(TOKMETA, JSON.stringify(tokmeta, null, 2), 'utf8')

      MODES = ['default_eos','no_eos','custom_stop']
      CUSTOM_STOP = '\n\n'
      for p in PROMPTS
        for mode in MODES
          [fp, gen, shots] = generateOnce(p, modelPath, adapterPath)
          if mode is 'custom_stop'
            gen = trimOnCustomStop(gen, CUSTOM_STOP).trim()
          allRows.push
            timestamp: ts
            seed: SEED
            model_id: base
            artifact: artifactLabel
            artifact_model_path: modelPath
            adapter_path: adapterPath or ''
            prompt_variant: 'fewshot-dynamic'
            mode: mode
            prompt: p
            input_text: fp
            output_text: gen
            generation: gen
            shots: shots
            max_new_tokens: MAX_NEW
            custom_stop: if mode is 'custom_stop' then CUSTOM_STOP else ''
          console.log "[#{mode}] #{p} → #{gen.slice(0,80)}..."

    # --- Write JSONL ---
    fs.writeFileSync(JSONL_PATH, '', 'utf8')
    fjson = fs.createWriteStream(JSONL_PATH, {flags:'a', encoding:'utf8'})
    for r in allRows
      fjson.write JSON.stringify(r, null, 0) + "\n"
    fjson.close()

    # --- Write CSV ---
    csvCols = ["timestamp","seed","model_id","artifact","artifact_model_path","adapter_path",
      "prompt_variant","mode","prompt","generation","output_text","shots","max_new_tokens","custom_stop"]

    csvOut = fs.createWriteStream(CSV_PATH, {encoding:'utf8'})
    csvOut.write(csvCols.join(',') + "\n")
    for r in allRows
      rr = {...r, shots: (r.shots or []).join(' | ')}
      line = csvCols.map((c)-> rr[c] ? '').join(',')
      csvOut.write(line + "\n")
    csvOut.close()

    console.log "Rows written: #{allRows.length} → #{JSONL_PATH} and #{CSV_PATH}"

    M.saveThis 'snapshot:rows', allRows
    M.saveThis "done:#{stepName}", true
    return