#!/usr/bin/env coffee
###
092_extract_md_for_voice.coffee — strict memo-aware (2025)
----------------------------------------------------------
STEP — Extract Markdown Stories for Voice Fine-Tuning

Reads:
  - Markdown file with "# " headers separating stories

Writes (to run.data_dir):
  - train.jsonl / valid.jsonl
  - <run.contract> / <run.report> / <run.catalog>

Config requirements (experiment.yaml):
  run:
    data_dir: ...
    contract: contract.json
    report:   report.json
    catalog:  catalog.json
  extract_md_for_voice:
    input_md: path/to/your.md
    valid_fraction: 0.10
    min_story_words: 50
    seed: 123
###

fs        = require 'fs'
path      = require 'path'
os        = require 'os'
crypto    = require 'crypto'
seedrand  = require 'seedrandom'

@step =
  desc: "Extract markdown stories into train/valid JSONL + contract/report/catalog (no defaults)"

  action: (M, stepName) ->
    # ---------- Load config from Memo ----------
    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    runCfg  = cfg['run']
    stepCfg = cfg[stepName]
    throw new Error "Missing 'run' section" unless runCfg?
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?

    for k in ['data_dir','contract','report','catalog']
      throw new Error "Missing required run.#{k}" unless k of runCfg

    for k in ['input_md','valid_fraction','min_story_words','seed']
      throw new Error "Missing required #{stepName}.#{k}" unless k of stepCfg

    DATA_DIR  = path.resolve(runCfg.data_dir)
    CONTRACT  = path.join(DATA_DIR, runCfg.contract)
    REPORT    = path.join(DATA_DIR, runCfg.report)
    CATALOG   = path.join(DATA_DIR, runCfg.catalog)

    INPUT_MD  = path.resolve(stepCfg.input_md)
    VALID_FRAC      = Number(stepCfg.valid_fraction)
    MIN_STORY_WORDS = parseInt(stepCfg.min_story_words)
    SEED            = String(stepCfg.seed)

    fs.mkdirSync(DATA_DIR, {recursive:true})

    LOG_DIR = path.join(DATA_DIR, 'logs')
    fs.mkdirSync(LOG_DIR, {recursive:true})
    LOG_PATH = path.join(LOG_DIR, "#{stepName}.log")
    log = (msg) ->
      stamp = new Date().toISOString().replace('T',' ').replace(/\.\d+Z$/,'')
      line  = "[#{stamp}] #{msg}"
      try fs.appendFileSync(LOG_PATH, line + os.EOL, 'utf8') catch e then null
      console.log line

    # ---------- Safety checks ----------
    unless fs.existsSync(INPUT_MD)
      throw new Error "[FATAL] Markdown input not found: #{INPUT_MD}"

    unless Number.isFinite(VALID_FRAC) and VALID_FRAC > 0 and VALID_FRAC < 1
      throw new Error "[FATAL] valid_fraction must be (0,1): got #{stepCfg.valid_fraction}"

    unless Number.isInteger(MIN_STORY_WORDS) and MIN_STORY_WORDS > 0
      throw new Error "[FATAL] min_story_words must be positive integer"

    rng = seedrand(SEED)

    TRAIN_JSONL = path.join(DATA_DIR, 'train.jsonl')
    VALID_JSONL = path.join(DATA_DIR, 'valid.jsonl')

    # ---------- Helpers ----------
    split_paragraphs = (s) ->
      (p.trim() for p in s.split(/\n{2,}/) when p.trim().length > 0)

    extract_md_stories = (mdPath) ->
      stories = []
      currentTitle = null
      currentBody  = []
      for line in fs.readFileSync(mdPath,'utf8').split(/\r?\n/)
        line = line.trimEnd()
        if line.startsWith '# '
          if currentTitle and currentBody.length
            stories.push [currentTitle, currentBody.join('\n').trim()]
          currentTitle = line.slice(2).trim()
          currentBody = []
        else if currentTitle?
          currentBody.push line
      if currentTitle and currentBody.length
        stories.push [currentTitle, currentBody.join('\n').trim()]
      stories

    count_lines_bytes = (p) ->
      buf = fs.readFileSync p
      # count newline-terminated lines; tolerate missing final newline
      lines = String(buf).split('\n').filter((x)->x.trim().length).length
      [lines, buf.length]

    sha256_file = (p) ->
      crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex')

    summarize_lengths = (p, field) ->
      lens = []
      for ln in fs.readFileSync(p,'utf8').split('\n') when ln.trim().length
        try
          obj = JSON.parse ln
          s = obj[field]
          lens.push s.length if typeof s is 'string'
        catch then null
      return {n:0} unless lens.length
      lens.sort (a,b)->a-b
      n = lens.length
      p95 = lens[Math.floor(0.95*(n-1))]
      {n, len_min:lens[0], len_med:lens[Math.floor(n/2)], len_95:p95, len_max:lens[n-1]}

    # ---------- Main ----------
    log "[INFO] #{stepName} starting"
    log "[INFO] Input:  #{INPUT_MD}"
    log "[INFO] Output: #{DATA_DIR}"
    log "[INFO] seed=#{SEED} valid_fraction=#{VALID_FRAC} min_story_words=#{MIN_STORY_WORDS}"

    stories = extract_md_stories INPUT_MD
    examples = []

    for idx, story of stories
      [title, text] = story
      continue unless text and text.split(/\s+/).length >= MIN_STORY_WORDS
      paragraphs = split_paragraphs text
      for i, para of paragraphs
        n = i + 1
        examples.push
          meta:
            doc_id: "story-#{idx}"
            title: title
            paragraph_index: n
          prompt: para + "\n\n"
          completion: ""

    log "[INFO] Extracted #{examples.length} examples"

    # deterministic shuffle (Fisher–Yates using rng)
    for i in [examples.length-1..1]
      j = Math.floor(rng() * (i+1))
      [examples[i], examples[j]] = [examples[j], examples[i]]

    n_valid = Math.max 1, Math.floor(examples.length * VALID_FRAC)
    valid = examples.slice 0, n_valid
    train = examples.slice n_valid

    write_jsonl = (fn, arr) ->
      out = fs.createWriteStream fn, encoding:'utf8'
      for ex in arr
        out.write JSON.stringify(ex) + '\n'
      out.end()

    write_jsonl TRAIN_JSONL, train
    write_jsonl VALID_JSONL, valid
    log "[OK] Wrote #{TRAIN_JSONL} (#{train.length})"
    log "[OK] Wrote #{VALID_JSONL} (#{valid.length})"

    # ---------- Contract / Report / Catalog ----------
    probeLine = fs.readFileSync(TRAIN_JSONL,'utf8').split('\n').find((l)->l.trim()) or '{}'
    probe = JSON.parse(probeLine)

    mode = if 'prompt' of probe and 'completion' of probe then 'sft' else 'plain'
    target_field = if mode is 'sft' then 'completion' else 'text'
    schema_fields = if mode is 'sft' then {prompt:'string', completion:'string'} else {text:'string'}

    created = new Date().toISOString().replace(/\.\d+Z$/,'Z')
    [t_lines, t_bytes] = count_lines_bytes TRAIN_JSONL
    [v_lines, v_bytes] = count_lines_bytes VALID_JSONL

    contract =
      created_utc: created
      data_dir: DATA_DIR
      filenames:
        train: {chosen: path.basename(TRAIN_JSONL), resolved: TRAIN_JSONL}
        valid: {chosen: path.basename(VALID_JSONL), resolved: VALID_JSONL}
      schema: {format: 'jsonl', fields: schema_fields}
      source: {mode, target_field, origin: 'markdown_file'}

    fs.writeFileSync(CONTRACT, JSON.stringify(contract, null, 2), 'utf8')

    report =
      created_utc: created
      counts: {train: t_lines, valid: v_lines}
      train_stats: summarize_lengths(TRAIN_JSONL, target_field)
      valid_stats: summarize_lengths(VALID_JSONL, target_field)
      target_field: target_field
      schema_mode: mode

    fs.writeFileSync(REPORT, JSON.stringify(report, null, 2), 'utf8')

    catalog =
      created_utc: created
      data_dir: DATA_DIR
      mode: mode
      target_field: target_field
      schema: schema_fields
      total_examples: {train: t_lines, valid: v_lines}
      files:
        train:
          path: TRAIN_JSONL
          lines: t_lines
          bytes: t_bytes
          sha256: sha256_file(TRAIN_JSONL)
        valid:
          path: VALID_JSONL
          lines: v_lines
          bytes: v_bytes
          sha256: sha256_file(VALID_JSONL)
      checksums:
        contract: sha256_file(CONTRACT)
        report:   sha256_file(REPORT)

    fs.writeFileSync(CATALOG, JSON.stringify(catalog, null, 2), 'utf8')
    log "[OK] Wrote contract/report/catalog to #{DATA_DIR}"

    # ---------- Memo signals ----------
    M.saveThis "done:#{stepName}", true
    M.saveThis "#{stepName}:counts", {train: t_lines, valid: v_lines}
    M.saveThis "#{stepName}:contract_path", CONTRACT
    return