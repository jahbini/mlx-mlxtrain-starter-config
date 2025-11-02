#!/usr/bin/env coffee
###
043_extract_keywords_kag.coffee — strict memo-aware version (2025)
------------------------------------------------------------------
STEP — Extract semantic keywords for KAG fine-tuning.

Reads a JSONL of {prompt, response} pairs and appends auto-generated
tags for Emotion, Location, and Character.

Outputs:
  <run.data_dir>/<output_jsonl>   (default: out_kag_keywords.jsonl)
###

fs   = require 'fs'
path = require 'path'
os   = require 'os'

@step =
  desc: "Extract semantic keyword tags for KAG fine-tuning"

  action: (M, stepName) ->
    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    stepCfg = cfg[stepName]
    throw new Error "Missing config for '#{stepName}'" unless stepCfg?
    runCfg  = cfg['run'] or {}
    throw new Error "Missing run section in experiment.yaml" unless runCfg?

    DATA_DIR  = path.resolve(stepCfg.output_dir or runCfg.data_dir or 'run/data')
    LOG_DIR   = path.resolve(stepCfg.log_dir or path.join(DATA_DIR, 'logs'))
    INPUT_JSONL  = path.join(DATA_DIR, stepCfg.input_jsonl  or 'out_kag.jsonl')
    OUTPUT_JSONL = path.join(DATA_DIR, stepCfg.output_jsonl or 'out_kag_keywords.jsonl')

    for d in [DATA_DIR, LOG_DIR]
      fs.mkdirSync(d, {recursive:true})

    LOG_PATH = path.join(LOG_DIR, "#{stepName}.log")
    log = (msg) ->
      stamp = new Date().toISOString().replace('T',' ').replace('Z','')
      line  = "[#{stamp}] #{msg}"
      try fs.appendFileSync(LOG_PATH, line + os.EOL, 'utf8') catch e then null
      console.log line

    unless fs.existsSync(INPUT_JSONL)
      throw new Error "Missing input file: #{INPUT_JSONL}"

    # --- Keyword regex tables ------------------------------------
    EMOTION_WORDS =
      joy:       /\b(happy|joy|delight|smile|laugh|hope|love|bliss)\b/i
      sorrow:    /\b(sad|grief|lonely|weep|cry|loss|mourning)\b/i
      anger:     /\b(angry|rage|fury|mad|hate|irritate)\b/i
      fear:      /\b(fear|terror|afraid|panic|horror|scared)\b/i
      wonder:    /\b(awe|wonder|mystery|curious|dream|magic|miracle)\b/i

    LOCATION_WORDS =
      sea:       /\b(sea|ocean|bay|shore|beach|wave|tide|harbor)\b/i
      forest:    /\b(forest|woods|tree|grove|pine|oak|maple|fern)\b/i
      mountain:  /\b(mountain|hill|peak|ridge|valley|cliff)\b/i
      city:      /\b(city|street|alley|building|market|cafe|bar)\b/i
      sky:       /\b(sky|cloud|sun|moon|star|wind|rain)\b/i

    CHARACTER_WORDS =
      your:      /\b(your)\b/i
      friend:    /\b(friend|buddy|pal|companion|stranger)\b/i
      woman:     /\b(woman|lady|girl|mother|daughter|queen)\b/i
      man:       /\b(man|boy|father|son|king)\b/i
      spirit:    /\b(spirit|ghost|angel|soul|god|goddess)\b/i

    titleTag = (k) -> "##{k[0].toUpperCase()}#{k.slice(1)}".replace(/^##/, '#')

    detect_tags = (text) ->
      tags = []
      for [k,re] in Object.entries(EMOTION_WORDS) when re.test(text) then tags.push(titleTag(k))
      for [k,re] in Object.entries(LOCATION_WORDS) when re.test(text) then tags.push(titleTag(k))
      for [k,re] in Object.entries(CHARACTER_WORDS) when re.test(text) then tags.push(titleTag(k))
      seen = new Set(); uniq = []
      for t in tags
        continue if seen.has(t)
        seen.add(t)
        uniq.push(t)
      uniq

    readLines = (p) ->
      fs.readFileSync(p, 'utf8').split(/\r?\n/).filter (l)-> l.trim().length > 0

    safeJSON = (l) ->
      try JSON.parse(l) catch err then null

    # --- Main logic -----------------------------------------------
    log "Starting step: #{stepName}"
    log "Input:  #{INPUT_JSONL}"
    log "Output: #{OUTPUT_JSONL}"

    lines = readLines(INPUT_JSONL)
    out   = fs.createWriteStream(OUTPUT_JSONL, encoding:'utf8')
    total = 0; bad = 0

    for line, idx in lines
      obj = safeJSON(line)
      unless obj?
        bad += 1
        log "[WARN] JSON parse failed on line #{idx+1}" if bad <= 5
        continue
      text = "#{obj.prompt or ''}\n#{obj.response or ''}"
      obj.tags = detect_tags(text)
      try
        out.write(JSON.stringify(obj) + '\n')
        total += 1
      catch e
        log "[WARN] Failed to write output at line #{idx+1}: #{e.message}"

    out.end()
    log "[OK] Wrote #{total} tagged entries to #{OUTPUT_JSONL} (#{bad} bad lines skipped)"

    stats =
      timestamp_utc: new Date().toISOString().replace(/\.\d+Z$/, 'Z')
      total
      bad
      output_jsonl: OUTPUT_JSONL

    M.saveThis "#{stepName}:stats", stats
    M.saveThis "done:#{stepName}", true
    return stats