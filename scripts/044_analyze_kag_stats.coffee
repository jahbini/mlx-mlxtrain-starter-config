#!/usr/bin/env coffee
###
044_analyze_kag_stats.coffee
----------------------------------------
STEP — Analyze KAG tag statistics
Summarizes frequency and co-occurrence of tags
from out_kag_keywords.jsonl (Stage 2 output).

Outputs:
  - run/data/kag_tag_stats.json
  - run/data/kag_tag_matrix.csv
  - run/data/logs/044_analyze_kag_stats.log
###

fs   = require 'fs'
path = require 'path'
os   = require 'os'
process.env.NODE_NO_WARNINGS = 1

# --- 1) Config loader ------------------------------------------------
{load_config} = require '../config_loader'
CFG       = load_config()
STEP_NAME = process.env.STEP_NAME or 'analyze_kag_stats'
STEP_CFG  = CFG.pipeline.steps?[STEP_NAME] or {}
PARAMS    = STEP_CFG.params or {}

DATA_DIR  = path.resolve PARAMS.output_dir or CFG.data.output_dir
LOG_DIR   = path.join DATA_DIR, 'logs'
INPUT_JSONL  = path.join DATA_DIR, PARAMS.input_jsonl or 'out_kag_keywords.jsonl'
OUT_JSON     = path.join DATA_DIR, PARAMS.output_stats or 'kag_tag_stats.json'
OUT_CSV      = path.join DATA_DIR, PARAMS.output_csv or 'kag_tag_matrix.csv'

fs.mkdirSync DATA_DIR, {recursive: true}
fs.mkdirSync LOG_DIR,  {recursive: true}

logPath = path.join LOG_DIR, "#{STEP_NAME}.log"
log = (msg) ->
  stamp = new Date().toISOString().replace('T',' ').replace('Z','')
  fs.appendFileSync logPath, "[#{stamp}] #{msg}" + os.EOL, 'utf8'
  console.log "[#{stamp}] #{msg}"

# --- 2) Helpers ------------------------------------------------------
increment = (obj, key, by=1) ->
  obj[key] = (obj[key] or 0) + by

pairKey = (a,b) ->
  if a < b then "#{a},#{b}" else "#{b},#{a}"

# --- 3) Main ---------------------------------------------------------
main = ->
  log "Starting step: #{STEP_NAME}"
  unless fs.existsSync INPUT_JSONL
    log "[FATAL] Missing input: #{INPUT_JSONL}"
    process.exit 1

  tagCounts = {}
  coOccur   = {}
  categoryCounts =
    emotion: 0
    location: 0
    character: 0
  totalEntries = 0

  lines = fs.readFileSync(INPUT_JSONL, 'utf8').split('\n').filter (l) -> l.trim().length > 0
  for line in lines
    try
      obj = JSON.parse line
      tags = obj.tags or []
      totalEntries += 1
      for t in tags
        increment tagCounts, t
        # infer category by prefix
        if /#(Joy|Sorrow|Anger|Fear|Wonder)/i.test t
          categoryCounts.emotion += 1
        else if /#(Sea|Forest|Mountain|City|Sky)/i.test t
          categoryCounts.location += 1
        else
          categoryCounts.character += 1

      # co-occurrence pairs
      for i in [0...tags.length]
        for j in [i+1...tags.length]
          key = pairKey(tags[i], tags[j])
          increment coOccur, key
    catch err
      log "[WARN] JSON parse failed: #{err.message}"

  # --- 4) Write JSON summary ----------------------------------------
  summary =
    total_entries: totalEntries
    tag_counts: tagCounts
    categories: categoryCounts
    co_occurrences: coOccur

  fs.writeFileSync OUT_JSON, JSON.stringify(summary, null, 2), 'utf8'
  log "[OK] Wrote stats → #{OUT_JSON}"

  # --- 5) Write CSV matrix ------------------------------------------
  allTags = Object.keys(tagCounts).sort()
  outCSV = fs.createWriteStream OUT_CSV, encoding:'utf8'
  outCSV.write "tag1,tag2,count\n"
  for [pair,count] in Object.entries coOccur
    [a,b] = pair.split(',')
    outCSV.write "#{a},#{b},#{count}\n"
  outCSV.end()
  log "[OK] Wrote co-occurrence matrix → #{OUT_CSV}"

  # --- 6) Human-readable summary ------------------------------------
  topTags = Object.entries(tagCounts).sort((a,b) -> b[1]-a[1]).slice(0,10)
  log "Top 10 tags:"
  for [t,c] in topTags
    log "  #{t.padEnd(12)} #{c}"

  log "Category totals: emotion=#{categoryCounts.emotion}, location=#{categoryCounts.location}, character=#{categoryCounts.character}"
  log "Completed successfully."
  process.exit 0

main()
