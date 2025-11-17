#!/usr/bin/env coffee
###
051_kag_merge.coffee — KAG merge step
-------------------------------------
• Reads story segments JSONL + emotion tags JSONL
• Matches on (meta.doc_id, meta.paragraph_index)
• Writes a new JSONL with structured "Instruction + Segment + Meta" text
• Output is a standard {"text": "..."} per line, ready for LoRA

{"meta":{"doc_id":"story-210","title":"leo-and-stations-healing.md","paragraph_index":"81"},"prompt":"Pathy look at it with disgust, &ldquo;Slugs, ugh, get rid of it.&rdquo; Leo shoots a glance to Station and flares his nostrils like Kirk Douglas in a '50s flick. Station shoots a look back like Victor Mature and becomes kittenish. &nbsp;They lock hands and walk up the path to their house: &quot;Thank you, Pathy: You have no idea the gift you have found.&quot;\n\n","completion":""}

{"meta":{"doc_id":"story-285","title":"the-man-who-walked.md","paragraph_index":"131"},"model":"microsoft/Phi-3-mini-4k-instruct","emotions":[{"emotion":"anger","intensity":"moderate"},{"emotion":"fear","intensity":"mild"}]}
###

fs   = require 'fs'
path = require 'path'

@step =
  desc: "Merge story segments with emotion tags into KAG training JSONL"

  action: (M, stepName) ->

    throw new Error "Missing stepName argument" unless stepName?

    cfg = M.theLowdown('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    runCfg = cfg.run
    throw new Error "Missing step config for #{stepName}" unless runCfg?

    # We deliberately do not pre-check keys; failures should happen at first use.
    storiesPath  = path.resolve(runCfg.stories)
    emotionsPath = path.resolve(runCfg.emotions)
    outPath      = runCfg.mergedStories

    readLines = (p) ->
      fs.readFileSync(p, 'utf8').split(/\r?\n/).filter (l) -> l.trim().length

    parseJSONL = (lines, label) ->
      out = []
      for line, idx in lines
        try
          out.push JSON.parse(line)
        catch e
          console.warn "[kag_merge] bad #{label} JSON at line #{idx+1} in #{label}: #{e.message}"
      out

    makeKey = (meta) ->
      doc  = meta?.doc_id ? meta?.docID ? meta?.id
      para = meta?.paragraph_index ? meta?.para ? meta?.paragraph
      "#{doc}|#{para}"

    # 1) Load emotions and build lookup map
    emoLines = readLines(emotionsPath)
    emoRows  = parseJSONL(emoLines, "emotions")

    emoMap = Object.create(null)
    for row in emoRows
      k = makeKey(row.meta or {})
      continue unless k? and k isnt 'undefined|undefined'
      emoArr = row.emotions or []
      emoMap[k] ?= []
      emoMap[k] = emoMap[k].concat(emoArr)

    # 2) Load stories and merge
    storyLines = readLines(storiesPath)
    storyRows  = parseJSONL(storyLines, "stories")

    outLines = []
    totalStories   = 0
    matchedStories = 0
    missingEmo     = 0

    for row in storyRows
      totalStories += 1
      meta = row.meta or {}
      k    = makeKey(meta)

      emos = if k? then emoMap[k] else null
      unless emos? and emos.length
        missingEmo += 1
        continue

      matchedStories += 1
      continue unless row.prompt?.length

      # Option C — structured schema, encoded into a single "text" field
      parts = 
        Meta: 
          doc_id: meta.doc_id
          paragraph_index: meta.paragraph_index
        Emotions: emos
        prompt: row.prompt

      text = JSON.stringify(parts)
      outLines.push text  # single "text" field per row

    # 3) Write output JSONL
    M.saveThis outPath, outLines.join("\n") + "\n"

    console.log "[kag_merge] stories=#{totalStories} matched=#{matchedStories} missing_emotions=#{missingEmo}"
    console.log "[kag_merge] wrote #{outLines.length} KAG rows to #{outPath}"

    # Memo bookkeeping (optional, but handy)
    M.saveThis "kag_merge:counts",
      total_stories: totalStories
      matched_stories: matchedStories
      missing_emotions: missingEmo
      output_jsonl: path.relative(process.cwd(), outPath)

    M.saveThis "done:#{stepName}", true
    return
