#!/usr/bin/env coffee
###
051_kag_merge.coffee — KAG merge step
-------------------------------------
• Reads story segments JSONL + emotion tags JSONL
• Matches on (meta.doc_id, meta.paragraph_index)
• Writes a new JSONL with structured "Instruction + Segment + Meta" text
• Output is a standard {"text": "..."} per line, ready for LoRA
###

fs   = require 'fs'
path = require 'path'

@step =
  desc: "Merge story segments with emotion tags into KAG training JSONL"

  action: (M, stepName) ->

    throw new Error "Missing stepName argument" unless stepName?

    cfg = M.theLowdown('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    stepCfg = cfg[stepName]
    throw new Error "Missing step config for #{stepName}" unless stepCfg?

    # We deliberately do not pre-check keys; failures should happen at first use.
    storiesPath  = path.resolve(stepCfg.stories_jsonl)
    emotionsPath = path.resolve(stepCfg.emotions_jsonl)
    outPath      = path.resolve(stepCfg.output_jsonl)

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

      # Option B — token-friendly emotion spec: anger:moderate, fear:mild
      specs = []
      for e in emos
        emo = String(e.emotion ? e.label ? '').trim()
        inten = String(e.intensity ? '').trim()
        continue unless emo.length
        spec = if inten.length then "#{emo}:#{inten}" else emo
        specs.push spec
      continue unless specs.length

      matchedStories += 1
      mixText = specs.join(", ")

      # Segment text — prefer prompt, then text, then completion
      seg =
        if typeof row.prompt is 'string' and row.prompt.length then row.prompt
        else if typeof row.text is 'string' and row.text.length then row.text
        else if typeof row.completion is 'string' and row.completion.length then row.completion
        else ''

      seg = seg.trim()
      continue unless seg.length

      # Option C — structured schema, encoded into a single "text" field
      parts = [
        "Instruction:"
        "Write or continue in a style that expresses the following emotional mix:"
        "  #{mixText}"
        ""
        "Segment:"
        seg
        ""
        "Meta:"
        "  doc_id: #{meta.doc_id}"
        "  paragraph_index: #{meta.paragraph_index}"
        ""
      ]

      text = parts.join("\n")
      outLines.push JSON.stringify({text})  # single "text" field per row

    # 3) Write output JSONL
    fs.mkdirSync(path.dirname(outPath), {recursive:true})
    fs.writeFileSync(outPath, outLines.join("\n") + "\n", 'utf8')

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
