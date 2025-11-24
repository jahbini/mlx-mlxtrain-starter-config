#!/usr/bin/env coffee
###
md2segments.coffee — Markdown anthology → JSONL segments (memo-native)

- Reads a single Markdown file containing many stories.
- Splits on "# " headers into stories.
- Optional split into paragraphs or keep whole stories.
- Cleans markdown / HTML junk.
- Emits an array of segment objects into the Memo under
  run.marshalled_stories (or run.story_segments), letting
  Memo’s JSONL meta-rule persist to disk.
###

fs   = require 'fs'
path = require 'path'

@step =
  desc: "Convert Markdown stories to JSONL segments (memo only)"

  action: (M, stepName) ->

    throw new Error "Missing stepName" unless stepName?

    cfg = M.theLowdown('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    runCfg  = cfg.run
    stepCfg = cfg[stepName]

    throw new Error "Missing run section"  unless runCfg?
    throw new Error "Missing step section" unless stepCfg?

    # --- Required config keys (no defaults) -----------------------
    inPath  = stepCfg.input_md
    outKey  = runCfg.marshalled_stories ? runCfg.story_segments
    mode    = stepCfg.split_mode

    throw new Error "Missing #{stepName}.input_md"       unless inPath?
    throw new Error "Missing run.marshalled_stories/run.story_segments" unless outKey?
    throw new Error "Missing #{stepName}.split_mode"     unless mode?
    unless mode in ['story','paragraph']
      throw new Error "split_mode must be 'story' or 'paragraph', got #{mode}"

    inAbs = path.resolve(inPath)
    throw new Error "Markdown input not found: #{inAbs}" unless fs.existsSync(inAbs)

    # --- Helpers --------------------------------------------------
    clean = (txt) ->
      s = String(txt ? '')

      # 1) remove our known template token
      s = s.replace(/{{{First Name}}}/g, 'friend')

      # 2) strip simple HTML entities
      s = s.replace(/&[a-zA-Z]+;/g, ' ')

      # 3) remove markdown link refs: [text][1], [1], [text](url)
      s = s.replace(/\[([^\]]+)\]\[\d+\]/g, '$1')
      s = s.replace(/\[\d+\]/g, '')
      s = s.replace(/\[([^\]]+)\]\([^)]+\)/g, '$1')

      # 4) strip emphasis markers _ * ** ***
      s = s.replace(/[_*]{1,3}([^*_]+)[_*]{1,3}/g, '$1')

      # 5) collapse whitespace / line breaks
      #s = s.replace(/\s*\n\s*/g, ' ')
      s = s.replace(/ {2,}/g, ' ')

      s.trim()

    safe = (title) ->
      String(title or '')
        .toLowerCase()
        .replace(/[^a-z0-9]+/g, '-')
        .replace(/^-+|-+$/g, '') or 'untitled'

    # --- Parse anthology into stories -----------------------------
    raw = fs.readFileSync(inAbs, 'utf8')
    lines = raw.split(/\r?\n/)

    stories = []
    currentTitle = null
    buf = []

    flushStory = ->
      return unless currentTitle? and buf.length
      body = buf.join("\n").trim()
      text = clean(body)
      return unless text.length
      stories.push
        title: currentTitle
        text:  text
      buf = []

    for line in lines
      if line.startsWith('# ')
        flushStory()
        currentTitle = line.slice(2).trim()
      else
        buf.push line

    flushStory()

    # --- Build segments in memory ---------------------------------
    rows = []
    splitMode = mode

    for S in stories
      baseId = safe(S.title)

      if splitMode is 'story'
        seg =
          meta:
            doc_id: baseId
            paragraph_index: "1"
            title: S.title
          text: S.text
        rows.push seg

      else
        # paragraph mode
        paras = S.text.split(/\n/).map((p)-> clean(p)).filter((p)-> p.length)
        idx = 1
        for p in paras
          seg =
            meta:
              doc_id: baseId
              paragraph_index: idx.toString().padStart(3, '0')
              title: S.title
            text: p
          rows.push seg
          idx += 1

    console.log "[md2segments] stories:", stories.length, "segments:", rows.length

    # --- Hand off to Memo; meta rules will persist ----------------
    M.saveThis outKey, rows
    M.saveThis "done:#{stepName}", true

    return
