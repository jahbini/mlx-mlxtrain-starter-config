#!/usr/bin/env coffee
###
094_oracle_kag.coffee — strict memo-aware (2025)
------------------------------------------------
STEP — Keyword-Augmented Generation (KAG) oracle

1. Parses Markdown stories → clean text in stories_out/
2. Calls local MLX model to infer emotional hashtags
3. Normalizes & reinforces hashtags across corpus
4. Builds hashtag index (hashtags/)
5. Writes unified story_hashtags.jsonl
###

fs      = require 'fs'
path    = require 'path'
os      = require 'os'
crypto  = require 'crypto'
child   = require 'child_process'

@step =
  desc: "Infer emotional hashtags from Markdown stories (strict memo-aware Oracle KAG)"

  action: (M, stepName) ->
    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    runCfg  = cfg['run']
    stepCfg = cfg[stepName]
    throw new Error "Missing 'run' section" unless runCfg?
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?

    for k in ['data_dir']
      throw new Error "Missing required run.#{k}" unless k of runCfg

    for k in ['model','input_md','num_tags','max_tokens','reinforce']
      throw new Error "Missing required #{stepName}.#{k}" unless k of stepCfg

    reinf = stepCfg.reinforce
    for k in ['top_n','min_global']
      throw new Error "Missing required #{stepName}.reinforce.#{k}" unless k of reinf

    DATA_DIR   = path.resolve(runCfg.data_dir)
    MODEL_ID   = stepCfg.model
    INPUT_MD   = path.resolve(stepCfg.input_md)
    NUM_TAGS   = parseInt(stepCfg.num_tags)
    MAX_TOKENS = parseInt(stepCfg.max_tokens)
    TOP_N      = parseInt(reinf.top_n)
    MIN_GLOBAL = parseInt(reinf.min_global)
    PROMPT     = stepCfg.prompt_template or "List #{NUM_TAGS} emotional or archetypal themes present in this story:"
    OUT_DIR    = path.join(DATA_DIR, 'oracle_kag')
    STORIES_OUT = path.join(OUT_DIR, 'stories_out')
    HASHTAGS_OUT = path.join(OUT_DIR, 'hashtags')
    fs.mkdirSync OUT_DIR, {recursive:true}
    fs.mkdirSync STORIES_OUT, {recursive:true}
    fs.mkdirSync HASHTAGS_OUT, {recursive:true}

    LOG_PATH = path.join(OUT_DIR, "#{stepName}.log")
    log = (msg) ->
      stamp = new Date().toISOString().replace('T',' ').replace(/\.\d+Z$/,'')
      line  = "[#{stamp}] #{msg}"
      try fs.appendFileSync(LOG_PATH, line + os.EOL, 'utf8') catch e then null
      console.log line

    log "=== Oracle KAG starting ==="
    log "Model: #{MODEL_ID}"
    log "Input: #{INPUT_MD}"
    log "Tags=#{NUM_TAGS}  MaxTokens=#{MAX_TOKENS}  Reinforce(top=#{TOP_N},min=#{MIN_GLOBAL})"

    # ---------- Helpers ----------
    clean_markdown_text = (txt) ->
      s = txt.replace(/{{{First Name}}}/g, 'friend')
      s = s.replace(/&[a-z]+;/g, ' ')
      s = s.replace(/\[([^\]]+)\]\[\d+\]/g, '$1')
      s = s.replace(/\[\d+\]/g, '')
      s = s.replace(/\[([^\]]+)\]\([^)]+\)/g, '$1')
      s = s.replace(/[_*]{1,3}([^*_]+)[_*]{1,3}/g, '$1')
      s = s.replace(/\s*\n\s*/g, ' ')
      s = s.replace(/ {2,}/g, ' ')
      s.trim()

    safe_dirname = (name) -> name.replace(/[^a-zA-Z0-9_-]/g, '_').slice(0,50)

    save_story = (outdir, title, text) ->
      dir = path.join(outdir, safe_dirname(title))
      fs.mkdirSync dir, {recursive:true}
      fs.writeFileSync path.join(dir,'story.txt'), text, 'utf8'

    parse_markdown = (mdPath, outdir) ->
      fs.mkdirSync outdir, {recursive:true}
      lines = fs.readFileSync(mdPath,'utf8').split(/\r?\n/)
      stories = {}
      currentTitle = null; buf = []
      for line in lines
        if line.startsWith '# '
          if currentTitle
            full = buf.join('\n').trim()
            cleaned = clean_markdown_text(full)
            stories[currentTitle] = cleaned
            save_story(outdir,currentTitle,cleaned)
            buf = []
          currentTitle = line.slice(2).trim()
        else
          buf.push line.trim()
      if currentTitle and buf.length
        cleaned = clean_markdown_text(buf.join('\n'))
        stories[currentTitle] = cleaned
        save_story(outdir,currentTitle,cleaned)
      stories

    normalize_tag = (tag) ->
      t = tag.replace(/^#/,'').trim()
      t = t.replace(/(.)\1{2,}/g, '$1$1')
      t = t.replace(/[^A-Za-z0-9_-]/g, '')
      return null unless t.length>=3 and t.length<=30
      '#' + t.toLowerCase()

    save_story_tags = (outdir, title, tags) ->
      dir = path.join(outdir, safe_dirname(title))
      fs.mkdirSync dir, {recursive:true}
      fp = path.join(dir,'hashtags.json')
      fs.writeFileSync fp, JSON.stringify({title,hashtags:tags},null,2), 'utf8'

    reinforce_hashtags = (all_story_tags, top_n, min_global) ->
      global_counts = {}
      for tags in all_story_tags
        for t in tags
          global_counts[t] = (global_counts[t] or 0) + 1
      reinforced = []
      for tags in all_story_tags
        local_counts = {}
        for t in tags
          local_counts[t] = (local_counts[t] or 0) + 1
        scored = {}
        for t,v of local_counts
          g = global_counts[t] or 0
          continue if g < min_global
          scored[t] = v + 0.5*g
        ranked = Object.keys(scored).sort((a,b)->scored[b]-scored[a]).slice(0,top_n)
        reinforced.push ranked
      reinforced

    build_hashtag_index = (storiesDir, hashtagsDir) ->
      fs.mkdirSync hashtagsDir, {recursive:true}
      for storyName in fs.readdirSync(storiesDir)
        storyDir = path.join(storiesDir, storyName)
        continue unless fs.statSync(storyDir).isDirectory()
        tagFile = path.join(storyDir, 'hashtags.json')
        storyFile = path.join(storyDir, 'story.txt')
        continue unless fs.existsSync(tagFile) and fs.existsSync(storyFile)
        tags = JSON.parse(fs.readFileSync(tagFile,'utf8')).hashtags
        for tag in tags
          tagDir = path.join(hashtagsDir, tag)
          fs.mkdirSync tagDir, {recursive:true}
          linkPath = path.join(tagDir, "#{storyName}.txt")
          try fs.symlinkSync(path.resolve(storyFile), linkPath) catch err then continue

    query_llm = (storyText) ->
      prompt = PROMPT + "\n\nStory:\n" + storyText.slice(0,3000)
      args = [
        '-m','mlx_lm.generate',
        '--model', MODEL_ID,
        '--prompt', prompt,
        '--max-tokens', MAX_TOKENS,
        '--verbose','false'
      ]
      result = child.spawnSync('python3', args, {encoding:'utf8'})
      if result.status isnt 0
        log "[ERROR] mlx_lm.generate failed: #{result.stderr}"
        return []
      outLines = result.stdout.split('\n').filter((l)->l.trim().length)
      tags = []
      for l in outLines
        t = l.trim().replace(/^[\-\•# ]+/,'')
        tags.push t if t.length
      tags.slice 0, NUM_TAGS

    # ---------- Main ----------
    t0 = Date.now()
    unless fs.existsSync(INPUT_MD)
      throw new Error "Markdown input not found: #{INPUT_MD}"

    stories = parse_markdown(INPUT_MD, STORIES_OUT)
    all_story_tags = []

    for title, text of stories
      tags_raw = query_llm(text)
      cleaned = Array.from(new Set((normalize_tag(t) for t in tags_raw when t?)))
      cleaned.sort()
      save_story_tags(STORIES_OUT, title, cleaned)
      all_story_tags.push(cleaned)
      log "#{title} => #{cleaned.join(', ')}"

    reinforced = reinforce_hashtags(all_story_tags, TOP_N, MIN_GLOBAL)
    titles = Object.keys(stories)
    for i in [0...titles.length]
      title = titles[i]; tags = reinforced[i]
      save_story_tags(STORIES_OUT, title, tags)
      log "🔗 Reinforced #{title} => #{tags.join(', ')}"

    out_jsonl = path.join(OUT_DIR, 'story_hashtags.jsonl')
    outf = fs.createWriteStream(out_jsonl, 'utf8')
    for title in titles
      fp = path.join(STORIES_OUT, safe_dirname(title), 'hashtags.json')
      data = JSON.parse(fs.readFileSync(fp,'utf8'))
      outf.write(JSON.stringify(data)+'\n')
    outf.end()

    build_hashtag_index(STORIES_OUT, HASHTAGS_OUT)

    elapsed = ((Date.now()-t0)/1000).toFixed(1)
    log "✅ Oracle KAG complete in #{elapsed}s"
    log "  stories_out/: #{STORIES_OUT}"
    log "  hashtags/:   #{HASHTAGS_OUT}"
    log "  summary:     #{out_jsonl}"

    M.saveThis "done:#{stepName}", true
    M.saveThis "#{stepName}:paths", {stories:STORIES_OUT, hashtags:HASHTAGS_OUT, jsonl:out_jsonl}
    return