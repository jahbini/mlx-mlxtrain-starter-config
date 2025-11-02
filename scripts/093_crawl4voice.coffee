#!/usr/bin/env coffee
###
093_crawl_for_voice_continuation.coffee — strict memo-aware (2025)
------------------------------------------------------------------
STEP — Crawl site for voice-continuation fine-tuning

Reads:
  Local HTML pages under base domain, extracts #bloviation stories,
  builds (prompt, completion) pairs for continuation-style SFT.

Writes:
  - train.jsonl / valid.jsonl
  - <run.contract> / <run.report> / <run.catalog>

Requires (experiment.yaml):
  run:
    data_dir: ...
    contract: contract.json
    report:   report.json
    catalog:  catalog.json
  crawl_for_voice_continuation:
    base: celarien.com
    valid_fraction: 0.1
    min_story_words: 50
    min_prompt_words: 40
    max_prompt_words: 200
    min_completion_words: 30
    max_completion_words: 200
    max_examples_per_story: 6
    pause_sec: 0.5
    request_timeout: 15000
    user_agent: Mozilla/5.0
###

fs      = require 'fs'
path    = require 'path'
os      = require 'os'
url     = require 'url'
axios   = require 'axios'
cheerio = require 'cheerio'
crypto  = require 'crypto'

@step =
  desc: "Crawl site and build continuation-style train/valid datasets (strict memo-aware)"

  action: async (M, stepName) ->
    throw new Error "Missing stepName argument" unless stepName?
    cfg = M?.theLowdown?('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    runCfg  = cfg['run']
    stepCfg = cfg[stepName]
    throw new Error "Missing 'run' section" unless runCfg?
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?

    for k in ['data_dir','contract','report','catalog']
      throw new Error "Missing required run.#{k}" unless k of runCfg

    required = [
      'base','valid_fraction','min_story_words',
      'min_prompt_words','max_prompt_words',
      'min_completion_words','max_completion_words',
      'max_examples_per_story','pause_sec',
      'request_timeout','user_agent'
    ]
    for k in required
      throw new Error "Missing required #{stepName}.#{k}" unless k of stepCfg

    DATA_DIR  = path.resolve(runCfg.data_dir)
    CONTRACT  = path.join(DATA_DIR, runCfg.contract)
    REPORT    = path.join(DATA_DIR, runCfg.report)
    CATALOG   = path.join(DATA_DIR, runCfg.catalog)

    fs.mkdirSync(DATA_DIR, {recursive:true})
    LOG_DIR = path.join(DATA_DIR, 'logs')
    fs.mkdirSync(LOG_DIR, {recursive:true})

    LOG_PATH = path.join(LOG_DIR, "#{stepName}.log")
    log = (msg) ->
      stamp = new Date().toISOString().replace('T',' ').replace(/\.\d+Z$/,'')
      line  = "[#{stamp}] #{msg}"
      try fs.appendFileSync(LOG_PATH, line + os.EOL, 'utf8') catch e then null
      console.log line

    BASE      = stepCfg.base
    VALID_FRAC= Number(stepCfg.valid_fraction)
    MIN_STORY = parseInt(stepCfg.min_story_words)
    MIN_PROMPT= parseInt(stepCfg.min_prompt_words)
    MAX_PROMPT= parseInt(stepCfg.max_prompt_words)
    MIN_COMP  = parseInt(stepCfg.min_completion_words)
    MAX_COMP  = parseInt(stepCfg.max_completion_words)
    MAX_PER   = parseInt(stepCfg.max_examples_per_story)
    PAUSE_SEC = Number(stepCfg.pause_sec)
    TIMEOUT   = parseInt(stepCfg.request_timeout)
    USER_AGENT= stepCfg.user_agent

    START_URL = "https://#{BASE}/"

    for [v,name] in [[VALID_FRAC,'valid_fraction'],[MIN_STORY,'min_story_words']]
      unless v? and v>0 then throw new Error "Invalid #{stepName}.#{name}"

    TRAIN_JSONL = path.join(DATA_DIR, 'train.jsonl')
    VALID_JSONL = path.join(DATA_DIR, 'valid.jsonl')

    # ---------- Helpers ----------
    sleep = (ms) -> new Promise (res) -> setTimeout res, ms
    word_count = (s) -> (s.match(/\w+/g) or []).length
    clip_by_words = (s,maxw) ->
      w = s.split(/\s+/)
      if w.length <= maxw then s else w.slice(0,maxw).join(' ')
    split_paragraphs = (s) ->
      (p.trim() for p in s.split(/\n{2,}/) when p.trim().length>0)

    demojibake = (s) ->
      repl =
        '\u00c2':'','â':'’','â':'“','â':'”','â':'–','â':'—',
        'â¢':'•','â¦':'…','â':'‘'
      for k,v of repl then s = s.replace(new RegExp(k,'g'),v)
      s.replace(/\s+\n/g,'\n').replace(/\n{3,}/g,'\n\n').trim()

    is_local_html = (href) ->
      return false unless href
      href = href.split('#')[0]
      return false unless href.endsWith('.html')
      u = new url.URL(href, START_URL)
      u.hostname is (new url.URL(START_URL)).hostname

    get_html = (target) ->
      axios.get(target, timeout: TIMEOUT, headers:{'User-Agent':USER_AGENT})
        .then((r)->r.data)
        .catch((e)->(log "⚠️ FAIL: #{target} (#{e.message})"; ''))

    discover_all_html = async (start) ->
      to_visit=[start]; visited=new Set(); pages=[]
      while to_visit.length>0 and pages.length<1000
        link = to_visit.shift()
        continue if visited.has(link)
        visited.add(link)
        html = await get_html(link)
        continue unless html
        pages.push [link,html]
        $ = cheerio.load html
        $('a[href]').each (_,a) ->
          href = $(a).attr('href')
          if is_local_html href
            nxt = new url.URL(href, START_URL).href
            unless visited.has(nxt) or to_visit.includes(nxt)
              to_visit.push nxt
        await sleep(PAUSE_SEC*1000)
      pages

    extract_story = (html) ->
      $ = cheerio.load html
      title = $('h2').eq(1).text().trim() or $('title').text().trim() or 'Untitled'
      div = $('#bloviation')
      return [title,''] unless div.length
      [title, demojibake(div.text().trim())]

    build_continuations = (doc_id,title,text,page_url) ->
      paras = split_paragraphs text
      return [] if paras.length<2
      exs=[]
      i=0
      while i<paras.length-1 and exs.length<MAX_PER
        prompt_parts=[]; w=0; j=i
        while j<paras.length-1 and w<MAX_PROMPT
          w+=word_count(paras[j])
          prompt_parts.push paras[j]
          j+=1
          if w>=MIN_PROMPT then break
        break unless prompt_parts.length
        comp_parts=[]; cw=0; k=j
        while k<paras.length and cw<MIN_COMP
          cw+=word_count(paras[k])
          comp_parts.push paras[k]; k+=1
        break unless comp_parts.length
        prompt="Title: #{title}\n\n"+prompt_parts.join('\n\n')
        completion=comp_parts.join('\n\n')
        prompt=clip_by_words(prompt,MAX_PROMPT+40)
        completion=clip_by_words(completion,MAX_COMP)
        unless completion in prompt
          exs.push
            meta:{doc_id,title,url:page_url}
            prompt:prompt
            completion:completion
        i=j
      exs

    sha256_file = (p) ->
      crypto.createHash('sha256').update(fs.readFileSync(p)).digest('hex')
    count_lines_bytes = (p) ->
      data=fs.readFileSync(p)
      [String(data).split('\n').filter((l)->l.trim().length).length,data.length]
    summarize_lengths = (p, field) ->
      lens=[]
      for ln in fs.readFileSync(p,'utf8').split('\n') when ln.trim()
        try
          obj=JSON.parse ln
          s=obj[field]; lens.push s.length if typeof s is 'string'
        catch then null
      return {n:0} unless lens.length
      lens.sort((a,b)->a-b); n=lens.length
      p95=lens[Math.floor(0.95*(n-1))] or lens[n-1]
      {n, len_min:lens[0], len_med:lens[Math.floor(n/2)], len_95:p95, len_max:lens[n-1]}

    # ---------- Main ----------
    log "🌐 Crawling #{START_URL}"
    pages = await discover_all_html START_URL
    log "Fetched #{pages.length} pages"

    all_examples=[]
    for [page_url,html] in pages
      [title,story]=extract_story html
      continue unless word_count(story)>=MIN_STORY
      slug=path.basename(page_url).replace('.html','')
      for ex in build_continuations(slug,title,story,page_url)
        all_examples.push ex

    dedup=new Map()
    for ex in all_examples
      key="#{ex.meta.doc_id}|#{ex.prompt.slice(0,1000)}"
      dedup.set key, ex
    examples=Array.from(dedup.values())
    log "Collected #{examples.length} examples"

    # shuffle (simple random)
    examples.sort -> Math.random() - 0.5
    n_valid=Math.max 1, Math.floor(examples.length*VALID_FRAC)
    valid=examples.slice 0,n_valid
    train=examples.slice n_valid

    write_jsonl=(fn,arr)->
      out=fs.createWriteStream(fn,encoding:'utf8')
      for ex in arr
        out.write(JSON.stringify(ex)+'\n')
      out.end()

    write_jsonl TRAIN_JSONL, train
    write_jsonl VALID_JSONL, valid
    log "[OK] Wrote #{train.length} train, #{valid.length} valid"

    created=new Date().toISOString().replace(/\.\d+Z$/,'Z')
    [t_lines,t_bytes]=count_lines_bytes TRAIN_JSONL
    [v_lines,v_bytes]=count_lines_bytes VALID_JSONL

    schema_fields={prompt:'string',completion:'string'}

    contract=
      created_utc:created
      data_dir:DATA_DIR
      filenames:
        train:{chosen:path.basename(TRAIN_JSONL),resolved:TRAIN_JSONL}
        valid:{chosen:path.basename(VALID_JSONL),resolved:VALID_JSONL}
      schema:{format:'jsonl',fields:schema_fields}
      source:{mode:'sft',target_field:'completion',origin:'web_crawl'}

    report=
      created_utc:created
      counts:{train:t_lines,valid:v_lines}
      train_stats:summarize_lengths(TRAIN_JSONL,'completion')
      valid_stats:summarize_lengths(VALID_JSONL,'completion')
      target_field:'completion'
      schema_mode:'sft'

    catalog=
      created_utc:created
      data_dir:DATA_DIR
      mode:'sft'
      target_field:'completion'
      schema:schema_fields
      total_examples:{train:t_lines,valid:v_lines}
      files:
        train:{path:TRAIN_JSONL,lines:t_lines,bytes:t_bytes,sha256:sha256_file(TRAIN_JSONL)}
        valid:{path:VALID_JSONL,lines:v_lines,bytes:v_bytes,sha256:sha256_file(VALID_JSONL)}
      checksums:
        contract:sha256_file(CONTRACT)
        report:sha256_file(REPORT)

    fs.writeFileSync(CONTRACT,JSON.stringify(contract,null,2),'utf8')
    fs.writeFileSync(REPORT,JSON.stringify(report,null,2),'utf8')
    fs.writeFileSync(CATALOG,JSON.stringify(catalog,null,2),'utf8')
    log "[OK] Wrote contract/report/catalog"

    M.saveThis "done:#{stepName}", true
    M.saveThis "#{stepName}:counts", {train:t_lines, valid:v_lines}
    return {train:t_lines, valid:v_lines}