#!/usr/bin/env coffee
###
093_crawl_for_voice_continuation.coffee
----------------------------------------
STEP — Crawl site for voice-continuation fine-tuning

Fetches all local HTML pages under BASE,
extracts #bloviation stories, and builds multi-paragraph
(prompt, completion) pairs suitable for continuation-style training.

Outputs:
  train.jsonl / valid.jsonl
  contract / catalog / report metadata
###

fs      = require 'fs'
path    = require 'path'
os      = require 'os'
url     = require 'url'
axios   = require 'axios'
cheerio = require 'cheerio'
crypto  = require 'crypto'

process.env.NODE_NO_WARNINGS = 1

# --- Config loader ---
{load_config} = require '../config_loader'
CFG       = load_config()
STEP_NAME = process.env.STEP_NAME or 'crawl_for_voice_continuation'
STEP_CFG  = CFG.pipeline.steps?[STEP_NAME] or {}
PARAMS    = STEP_CFG.params or {}

# --- Paths & setup ---
RUN_DIR   = path.resolve PARAMS.run_dir or CFG.run.output_dir
OUT_DIR   = path.join RUN_DIR, PARAMS.output_dir or CFG.data.output_dir
LOG_DIR   = path.join OUT_DIR, 'logs'
TRAIN_JSONL = path.join OUT_DIR, 'train.jsonl'
VALID_JSONL = path.join OUT_DIR, 'valid.jsonl'
fs.mkdirSync OUT_DIR, {recursive: true}
fs.mkdirSync LOG_DIR, {recursive: true}

logPath = path.join LOG_DIR, "#{STEP_NAME}.log"
log = (msg) ->
  stamp = new Date().toISOString().replace('T',' ').replace('Z','')
  line  = "[#{stamp}] #{msg}"
  fs.appendFileSync logPath, line + os.EOL, 'utf8'
  console.log line

# --- Parameters ---
BASE      = PARAMS.base or CFG.web.base
START_URL = "https://#{BASE}/"
USER_AGENT = PARAMS.user_agent or CFG.web.user_agent or 'Mozilla/5.0'
TIMEOUT   = PARAMS.request_timeout or CFG.web.request_timeout or 15000
PAUSE_SEC = PARAMS.pause_sec or CFG.web.pause_sec or 0.5
SEED      = PARAMS.seed or CFG.run.seed or 42
VALID_FRAC= PARAMS.valid_fraction or CFG.web.valid_fraction or 0.1

# Word budgets
MIN_STORY_WORDS        = PARAMS.min_story_words        or CFG.web.min_story_words        or 50
MIN_PROMPT_WORDS       = PARAMS.min_prompt_words       or CFG.web.min_prompt_words       or 40
MAX_PROMPT_WORDS       = PARAMS.max_prompt_words       or CFG.web.max_prompt_words       or 200
MIN_COMPLETION_WORDS   = PARAMS.min_completion_words   or CFG.web.min_completion_words   or 30
MAX_COMPLETION_WORDS   = PARAMS.max_completion_words   or CFG.web.max_completion_words   or 200
MAX_EXAMPLES_PER_STORY = PARAMS.max_examples_per_story or CFG.web.max_examples_per_story or 6
MAX_PAGES              = PARAMS.max_pages or 5000

# --- Helpers ---
sleep = (ms) -> new Promise (resolve) -> setTimeout resolve, ms

normalize_ws = (s) ->
  return '' unless s?
  s.replace(/\s*\n\s*/g, ' ').replace(/ {2,}/g, ' ').trim()

demojibake = (s) ->
  map =
    '\u00c2': ''
    'â': '’'
    'â': '“'
    'â': '”'
    'â': '–'
    'â': '—'
    'â¢': '•'
    'â¦': '…'
    'â': '‘'
    'â¨': ' '
    'âª': ''
    'â«': ''
    'â¬': ''
  for k,v of map
    s = s.replace(new RegExp(k,'g'),v)
  s.replace(/[ \t]+\n/g,'\n').replace(/\n{3,}/g,'\n\n').trim()

split_paragraphs = (s) ->
  (p.trim() for p in s.split(/\n{2,}/) when p.trim().length > 0)

word_count = (s) ->
  (s.match(/\w+/g) or []).length

clip_by_words = (s, maxw) ->
  words = s.split(/\s+/)
  if words.length <= maxw then s else words.slice(0,maxw).join(' ')

is_local_html = (href) ->
  return false unless href
  href = href.split('#')[0]
  return false unless href.endsWith('.html')
  u = new url.URL(href, START_URL)
  u.hostname is (new url.URL(START_URL)).hostname

get_html = (target) ->
  axios.get(target, timeout: TIMEOUT, headers: {'User-Agent': USER_AGENT})
    .then((r)->r.data)
    .catch((e)->(log "FAIL #{target}: #{e.message}"; ''))

discover_all_html = async (start) ->
  to_visit = [start]
  visited  = new Set()
  pages = []
  while to_visit.length > 0 and pages.length < MAX_PAGES
    link = to_visit.shift()
    continue if visited.has link
    visited.add link
    html = await get_html link
    continue unless html
    pages.push [link, html]
    $ = cheerio.load html
    $('a[href]').each (_,a) ->
      href = $(a).attr('href')
      if is_local_html href
        nxt = new url.URL(href, START_URL).href
        unless visited.has(nxt) or to_visit.includes(nxt)
          to_visit.push nxt
    await sleep(PAUSE_SEC * 1000)
  pages

extract_story = (html) ->
  $ = cheerio.load html
  h2 = $('h2').eq(1)
  title = h2.text().trim() or $('title').text().trim() or 'Untitled'
  div = $('#bloviation')
  return [title,''] unless div.length
  [title, demojibake(div.text().trim())]

build_continuations = (doc_id, title, text, page_url) ->
  paras = split_paragraphs text
  return [] if paras.length < 2
  exs = []
  i = 0
  while i < paras.length - 1 and exs.length < MAX_EXAMPLES_PER_STORY
    prompt_parts = []
    w = 0; j = i
    while j < paras.length - 1 and w < MAX_PROMPT_WORDS
      w += word_count(paras[j])
      prompt_parts.push paras[j]
      j += 1
      if w >= MIN_PROMPT_WORDS then break
    break unless prompt_parts.length

    comp_parts = []
    cw = 0; k = j
    while k < paras.length and cw < MIN_COMPLETION_WORDS
      cw += word_count(paras[k])
      comp_parts.push paras[k]
      k += 1
    break unless comp_parts.length

    prompt = "Title: #{title}\n\n" + prompt_parts.join('\n\n')
    completion = comp_parts.join('\n\n')
    prompt = clip_by_words(prompt, MAX_PROMPT_WORDS + 40)
    completion = clip_by_words(completion, MAX_COMPLETION_WORDS)

    unless completion in prompt
      exs.push
        meta:
          doc_id: doc_id
          title: title
          url: page_url
        prompt: prompt
        completion: completion
    i = j
  exs

sha256_file = (p) ->
  data = fs.readFileSync p
  crypto.createHash('sha256').update(data).digest('hex')

count_lines_bytes = (p) ->
  data = fs.readFileSync p
  [data.toString().split('\n').length - 1, data.length]

summarize_lengths = (p, field) ->
  lens = []
  for ln in fs.readFileSync(p,'utf8').split('\n')
    continue unless ln.trim()
    try
      obj = JSON.parse ln
      s = obj[field]
      lens.push s.length if typeof s is 'string'
    catch then continue
  return {n:0} unless lens.length
  lens.sort (a,b)->a-b
  n = lens.length
  p95 = lens[Math.floor(0.95*(n-1))] or lens[n-1]
  {n, len_min:lens[0], len_med:lens[Math.floor(n/2)], len_95:p95, len_max:lens[n-1]}

# --- Main ---
main = ->
  log "Crawling site-local HTML: #{START_URL}"
  pages = await discover_all_html START_URL
  log "Fetched #{pages.length} pages"

  all_examples = []
  for [page_url, html] in pages
    [title, story] = extract_story html
    continue unless word_count(story) >= MIN_STORY_WORDS
    slug = path.basename(page_url).replace('.html','')
    exs = build_continuations slug, title, story, page_url
    all_examples.push ex for ex in exs

  # Deduplicate
  dedup = new Map()
  for ex in all_examples
    key = "#{ex.meta.doc_id}|#{ex.prompt.slice(0,2000)}"
    dedup.set key, ex
  examples = Array.from dedup.values()

  log "Collected #{examples.length} examples"
  examples.sort -> Math.random() - 0.5

  n_valid = Math.max 1, Math.floor(examples.length * VALID_FRAC)
  valid = examples.slice 0, n_valid
  train = examples.slice n_valid

  write_jsonl = (fname, arr) ->
    out = fs.createWriteStream fname, encoding:'utf8'
    for ex in arr
      out.write JSON.stringify(ex) + '\n'
    out.end()

  write_jsonl TRAIN_JSONL, train
  write_jsonl VALID_JSONL, valid
  log "[OK] Wrote train.jsonl (#{train.length}), valid.jsonl (#{valid.length})"

  # --- Metadata ---
  CONTRACT_PATH = path.join OUT_DIR, CFG.data?.contract or 'data_contract.json'
  CATALOG_PATH  = path.join OUT_DIR, CFG.data?.catalog  or 'data_catalog.json'
  REPORT_PATH   = path.join OUT_DIR, CFG.data?.report   or 'data_report.json'

  probeLine = fs.readFileSync(TRAIN_JSONL,'utf8').split('\n').find((l)->l.trim())
  probe = if probeLine then JSON.parse(probeLine) else {}
  if 'prompt' of probe and 'completion' of probe
    mode='sft'; target_field='completion'; schema_fields={prompt:'string',completion:'string'}
  else if 'text' of probe
    mode='plain'; target_field='text'; schema_fields={text:'string'}
  else
    log "[FATAL] Could not infer dataset schema"; process.exit 2

  created = new Date().toISOString().replace('T',' ').replace('Z','')
  [t_lines,t_bytes] = count_lines_bytes TRAIN_JSONL
  [v_lines,v_bytes] = count_lines_bytes VALID_JSONL

  contract =
    created_utc: created
    data_dir: OUT_DIR
    filenames:
      train: chosen:path.basename(TRAIN_JSONL), resolved:TRAIN_JSONL
      valid: chosen:path.basename(VALID_JSONL), resolved:VALID_JSONL
    schema:
      format:'jsonl'
      fields:schema_fields
    source:
      mode:mode
      target_field:target_field
      origin:'web_crawl'

  catalog =
    created_utc: created
    files:
      train: path:TRAIN_JSONL, lines:t_lines, bytes:t_bytes, sha256:sha256_file TRAIN_JSONL
      valid: path:VALID_JSONL, lines:v_lines, bytes:v_bytes, sha256:sha256_file VALID_JSONL
    entries:
      train: path:TRAIN_JSONL, stats:{num_valid_examples:t_lines,num_bytes:t_bytes}
      valid: path:VALID_JSONL, stats:{num_valid_examples:v_lines,num_bytes:v_bytes}

  report =
    created_utc: created
    counts: train:t_lines, valid:v_lines
    train_stats: summarize_lengths TRAIN_JSONL, target_field
    valid_stats: summarize_lengths VALID_JSONL, target_field
    target_field: target_field
    schema_mode: mode

  fs.writeFileSync CONTRACT_PATH, JSON.stringify(contract,null,2)
  fs.writeFileSync CATALOG_PATH,  JSON.stringify(catalog,null,2)
  fs.writeFileSync REPORT_PATH,   JSON.stringify(report,null,2)
  log "[INFO] Wrote contract/catalog/report to #{OUT_DIR}"
  process.exit 0

# --- Run ---
main()
