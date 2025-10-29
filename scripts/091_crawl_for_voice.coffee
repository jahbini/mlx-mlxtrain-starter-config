#!/usr/bin/env coffee
###
081_crawl_for_voice.coffee
----------------------------------------
STEP — Crawl site for stories and prepare voice dataset

Fetches all local HTML pages under BASE domain,
extracts text from #bloviation divs,
splits into paragraphs, and produces train/valid JSONL datasets
for voice-style fine-tuning (story summarization & tone capture).

Config example (in experiment.yaml):

  crawl_for_voice:
    run: scripts/081_crawl_for_voice.coffee
    base: celarien.com
    output_dir: run/data/voice
    valid_fraction: 0.1
    min_story_words: 50
###

fs      = require 'fs'
path    = require 'path'
os      = require 'os'
url     = require 'url'
axios   = require 'axios'
cheerio = require 'cheerio'
yaml    = require 'js-yaml'

process.env.NODE_NO_WARNINGS = 1

# -------------------------------------------------------------------
# 1) Load step-aware configuration
# -------------------------------------------------------------------
STEP_NAME = process.env.STEP_NAME or 'crawl_for_voice'
CFG_PATH  = process.env.CFG_OVERRIDE or path.join process.cwd(), 'experiment.yaml'

try
  CFG_FULL = yaml.load fs.readFileSync(CFG_PATH, 'utf8')
catch err
  console.error "⚠️ Could not load #{CFG_PATH}: #{err.message}"
  CFG_FULL = {}

RUN_CFG  = CFG_FULL?.run or {}
STEP_CFG = CFG_FULL?[STEP_NAME] or {}
PARAMS   = STEP_CFG?.params or {}

# Paths
RUN_DIR   = path.resolve PARAMS.run_dir or RUN_CFG.output_dir or 'run'
OUT_DIR   = path.join RUN_DIR, PARAMS.output_dir or 'data/voice'
LOG_DIR   = path.join OUT_DIR, 'logs'
TRAIN_JSONL = path.join OUT_DIR, 'train.jsonl'
VALID_JSONL = path.join OUT_DIR, 'valid.jsonl'

fs.mkdirSync OUT_DIR, {recursive:true}
fs.mkdirSync LOG_DIR, {recursive:true}

# -------------------------------------------------------------------
# 2) Logging utility
# -------------------------------------------------------------------
LOG_PATH = path.join LOG_DIR, "#{STEP_NAME}.log"
log = (msg) ->
  stamp = new Date().toISOString().replace('T',' ').replace('Z','')
  line  = "[#{stamp}] #{msg}"
  try fs.appendFileSync LOG_PATH, line + os.EOL, 'utf8' catch e then null
  console.log line

# -------------------------------------------------------------------
# 3) Parameters
# -------------------------------------------------------------------
BASE      = PARAMS.base or RUN_CFG.base or 'localhost'
START_URL = "https://#{BASE}/"
USER_AGENT = PARAMS.user_agent or 'Mozilla/5.0'
REQUEST_TIMEOUT = PARAMS.request_timeout or 15000
PAUSE_SEC  = PARAMS.pause_sec or 0.5
VALID_FRAC = PARAMS.valid_fraction or 0.1
MIN_WORDS  = PARAMS.min_story_words or 50
MAX_PAGES  = PARAMS.max_pages or 1000

# -------------------------------------------------------------------
# 4) Helpers
# -------------------------------------------------------------------
sleep = (ms) -> new Promise (resolve) -> setTimeout resolve, ms

normalize_ws = (s) ->
  return '' unless s?
  s.replace(/\s*\n\s*/g, ' ').replace(/ {2,}/g, ' ').trim()

demojibake = (s) ->
  replacements =
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
  for k,v of replacements
    s = s.replace(new RegExp(k,'g'), v)
  s.replace(/[ \t]+\n/g, '\n').replace(/\n{3,}/g, '\n\n').trim()

split_paragraphs = (txt) ->
  (p.trim() for p in txt.split(/\n{2,}/) when p.trim().length > 0)

ordinal_suffix = (n) ->
  if 10 <= n % 100 <= 20 then 'th' else {1:'st',2:'nd',3:'rd'}[n % 10] or 'th'

# -------------------------------------------------------------------
# 5) Crawler
# -------------------------------------------------------------------
is_local_html = (href) ->
  return false unless href
  href = href.split('#')[0]
  return false unless href.endsWith('.html')
  u = new url.URL(href, START_URL)
  u.hostname is (new url.URL(START_URL)).hostname

get_html = (target) ->
  axios.get(target, timeout: REQUEST_TIMEOUT, headers: {'User-Agent': USER_AGENT})
    .then((r) -> r.data)
    .catch((e) -> log "⚠️ FAIL: #{target} (#{e.message})"; '')

discover_all_html = async (start_url) ->
  to_visit = [start_url]
  visited  = new Set()
  pages    = []
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
        next_url = new url.URL(href, START_URL).href
        if not visited.has(next_url) and not to_visit.includes(next_url)
          to_visit.push next_url
    await sleep PAUSE_SEC * 1000
  pages

extract_story = (html) ->
  $ = cheerio.load html
  title = $('h2').eq(1).text().trim() or $('title').text().trim() or 'Untitled'
  div = $('#bloviation')
  return [title, ''] unless div.length
  text = demojibake div.text().trim()
  [title, text]

# -------------------------------------------------------------------
# 6) Main
# -------------------------------------------------------------------
main = ->
  log "🌐 Crawling site: #{START_URL}"
  pages = await discover_all_html START_URL
  log "Fetched #{pages.length} pages"

  all_examples = []
  for [page_url, html] in pages
    [title, story] = extract_story html
    continue unless story.split(/\s+/).length >= MIN_WORDS
    slug = path.basename(page_url).replace('.html','')
    paragraphs = split_paragraphs story
    for i,para of paragraphs
      n = parseInt(i) + 1
      prompt = "This is the #{n}#{ordinal_suffix(n)} paragraph from the story \"#{title}\". Please summarize it and note its tone and rhythm."
      all_examples.push
        meta:
          doc_id: slug
          title: title
          url: page_url
          paragraph_index: n
        prompt: prompt
        completion: ""

  # Split train/valid
  all_examples.sort -> Math.random() - 0.5
  n_valid = Math.max 1, Math.floor(all_examples.length * VALID_FRAC)
  valid = all_examples.slice 0, n_valid
  train = all_examples.slice n_valid

  write_jsonl = (fn, arr) ->
    out = fs.createWriteStream fn, encoding:'utf8'
    for ex in arr
      out.write JSON.stringify(ex) + '\n'
    out.end()

  write_jsonl TRAIN_JSONL, train
  write_jsonl VALID_JSONL, valid
  log "[OK] Wrote #{train.length} → #{TRAIN_JSONL}"
  log "[OK] Wrote #{valid.length} → #{VALID_JSONL}"

  try
    if global.M? and typeof global.M.saveThis is 'function'
      global.M.saveThis "done:#{STEP_NAME}", true
      global.M.saveThis "#{STEP_NAME}:counts", {train: train.length, valid: valid.length}
  catch e
    log "(memo skip) #{e.message}"

  process.exit 0

main()