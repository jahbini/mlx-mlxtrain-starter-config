###
  022_prepare_prompts.coffee
  --------------------------
  Direct CoffeeScript port of 022_prepare_prompts.py
  ✅ Ready for future declarative integration via M.memo pipeline.

  Function:
    - Loads dataset contract
    - Samples a few training examples
    - Applies a named prompt-formatting template
    - Prints before/after preview
    - Writes formatting policy JSON for downstream steps
###

fs    = require 'fs'
path  = require 'path'
yaml  = require 'js-yaml'
textwrap = require 'textwrap'

# --- STEP-AWARE CONFIG ---
CFG_PATH  = process.env.CFG_OVERRIDE or path.join(process.cwd(), 'experiment.yaml')
STEP_NAME = process.env.STEP_NAME or 'prepare_prompts'
cfgFull   = yaml.load(fs.readFileSync(CFG_PATH, 'utf8'))
STEP_CFG  = cfgFull[STEP_NAME] or {}
RUN_CFG   = cfgFull['run'] or {}

RUN_DIR   = path.resolve(RUN_CFG.data_dir or 'data')
fs.mkdirSync(RUN_DIR, {recursive:true})
CONTRACT  = path.join(RUN_DIR, RUN_CFG.contract or 'data_contract.json')
POLICY    = path.join(RUN_DIR, RUN_CFG.policy or 'prompt_policy.json')

# --- Parameters ---
TEMPLATE_NAME = STEP_CFG.template_name or 'plain_text_passthrough'
STOP_STRINGS  = STEP_CFG.stop_strings or []
USE_EOS_TOKEN = STEP_CFG.use_eos_token or false

# --- Helpers ---
readJSON = (p) -> JSON.parse(fs.readFileSync(p, 'utf8'))

loadContract = (p) ->
  c = readJSON(p)
  dataDir = path.resolve(c.data_dir)
  files = {}
  for k,v of c.filenames when v.resolved?
    files[k] = v.resolved
  fields = c.schema?.fields or {}
  textField = Object.keys(fields).find((k)-> String(fields[k]).toLowerCase() is 'string') or 'text'
  {dataDir, files, textField}

readFirstNTexts = (p, n=3, field='text') ->
  out = []
  lines = fs.readFileSync(p, 'utf8').split(/\r?\n/)
  for line in lines
    break if out.length >= n
    try
      obj = JSON.parse(line)
    catch e
      continue
    val = obj[field]
    if typeof val is 'string'
      out.push(val)
  out

# --- Load contract + samples ---
{dataDir, files, textField} = loadContract(CONTRACT)
trainPath = files.train or path.join(dataDir, 'train.jsonl')
samples = readFirstNTexts(trainPath, 3, textField)

# --- Formatters ---
fmtPlain = (text) -> text

fmtIclMinimal = (text) ->
  "### Instruction\nShare an important thought.\n\n### Response\n" + text.trim()

fmtLlama3Style = (text) ->
  "<s>[INSTRUCTION]\nShare an important thought.\n[/INSTRUCTION]\n[RESPONSE]\n" + text.trim() + "\n[/RESPONSE]</s>"

FORMATTERS =
  plain_text_passthrough: fmtPlain
  icl_minimal: fmtIclMinimal
  llama3_style: fmtLlama3Style

unless FORMATTERS[TEMPLATE_NAME]?
  console.error "Unknown TEMPLATE_NAME: #{TEMPLATE_NAME}"
  process.exit(1)

formatter = FORMATTERS[TEMPLATE_NAME]

# --- Preview ---
console.log "=== FORMAT PREVIEW ==="
console.log "Template: #{TEMPLATE_NAME}"
for i in [0...samples.length]
  txt = samples[i]
  before = txt.replace(/\n/g, ' \\n ')
  after  = formatter(txt).replace(/\n/g, ' \\n ')
  console.log "\n--- Example #{i+1}: BEFORE ---"
  console.log before.slice(0,220) + (if before.length>220 then '…' else '')
  console.log "--- Example #{i+1}: AFTER  ---"
  console.log after.slice(0,220) + (if after.length>220 then '…' else '')

# --- Persist policy ---
policy =
  template_name: TEMPLATE_NAME
  text_field: textField
  stop_strings: STOP_STRINGS
  use_eos_token: USE_EOS_TOKEN
  notes: [
    "This policy describes how to *format* examples when generating or materializing new data."
    "Your current JSONL will not be changed by this step."
    "Downstream steps can choose to apply this formatter or keep passthrough depending on the experiment."
  ]
  preview: for i in [0...Math.min(2, samples.length)]
    before: samples[i]
    after: formatter(samples[i])

fs.writeFileSync(POLICY, JSON.stringify(policy, null, 2), 'utf8')
console.log "\nWrote #{POLICY}"

# Optional memo save
try
  if global.M? and typeof global.M.saveThis is 'function'
    global.M.saveThis 'prepare_prompts:policy', policy
catch e
  console.warn "(memo skip)", e.message