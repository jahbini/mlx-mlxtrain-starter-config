#!/usr/bin/env coffee
###
chat_jim.coffee
----------------------------------------
Chat interactively with the fine-tuned “Jim” model (Phi-3, MLX-LM).

Equivalent to the old Python version `chat_jim.py`.

Usage:
  $ coffee scripts/chat_jim.coffee
  Type a story seed → get a response in Jim's voice.

Dependencies:
  - Python environment with mlx-lm installed
  - CoffeeScript 2+
###

fs = require 'fs'
path = require 'path'
readline = require 'readline'
{spawnSync} = require 'child_process'

# -------------------------------------------------------------------
# 1) Model & Prompt Config
# -------------------------------------------------------------------
MODEL_NAME  = process.env.MODEL_NAME  or 'microsoft/Phi-3-mini-4k-instruct'
MODEL_PATH  = process.env.MODEL_PATH  or 'phi3-mlx/model.safetensors'
MAX_TOKENS  = parseInt(process.env.MAX_TOKENS or '512', 10)

PROMPT_TEMPLATE = """
You are St. John's Jim, a myth-weaving, bar-stool Buddha of the Pacific Northwest.
Tell a new short story in your usual voice. Base it on this seed:
"""

# -------------------------------------------------------------------
# 2) Terminal setup
# -------------------------------------------------------------------
rl = readline.createInterface
  input: process.stdin
  output: process.stdout
  historySize: 50
  prompt: 'Seed > '

console.log "\n🌀 Chatting with the Jim-tuned model."
console.log "Type your story seed and press Enter. Ctrl+C to exit.\n"
rl.prompt()

# -------------------------------------------------------------------
# 3) MLX-LM bridge
# -------------------------------------------------------------------
generate_text = (prompt) ->
  args = [
    '-m', 'mlx_lm.generate'
    '--model', MODEL_NAME
    '--weights', MODEL_PATH
    '--prompt', prompt
    '--max-tokens', MAX_TOKENS
    '--verbose', 'false'
  ]

  result = spawnSync 'python3', args, encoding: 'utf8'
  if result.status isnt 0
    console.error "[ERROR] mlx_lm.generate failed:"
    console.error result.stderr
    return '(error generating text)'

  # Sometimes MLX prints logs, so take the last non-empty line
  lines = result.stdout.split('\n').filter (l) -> l.trim().length > 0
  return lines[lines.length - 1] or '(no output)'

# -------------------------------------------------------------------
# 4) Interactive Loop
# -------------------------------------------------------------------
rl.on 'line', (input) ->
  user_input = input.trim()
  if not user_input
    rl.prompt()
    return

  full_prompt = PROMPT_TEMPLATE + user_input + '\n'
  console.log "\n⏳ Thinking...\n"

  output = generate_text full_prompt
  console.log "📘 Jim says:\n" + output + "\n"

  rl.prompt()

rl.on 'SIGINT', ->
  console.log "\n👋 Goodbye!"
  process.exit 0