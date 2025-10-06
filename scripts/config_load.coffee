###
Config Loader (CoffeeScript)
----------------------------

Loads pipeline configuration from:
  1. default.config (YAML or JSON)
  2. override.yaml (or override.json) in the working directory

Returns a merged config object:
  - Values in override replace values in default.
  - EXEC and PWD are injected from environment variables.

Usage:
  {load_config} = require './config_loader'
  cfg = load_config()
###

fs     = require 'fs'
path   = require 'path'
yaml   = require 'js-yaml'

# --- helper: deep merge ---
deepMerge = (target, source) ->
  for own key, val of source
    if val? and typeof val is 'object' and not Array.isArray val
      target[key] = deepMerge target[key] or {}, val
    else
      target[key] = val
  target

# --- load file (YAML or JSON) ---
loadFile = (p) ->
  return {} unless fs.existsSync p
  ext = path.extname p
  raw = fs.readFileSync p, 'utf8'
  try
    if ext in ['.yaml', '.yml']
      return yaml.load raw
    else if ext is '.json' or ext is '.config'
      return JSON.parse raw
    else
      # try YAML first, then JSON
      try yaml.load raw catch e then JSON.parse raw
  catch err
    console.error "[FATAL] Could not parse config file: #{p}", err
    process.exit 1

# --- main loader ---
exports.load_config = ->
  execDir = process.env.EXEC or process.cwd()
  pwdDir  = process.env.PWD  or process.cwd()

  defaultPath = path.join execDir, 'config/default.yaml'
  overrideYaml = path.join pwdDir, 'override.yaml'
  overrideJson = path.join pwdDir, 'override.json'

  base = loadFile defaultPath
  override = {}
  override = loadFile overrideYaml if fs.existsSync overrideYaml
  override = loadFile overrideJson if fs.existsSync overrideJson and Object.keys(override).length is 0

  merged = deepMerge {}, base
  merged = deepMerge merged, override

  # inject runtime paths
  merged.run ?= {}
  merged.run.exec_dir = execDir
  merged.run.pwd_dir  = pwdDir

  return merged
