#!/usr/bin/env coffee
###
04_register.coffee — memo-native checkpoint
-------------------------------------------
Confirms that experiments.csv exists, validates headers,
and memoizes a canonical pointer (run:experiments_csv).
###

fs = require 'fs'
path = require 'path'
crypto = require 'crypto'

@step =
  desc: "Register experiments.csv and record pipeline lock hash"

  action: (M, stepName) ->
    throw new Error "Missing stepName" unless stepName?
    cfg = M.theLowdown('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    runCfg = cfg.run
    stepCfg = cfg[stepName]
    throw new Error "Missing run section" unless runCfg?

    EXP_CSV = runCfg.experiments_csv
    throw new Error "Missing run.experiments_csv" unless EXP_CSV?

    csv = M.theLowdown(EXP_CSV)?.value ? fs.readFileSync(EXP_CSV, 'utf8')
    throw new Error "experiments.csv missing in memo and on disk" unless csv?

    lines = csv.trim().split(/\r?\n/)
    headers = lines[0]?.split(',')
    throw new Error "Invalid experiments.csv (no header)" unless headers?.length

    # Compute a stable hash for pipeline lock
    hash = crypto.createHash('sha1').update(csv, 'utf8').digest('hex')
    M.saveThis "lock_hash", hash
    M.saveThis "register:experiments_csv", EXP_CSV
    M.saveThis "done:#{stepName}", true

    console.log "Registered #{EXP_CSV} (#{lines.length - 1} row(s))"
    console.log "lock_hash =", hash
    return
