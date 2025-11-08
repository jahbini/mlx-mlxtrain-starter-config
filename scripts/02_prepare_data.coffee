#!/usr/bin/env coffee
###
02_prepare_data.coffee — memo-native version (final)
----------------------------------------------------
Reads contract and dataset lines directly from @memo.
Performs validation + basic stats.
Writes report via M.saveThis(), never directly to disk.
###

crypto = require 'crypto'

@step =
  desc: "Validate and analyze dataset files to produce data_report.json"

  action: (M, stepName) ->
    throw new Error "Missing stepName argument" unless stepName?

    cfg = M.theLowdown('experiment.yaml')?.value
    throw new Error "Missing experiment.yaml in memo" unless cfg?

    runCfg = cfg.run
    throw new Error "Missing global run section" unless runCfg?

    stepCfg = cfg[stepName]
    throw new Error "Missing step config for '#{stepName}'" unless stepCfg?

    CONTRACT_KEY = stepCfg.contract
    REPORT_KEY   = stepCfg.report
    contract = M.theLowdown(CONTRACT_KEY).value
    #console.log "JIM contract", CONTRACT_KEY, contract
    throw new Error "Missing contract in memo: #{CONTRACT_KEY}" unless contract?

    hashText = (s) ->
      crypto.createHash('sha256').update(String(s), 'utf8').digest('hex')

    EOS_MARKERS = ['</s>', '###', '\n\n', '<|eot_id|>', '<|endoftext|>']

    charClasses = (s) ->
      ctrl = ws = nonascii = 0
      for ch in s
        code = ch.charCodeAt(0)
        if code <= 31 or code is 127 then ctrl++
        if /\s/.test(ch) then ws++
        if code > 127 then nonascii++
      {control: ctrl, whitespace: ws, non_ascii: nonascii}

    percentiles = (vals, q=[5,25,50,75,95]) ->
      return (("p#{p}": 0) for p in q) unless vals?.length
      arr = vals.slice().sort((a,b)->a-b)
      out = {}
      for p in q
        k = Math.max(0, Math.min(arr.length-1, Math.round((p/100)*(arr.length-1))))
        out["p#{p}"] = arr[k]
      out

    scanLines = (lines, field) ->
      n_lines = bad_json = missing_field = non_str = 0
      empty = ws_only = lead_ws = trail_ws = ctrl_lines = 0
      lengths = []; hashes = []
      eos_hits = {}; eos_hits[m] = 0 for m in EOS_MARKERS
      good = []; bad = []

      for line in lines when line.trim().length
        n_lines++
        try obj = JSON.parse(line)
        catch e
          bad_json++
          if bad.length < 3 then bad.push "[bad_json] #{line.slice(0,120)}"
          continue

        unless field of obj
          missing_field++
          if bad.length < 3 then bad.push "[missing_field] #{line.slice(0,120)}"
          continue

        val = obj[field]
        unless typeof val is 'string'
          non_str++
          if bad.length < 3 then bad.push "[non_string] #{String(val).slice(0,120)}"
          continue

        if val is '' then empty++
        if val.trim() is '' then ws_only++
        if /^\s/.test(val) then lead_ws++
        if /\s$/.test(val) then trail_ws++

        cc = charClasses(val)
        if cc.control > 0 then ctrl_lines++

        L = val.length
        lengths.push L
        hashes.push hashText(val)
        for m in EOS_MARKERS when val.includes(m)
          eos_hits[m] += 1

        if good.length < 3 then good.push val

      # duplicates
      counts = {}; dup_count = 0; dup_examples = []
      for h in hashes
        counts[h] ?= 0; counts[h] += 1
      for h,cnt of counts when cnt > 1
        dup_count += cnt - 1
        if dup_examples.length < 3 then dup_examples.push h

      lens =
        count: lengths.length
        min: if lengths.length then Math.min.apply(null, lengths) else 0
        max: if lengths.length then Math.max.apply(null, lengths) else 0
        mean: if lengths.length then lengths.reduce((a,b)->a+b)/lengths.length else 0
        median: if lengths.length then lengths.slice().sort((a,b)->a-b)[Math.floor(lengths.length/2)] else 0
        percentiles: percentiles(lengths)

      {
        lines: n_lines
        valid_examples: lengths.length
        errors: {bad_json, missing_field, non_string_field: non_str}
        empties:
          empty_exact: empty
          whitespace_only: ws_only
          leading_whitespace: lead_ws
          trailing_whitespace: trail_ws
        control_char_lines: ctrl_lines
        duplicates:
          duplicate_example_count: dup_count
          sha256_examples: dup_examples
        length_chars: lens
        eos_markers_hits: eos_hits
        samples:
          good_first3: good
          bad_first3: bad
      }

    # --- main logic ---
    text_field = 'text'
    splits = {}
    #console.log "JIM contract is", contract
    for split, fileKey of contract.filenames
      data = M.theLowdown('data/'+fileKey.chosen).value
      #console.log "JIM the Data", data,fileKey
      unless data?
        throw new Error "Missing dataset for split #{split} (#{fileKey.chosen})"
      lines = data
      splits[split] = scanLines(lines, text_field)

    report =
      created_utc: new Date().toISOString().replace(/\.\d+Z$/, 'Z')
      text_field: text_field
      splits: splits

    M.saveThis REPORT_KEY, report
    M.saveThis "done:#{stepName}", true
    console.log "validated splits:", Object.keys(splits).join(', ')
    return
