# utils/safetensor_utils.coffee
# ---------------------------------------------------------------
# Minimal synchronous loader and saver for HuggingFace safetensors
# Only supports:
#   • float32 tensors (LoRA adapters)
#   • contiguous buffers
#
# Exports:
#   safeLoadTensors(path)  -> { name: Float32Array }
#   safeSaveTensors(path, map)
#
# Pipeline-safe: synchronous, stable, simple
# ---------------------------------------------------------------

fs   = require 'fs'
path = require 'path'


# ---------------------------------------------------------------
# Helper: Read uint32 LE
# ---------------------------------------------------------------
readU32 = (buf, offset) ->
  (buf[offset]) |
  (buf[offset+1] << 8) |
  (buf[offset+2] << 16) |
  (buf[offset+3] << 24)


# ---------------------------------------------------------------
# Load safetensors file synchronously
# Returns { layerName: Float32Array }
# ---------------------------------------------------------------
exports.safeLoadTensors = (absPath) ->
  raw = fs.readFileSync(absPath)
  offset = 0

  # Header length (u32)
  if raw.length < 8
    throw new Error "Invalid safetensors file (too small): #{absPath}"

  headerLen = readU32(raw, offset)
  offset += 8   # skip header_len + reserved uint32 (zeros)

  jsonStart = offset
  jsonEnd   = jsonStart + headerLen

  if jsonEnd > raw.length
    throw new Error "Invalid safetensor header length for #{absPath}"

  headerJson = raw.slice(jsonStart, jsonEnd).toString('utf8')
  header = JSON.parse(headerJson)

  tensors = {}

  # Each tensor entry looks like:
  #   name: { dtype: "F32", shape: [...], data_offsets: [start,end] }
  for own name, meta of header
    {dtype, shape, data_offsets} = meta

    unless dtype is "F32"
      throw new Error "Only F32 supported in safeLoadTensors (#{name})"

    [start, end] = data_offsets
    if end > raw.length
      throw new Error "Out-of-range tensor offsets in #{name}"

    buf = raw.slice(end - (end-start), end)
    arr = new Float32Array(buf.buffer, buf.byteOffset, buf.length / 4)

    # Clone to a clean backing store
    tensors[name] = new Float32Array(arr)

  tensors


# ---------------------------------------------------------------
# Save safetensors synchronously
# Input:
#   map[name] = Float32Array
# ---------------------------------------------------------------
exports.safeSaveTensors = (absPath, map) ->

  # Build header metadata
  header = {}
  offset = 0

  ordered = Object.keys(map).sort()

  # For now we pack tensors sequentially
  for name in ordered
    arr = map[name]
    byteLen = arr.length * 4
    header[name] =
      dtype: "F32"
      shape: [arr.length]           # LoRA adapters are flat or treat as flat
      data_offsets: [offset, offset + byteLen]
    offset += byteLen

  headerBuf = Buffer.from(JSON.stringify(header), 'utf8')
  headerLen = headerBuf.length

  # Safetensors header: 8 bytes
  out = Buffer.alloc(8 + headerLen + offset)

  # Write header length (u32) + 4 bytes zeros
  out.writeUInt32LE(headerLen, 0)
  out.writeUInt32LE(0, 4)

  # Write JSON header
  headerBuf.copy(out, 8)

  # Write tensor data sequentially
  dataStart = 8 + headerLen
  cursor    = dataStart

  for name in ordered
    arr = map[name]
    buf = Buffer.from(arr.buffer)
    buf.copy(out, cursor)
    cursor += buf.length

  # Ensure directory exists
  fs.mkdirSync(path.dirname(absPath), {recursive:true})

  # Write file
  fs.writeFileSync(absPath, out)

  true
