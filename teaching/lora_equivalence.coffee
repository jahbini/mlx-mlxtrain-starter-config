###
#  teaching/lora_equivalence_16x16.coffee
#
#  A pipeline-compatible teaching step.
#  Verifies:
#     JS-toy LoRA  ==  MLX LoRA
#
#  This script is designed SPECIFICALLY for override.yaml-based
#  teaching/testing. It has:
#    - zero mainline config dependencies
#    - a single exported entrypoint function
#    - your explicit-debug CoffeeScript style
#
#  The pipeline will call:
#      run(context)
#
#  Where:
#      context.params.matrix_dim
#      context.params.rank
#      context.params.scale
#      context.params.verify_equivalence
###
# ------------------------------------------------------------

import * as mx from "mlx/core"
import * as nn from "mlx/nn"

# ------------------------------------------------------------
#  Deterministic JS RNG (simple LCG)
# ------------------------------------------------------------

makeRNG = (seed) ->
  state = seed
  next = ->
    state = (state * 16807) % 2147483647
    return (state - 1) / 2147483646
  return next

# ------------------------------------------------------------
#  Generate JS matrices/vectors
# ------------------------------------------------------------

makeMatrix = (rows, cols, rng) ->
  out = []
  for r in [0...rows]
    row = []
    for c in [0...cols]
      row.push rng() * 2.0 - 1.0
    out.push row
  return out

makeVector = (n, rng) ->
  v = []
  for i in [0...n]
    v.push rng() * 2.0 - 1.0
  return v

# ------------------------------------------------------------
#  Toy LoRA math (pure JS loops)
# ------------------------------------------------------------

toyMatVec = (M, v) ->
  rows = M.length
  cols = v.length
  out = new Array(rows)
  for r in [0...rows]
    sum = 0.0
    for c in [0...cols]
      sum += M[r][c] * v[c]
    out[r] = sum
  return out

toyScale = (v, s) ->
  out = new Array(v.length)
  for i in [0...v.length]
    out[i] = v[i] * s
  return out

toyAdd = (a, b) ->
  out = new Array(a.length)
  for i in [0...a.length]
    out[i] = a[i] + b[i]
  return out

toyLoRA = (W, A, B, x, scale) ->
  Wx   = toyMatVec W, x
  Bx   = toyMatVec B, x
  AxBx = toyMatVec A, Bx
  scaled = toyScale AxBx, scale
  y = toyAdd Wx, scaled

  return { Wx, Bx, AxBx, y }

# ------------------------------------------------------------
#  MLX LoRA forward
# ------------------------------------------------------------

mlxLoRA = (Wm, Am, Bm, xm, scale) ->
  Wx   = mx.matmul Wm, xm
  Bx   = mx.matmul Bm, xm
  AxBx = mx.matmul Am, Bx
  y    = Wx + scale * AxBx
  return { Wx, Bx, AxBx, y }

# ------------------------------------------------------------
#  Pipeline entrypoint
# ------------------------------------------------------------

exports.run = (context) ->

  # ---- Extract parameters or defaults ----
  params = context.params ? {}

  let matrixDim = params.matrix_dim ? 16
  let rank      = params.rank        ? 4
  let scale     = params.scale       ? 0.8
  let seed      = params.seed        ? 12345

  let verify    = params.verify_equivalence ? true

  console.log "=== Teaching LoRA Equivalence Check ==="
  console.log "Dim: #{matrixDim}x#{matrixDim}, Rank: #{rank}, Scale: #{scale}"

  # ---- Build deterministic JS matrices ----
  rng = makeRNG seed

  W_js = makeMatrix matrixDim, matrixDim, rng
  A_js = makeMatrix matrixDim, rank,      rng
  B_js = makeMatrix rank,      matrixDim, rng
  x_js = makeVector matrixDim, rng

  # ---- Toy forward ----
  toy = toyLoRA W_js, A_js, B_js, x_js, scale

  # ---- MLX tensors ----
  W_m = mx.array W_js
  A_m = mx.array A_js
  B_m = mx.array B_js
  x_m = mx.array([x_js]).T    # column shape (dim,1)

  mlx = mlxLoRA W_m, A_m, B_m, x_m, scale

  # force MLX compute
  mx.eval mlx.y, mlx.Wx, mlx.Bx, mlx.AxBx

  # flatten MLX result
  y_mlx = mlx.y.toArray().map (row) -> row[0]

  # ---- Verification ----
  if verify
    diff = []
    maxAbs = 0.0
    for i in [0...matrixDim]
      d = y_mlx[i] - toy.y[i]
      diff.push d
      if Math.abs(d) > maxAbs then maxAbs = Math.abs(d)

    console.log "Max |diff| = #{maxAbs}"

    if maxAbs < 1e-6
      console.log ">>> SUCCESS: Toy LoRA == MLX LoRA"
    else
      console.log ">>> WARNING: mismatch detected!"

    # show a slice for sanity
    console.log "Toy y[0..7]: ", toy.y.slice(0,8)
    console.log "MLX y[0..7]: ", y_mlx.slice(0,8)
    console.log "Diff[0..7]:  ", diff.slice(0,8)

  # return values so the pipeline/MM can inspect or save if needed
  return {
    toy_y: toy.y
    mlx_y: y_mlx
    ok: verify and (maxAbs < 1e-6)
  }
