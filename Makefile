PY    ?= python

# Config-driven names (from default.yaml)
RUN_DIR     = run
ARTIFACTS   = $(RUN_DIR)/artifacts.json
EXPERIMENTS = $(RUN_DIR)/experiments.csv
DATA_DIR    = run/data
EVAL_DIR    = eval_out

CONTRACT    = $(DATA_DIR)/data_contract.json
GEN_BASE    = $(EVAL_DIR)/generations
GEN_JSONL   = $(GEN_BASE).jsonl
GEN_CSV     = $(GEN_BASE).csv
SUMMARY     = $(EVAL_DIR)/eos_summary.csv
ANALYSIS    = $(EVAL_DIR)/eos_analysis.json
ABLATIONS   = $(EVAL_DIR)/ablation_generations.jsonl
REPORT      = $(EVAL_DIR)/report.md

# -------------------------------------------------------------------
# 0) Manifest
manifest:
	$(PY) scripts/00_manifest.py
	mkdir $(DATA_DIR)

# 2) Fetch HF dataset
fetch-hf: $(DATA_DIR)
	$(PY) scripts/01_fetch_hf_dataset.py

# 3) Prepare data
prepare: $(CONTRACT)
	$(PY) scripts/02_prepare_data.py

# 3a) Prepare prompts
prepare-prompts: $(CONTRACT)
	$(PY) scripts/022_prepare_prompts.py

# 3b) Prepare experiments
prepare-experiments: $(CONTRACT)
	$(PY) scripts/023_prepare_experiments.py

# 3c) Register run  creates ARTIFACTS
register: $(CONTRACT)
	$(PY) scripts/031_register.py

# 3d) Fuse model (optional)
fuse: $(ARTIFACTS)
	$(PY) scripts/032_fuse.py

# 4) Train
train: $(CONTRACT)
	$(PY) scripts/03_train.py

# 5) Eval (metrics, sanity)
eval: $(ARTIFACTS) $(CONTRACT)
	$(PY) scripts/05_eval.py

# 4.1 snapshot (deterministic generations)
snapshot: $(ARTIFACTS) $(CONTRACT)
	$(PY) scripts/04_snapshot.py

# 4.1a) metrics on snapshot
metrics: $(GEN_JSONL) $(CONTRACT)
	$(PY) scripts/041_metrics.py

# 4.2 sanity checks
sanity: $(ARTIFACTS)
	$(PY) scripts/042_sanity.py

# 9) Alternative crawler (voice data)
crawl-voice:
	$(PY) scripts/09_crawl4voice.py

# REPL (manual check)
repl: $(ARTIFACTS)
	$(PY) scripts/repl.py

# Convenience groups
data: fetch-hf prepare prepare-prompts prepare-experiments register
diagnostics: snapshot metrics sanity

all: manifest data train fuse diagnostics  eval
	@echo "Pipeline complete. For interactive test: make repl"

clean:
	@echo "Add rm -rf $(RUN_DIR) $(DATA_DIR) $(EVAL_DIR) if you want a hard clean"
	rm -rf $(RUN_DIR) $(DATA_DIR) $(EVAL_DIR)
