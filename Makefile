# Macro to run a Python script from EXEC, inside the current CURDIR
define RUN_SCRIPT
(cd $(PWD) && $(PY) $(EXEC)/scripts/$(1))
endef
PY    ?= python

# Dynamically find all override YAMLs (including the base override.yaml)
OVERRIDE_FILES := $(wildcard override*.yaml)

# Strip leading "override." and trailing ".yaml", or just "override.yaml" -> "default"
EXPERIMENTS := $(foreach f,$(OVERRIDE_FILES),$(if $(filter override.yaml,$(f)),default,$(basename $(subst override.,,$(f)))))

# Build targets: train-default, train-joe, etc.
train-%:
	@sh -c '\
		case "$*" in \
			default) CONFIG=override.yaml ;; \
			*) CONFIG=override.$*.yaml ;; \
		esac; \
		echo "Using config: $$CONFIG"; \
		if [ -f "$$CONFIG" ]; then \
			python3 scripts/03_train.py --config "$$CONFIG" || \
			echo "$* FAILED at $$(date)" >> run/failures.log; \
		else \
			echo "Missing config: $$CONFIG"; \
			exit 1; \
		fi \
	'

# Aggregate rule to run all known configs
all: $(addprefix train-, $(EXPERIMENTS))
# Config-driven names (from default.yaml)
RUN_DIR     = $(PWD)/run
ARTIFACTS   = $(RUN_DIR)/artifacts.json
EXPERIMENTS = $(RUN_DIR)/experiments.csv
DATA_DIR    = $(PWD)/run/data
EVAL_DIR    = $(PWD)/eval_out

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
	$(call RUN_SCRIPT,00_manifest.py)
	-mkdir $(DATA_DIR)
        

# 2) Fetch HF dataset
fetch-hf: $(DATA_DIR)
	$(call RUN_SCRIPT,01_fetch_hf_dataset.py)

# 3) Prepare data
prepare: $(CONTRACT)
	$(call RUN_SCRIPT,02_prepare_data.py)

# 3a) Prepare prompts
prepare-prompts: $(CONTRACT)
	$(call RUN_SCRIPT,022_prepare_prompts.py)

# 3b) Prepare experiments
prepare-experiments: $(CONTRACT)
	$(call RUN_SCRIPT,023_prepare_experiments.py)

# 3c) Register run  creates ARTIFACTS
register: $(CONTRACT)
	$(call RUN_SCRIPT,031_register.py)

# 3d) Fuse model (optional)
fuse: $(ARTIFACTS)
	$(call RUN_SCRIPT,032_fuse.py)

# 4) Train
train: $(CONTRACT)
	$(call RUN_SCRIPT,03_train.py)

# 5) Eval (metrics, sanity)
eval: $(ARTIFACTS) $(CONTRACT)
	$(call RUN_SCRIPT,05_eval.py)

# 4.1 snapshot (deterministic generations)
snapshot: $(ARTIFACTS) $(CONTRACT)
	$(call RUN_SCRIPT,04_snapshot.py)

# 4.1a) metrics on snapshot
metrics: $(GEN_JSONL) $(CONTRACT)
	$(call RUN_SCRIPT,041_metrics.py)

# 4.2 sanity checks
sanity: $(ARTIFACTS)
	$(call RUN_SCRIPT,042_sanity.py)

# 9) Alternative crawler (voice data)
crawl-voice:
	$(call RUN_SCRIPT,09_crawl4voice.py)

# REPL (manual check)
repl: $(ARTIFACTS)
	$(call RUN_SCRIPT,repl.py)

# Convenience groups
data: fetch-hf prepare prepare-prompts prepare-experiments register
gotdata: prepare prepare-prompts prepare-experiments register train fuse diagnostics eval

diagnostics: snapshot metrics sanity

all: manifest data train fuse diagnostics  eval
	@echo "Pipeline complete. For interactive test: make repl"

clean:
	@echo "Add rm -rf $(RUN_DIR) $(DATA_DIR) $(EVAL_DIR) if you want a hard clean"
	rm -rf $(RUN_DIR) $(DATA_DIR) $(EVAL_DIR)
