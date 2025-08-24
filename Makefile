.PHONY: all setup fetch prep train eval
CONFIG=configs/default.yaml

step:
environment:
	echo "starting environment"
	#python -m pip install -r  requirements.txt
manifest:
	echo "starting manifest"
	python scripts/00_manifest.py --config $(CONFIG)

fetch:
	echo "starting fetch"
	python scripts/01_fetch_hf_dataset.py --config $(CONFIG)

validate:
	echo "starting validate"
	python scripts/02_prepare_data.py --config $(CONFIG)

prompts:
	echo "starting prompt preparation"
	python scripts/022_prepare_prompts.py

experiments:
	echo "starting experiments preparation"
	python scripts/023_prepare_experiments.py

train:
	echo "starting train"
	python scripts/03_train.py --config $(CONFIG)

register:
	echo "starting register"
	python scripts/031_register.py --config $(CONFIG)

fuse:
	echo "starting fuse"
	python scripts/032_fuse.py --config $(CONFIG)

snapshot:
	echo "starting snapshot"
	python scripts/04_snapshot.py --config $(CONFIG)

quality:
	python scripts/041_metrics.py --config $(CONFIG)
	python scripts/042_sanity.py --config $(CONFIG)
eval:
	echo "starting eval"
	python scripts/05_eval.py --config $(CONFIG)
	echo "finished"

all: environment setup fetch validate prompts experiments train register fuse snapshot  eval
