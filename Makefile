.PHONY: all setup fetch prep train eval
CONFIG=configs/default.yaml

environment:
	python -m pip install -r  requirements.txt
setup:
	python scripts/00_setup_data_dir.py --config $(CONFIG)

fetch:
	python scripts/01_fetch_hf_dataset.py --config $(CONFIG)

prep:
	python scripts/02_prepare_data.py --config $(CONFIG)

train:
	python scripts/03_train.py --config $(CONFIG)

eval:
	python scripts/04_eval.py --config $(CONFIG)

all: setup fetch prep train eval
