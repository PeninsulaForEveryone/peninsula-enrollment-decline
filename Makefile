.PHONY: install run run-force enrollment housing merge clean lint

install:
	python3 -m pip install -r requirements.txt

run:
	python3 -m pipeline.run_all

run-force:
	python3 -m pipeline.run_all --force

enrollment:
	python3 -m pipeline.run_all --step enrollment

housing:
	python3 -m pipeline.run_all --step housing

merge:
	python3 -m pipeline.run_all --step merge

clean:
	rm -rf data/raw/ data/processed/*.parquet data/processed/*.csv

lint:
	python3 -m py_compile pipeline/config.py pipeline/fetch_enrollment.py pipeline/fetch_housing.py pipeline/merge.py pipeline/run_all.py && echo "Syntax OK"
