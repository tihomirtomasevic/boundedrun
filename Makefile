.PHONY: help install test lint fmt examples bounds clean

help:
	@grep -E '^[a-z]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-10s\033[0m %s\n", $$1, $$2}'

install:  ## editable install with dev extras
	python -m pip install -e ".[dev]"

test:  ## run the offline test suite
	pytest -q

lint:  ## ruff check + format check
	ruff check .
	ruff format --check .

fmt:  ## apply ruff formatting
	ruff format .
	ruff check --fix .

examples:  ## run all three examples
	python examples/01_classify.py
	python examples/02_bounds.py
	python examples/03_graduation.py

bounds:  ## the CI budget guard, run locally
	boundedrun bounds examples.01_classify:pipeline --max-cost 0.10 --max-calls 5

clean:
	rm -rf .pytest_cache .ruff_cache **/__pycache__ runs.db examples/*.db *.db-wal *.db-shm
