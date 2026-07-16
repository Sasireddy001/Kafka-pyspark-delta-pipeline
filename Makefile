.PHONY: install test lint benchmark clean

install:
	pip install -e ".[dev]"

test:
	pytest

lint:
	flake8 src tests --count --max-line-length=120 --statistics
	black --check src tests

format:
	black src tests

benchmark:
	python benchmark/benchmark.py --rows 100000

clean:
	rm -rf build dist *.egg-info .pytest_cache .mypy_cache __pycache__ \
	       spark-warehouse metastore_db .ruff_cache
