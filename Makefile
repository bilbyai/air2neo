BUMP ?= patch

.PHONY: test setup release release-test bump tox coverage lint format clean

usage:
	cat Makefile

setup:
	git config core.hooksPath scripts/githooks
	uv sync --group dev

release:
	make clean
	uv build --no-sources --sdist --wheel --out-dir ./dist
	uv publish ./dist/*

release-test:
	make clean
	uv build --no-sources --sdist --wheel --out-dir ./dist
	uv publish --index testpypi ./dist/*

test:
	uv run pytest -v -m 'not integration'

test-e2e:
	uv run pytest -v

tox:
	uv run tox -e py

coverage:
	uv run pytest --cov=air2neo --cov-report=html
	open htmlcov/index.html

lint:
	uv run ruff format --check .
	uv run ruff check .

format:
	uv run ruff format .
	uv run ruff check --fix .

clean:
	rm -rf dist build *.egg-info
