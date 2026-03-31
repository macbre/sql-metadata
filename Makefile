install:
	poetry install

test:
	poetry run pytest -vv

coverage:
	poetry run pytest -vv --cov=sql_metadata --cov-report=term --cov-report=html

lint:
	poetry run ruff check --fix sql_metadata

format:
	poetry run ruff format .

publish:
	# run git tag -a v0.0.0 before running make publish
	poetry build
	poetry publish

.PHONY: test
