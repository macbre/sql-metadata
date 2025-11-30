install:
	poetry install

test:
	poetry run pytest -vv

coverage:
	poetry run pytest -vv --cov=sql_metadata --cov-report=term --cov-report=xml

lint:
	poetry run flake8 sql_metadata
	poetry run pylint sql_metadata

format:
	poetry run black .

publish:
	# run git tag -a v0.0.0 before running make publish
	poetry build
	poetry publish

.PHONY: test
