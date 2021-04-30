install:
	poetry install

test:
	poetry run pytest -vv

coverage:
	poetry run pytest -vv --cov=sql_metadata --cov-report=term

lint:
	poetry run pylint sql_metadata

publish:
	# run git tag -a v0.0.0 before running make publish
	poetry build
	poetry publish

.PHONY: test
