coverage_options = --include='sql_metadata.py' --omit='test/*'

install:
	poetry install

test:
	poetry run pytest -vv

coverage:
	rm -f .coverage*
	rm -rf htmlcov/*
	coverage run -p -m pytest -vv
	coverage combine
	coverage html -d htmlcov $(coverage_options)
	coverage xml -i
	coverage report $(coverage_options)

lint:
	poetry run pylint sql_metadata.py

publish:
	# run git tag -a v0.0.0 before running make publish
	poetry build
	poetry publish

.PHONY: test
