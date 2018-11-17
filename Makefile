coverage_options = --include='sql_metadata.py' --omit='test/*'

install:
	pip install -e .[dev]

test:
	pytest -v

coverage:
	rm -f .coverage*
	rm -rf htmlcov/*
	coverage run -p -m pytest -v
	coverage combine
	coverage html -d htmlcov $(coverage_options)
	coverage xml -i
	coverage report $(coverage_options)

lint:
	pylint sql_metadata.py

publish:
	# run git tag -a v0.0.0 before running make publish
	python setup.py sdist
	twine upload dist/*

.PHONY: test
