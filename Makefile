coverage_options = --include='sql_metadata.py' --omit='test/*'

install:
	pip install -e .

test:
	py.test

coverage:
	rm -f .coverage*
	rm -rf htmlcov/*
	coverage run -p -m py.test
	coverage combine
	coverage html -d htmlcov $(coverage_options)
	coverage xml -i
	coverage report $(coverage_options)

lint:
	pylint . --ignore=test

publish:
	# run git tag -a v0.0.0 before running make publish
	python setup.py sdist upload -r pypi

.PHONY: test
