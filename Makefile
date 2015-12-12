# Some simple testing tasks (sorry, UNIX only).

doc:
	cd docs && make html
	@echo "open file://`pwd`/docs/_build/html/index.html"

flake:
	exclude=$$(python -c "import sys;sys.stdout.write('--exclude test_pep492.py') if sys.version_info[:3] < (3, 5, 0) else None"); \
	flake8 aiopg examples tests $$exclude

test: flake
	py.test -q tests

vtest: flake
	py.test tests

cov cover coverage: flake
	py.test --cov=aiopg --cov=tests --cov-report=html --cov-report=term tests
	@echo "open file://`pwd`/htmlcov/index.html"

clean:
	find . -name __pycache__ |xargs rm -rf
	find . -type f -name '*.py[co]' -delete
	find . -type f -name '*~' -delete
	find . -type f -name '.*~' -delete
	find . -type f -name '@*' -delete
	find . -type f -name '#*#' -delete
	find . -type f -name '*.orig' -delete
	find . -type f -name '*.rej' -delete
	rm -f .coverage
	rm -rf coverage
	rm -rf docs/_build
	rm -rf .tox

.PHONY: all test vtest cov clean
