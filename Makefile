# Some simple testing tasks (sorry, UNIX only).

doc:
	cd docs && make html
	echo "open file://`pwd`/docs/_build/html/index.html"

pep:
	pep8 aiopg examples tests

flake:
	pyflakes aiopg examples tests

test: pep flake
	py.test -q tests

vtest: pep flake
	py.test tests

cov cover coverage: pep flake
	py.test --cov=aiopg --cov=tests --cov-report=html --cov-report=term tests

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

.PHONY: all pep test vtest cov clean
