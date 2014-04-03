# Some simple testing tasks (sorry, UNIX only).

PYTHON=python3.3

FILTER=

doc:
	cd docs && make html
	echo "open file://`pwd`/docs/_build/html/index.html"

pep:
	pep8 aiopg examples tests

flake:
	pyflakes3 .

test: pep flake
	$(PYTHON) runtests.py $(FILTER)

vtest: pep flake
	$(PYTHON) runtests.py -v $(FILTER)

testloop: pep flake
	$(PYTHON) runtests.py --forever $(FILTER)

cov cover coverage: pep flake
	$(PYTHON) runtests.py --coverage $(FILTER)

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

.PHONY: all pep test vtest testloop cov clean
