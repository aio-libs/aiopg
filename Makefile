# Some simple testing tasks (sorry, UNIX only).

clean-docs:
	cd docs && rm -rf _build/html

doc: clean-docs
	cd docs && make html
	@echo "open file://`pwd`/docs/_build/html/index.html"

isort:
	isort -rc aiopg
	isort -rc tests
	isort -rc examples

flake: .flake

.flake: $(shell find aiopg -type f) \
	    $(shell find tests -type f) \
	    $(shell find examples -type f)
	flake8 aiopg tests examples
	python setup.py check -rms
	@if ! isort -c -rc aiopg tests examples; then \
            echo "Import sort errors, run 'make isort' to fix them!!!"; \
            isort --diff -rc aiopg tests examples; \
            false; \
	fi

test: flake
	pytest -q tests

vtest: flake
	pytest tests

cov cover coverage: flake
	py.test -svvv -rs --cov=aiopg --cov-report=html --cov-report=term tests
	@echo "open file://`pwd`/htmlcov/index.html"

cov-ci: flake
	py.test -svvv -rs --cov=aiopg --cov-report=term tests --pg_tag all

clean-pip:
	pip freeze | grep -v "^-e" | xargs pip uninstall -y

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

.PHONY: all isort flake test vtest cov clean clean-pip clean-docs
