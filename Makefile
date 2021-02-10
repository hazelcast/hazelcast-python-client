.PHONY: test test-all

PYTHON ?= "python"

test:
	# Runs only unit tests
	nosetests -w tests/unittests

test-all:
	# Runs unit and integration tests
	$(PYTHON) run_tests.py
