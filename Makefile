.PHONY: test test-cover

test:
	pytest

test-cover:
	pytest --cov=hazelcast --cov-report=xml
