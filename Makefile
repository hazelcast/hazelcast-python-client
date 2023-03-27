.PHONY: check test test-cover

check:
	mypy --show-error-codes hazelcast
	black --check --config black.toml .

test:
	pytest

test-cover:
	pytest --cov=hazelcast --cov-report=xml
