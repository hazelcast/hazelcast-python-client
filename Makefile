.PHONY: check test test-cover

check:
	mypy --show-error-codes hazelcast
	black --check --config black.toml .

test:
	pytest -m "not enterprise"

test-enterprise:
	pytest

test-cover:
	pytest --cov=hazelcast --cov-report=xml
