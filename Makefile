.PHONY: check test test-cover

check:
	mypy --show-error-codes hazelcast
	black --check --config black.toml .

test:
	pytest --verbose -m "not enterprise"

test-enterprise:
	pytest --verbose

test-cover:
	pytest --cov=hazelcast --cov-report=xml
