.PHONY: install test lint format type-check ci execute-notebooks test-notebooks clean

# Install all dependencies (including dev)
install:
	poetry install

# Run unit tests with coverage
test:
	poetry run pytest

# Lint with ruff
lint:
	poetry run ruff check src tests

# Format code with ruff
format:
	poetry run ruff format src tests

# Check formatting without modifying files
format-check:
	poetry run ruff format --check src tests

# Run mypy type checking
type-check:
	poetry run mypy src

# Execute example notebooks and update their outputs
# Skips gracefully when no notebooks exist yet.
execute-notebooks:
	@notebooks=$$(find examples -name "*.ipynb" 2>/dev/null); \
	if [ -z "$$notebooks" ]; then \
		echo "No notebooks found in examples/ — skipping."; \
	else \
		poetry run jupyter nbconvert --to notebook --execute --inplace $$notebooks; \
	fi

# Run notebooks to verify they execute without error (non-destructive)
# Skips gracefully when no notebooks exist yet.
test-notebooks:
	@notebooks=$$(find examples -name "*.ipynb" 2>/dev/null); \
	if [ -z "$$notebooks" ]; then \
		echo "No notebooks found in examples/ — skipping."; \
	else \
		poetry run jupyter nbconvert --to notebook --execute $$notebooks --output-dir /tmp/nb-test; \
	fi

# Run the full CI check suite locally
ci: lint format-check type-check test

# Remove build artefacts and caches
clean:
	rm -rf dist/ .coverage coverage.xml htmlcov/ .mypy_cache/ .ruff_cache/ .pytest_cache/
	find . -type d -name __pycache__ -exec rm -rf {} +
