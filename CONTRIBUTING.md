# Contributing to nexa-marketdata

Thanks for your interest in contributing. This document covers how to get set up,
the conventions we follow, and the process for getting changes merged.

## Getting started

### Prerequisites

- Python 3.11+
- [Poetry](https://python-poetry.org/docs/#installation) for dependency management
- Make (for common tasks)
- A GitHub account

### Setup

```bash
# Clone the repo
git clone https://github.com/phasenexa/nexa-marketdata.git
cd nexa-marketdata

# Install dependencies (including dev extras)
poetry install

# Verify everything works
make ci
```

### Project structure

```
src/nexa_marketdata/    # library source
tests/                  # test suite
tests/fixtures/         # recorded HTTP responses (VCR cassettes)
examples/               # Jupyter notebooks
docs/                   # documentation
```

See `CLAUDE.md` for a full breakdown of the code layout and domain context.

## Development workflow

We use **trunk-based development**. The `main` branch is protected and all changes
go through pull requests.

### 1. Create a feature branch

```bash
git checkout main && git pull
git checkout -b feat/your-feature-name
```

Branch naming conventions:

| Prefix       | Use for                    |
|--------------|----------------------------|
| `feat/`      | New features               |
| `fix/`       | Bug fixes                  |
| `refactor/`  | Refactoring (no new behaviour) |
| `docs/`      | Documentation updates      |
| `test/`      | Test improvements          |
| `chore/`     | Maintenance (deps, config) |

### 2. Make your changes

Write code, write tests. See the code style section below. Commit as you go
with focused, atomic commits.

### 3. Run the checks

Before opening a PR, run the full check suite locally:

```bash
# Everything in one command
make ci

# Or individually:
poetry run ruff check src tests        # lint
poetry run ruff format --check src tests  # format check
poetry run mypy src                    # type checking
make test                              # tests with coverage
make test-notebooks                    # example notebooks
```

### 4. Open a pull request

```bash
git push -u origin feat/your-feature-name
gh pr create --title "feat: short description" --body "Why this change is needed."
```

PR requirements:

- Clear title describing the change
- Description explaining the motivation
- All CI checks pass
- Code coverage meets or exceeds 80%
- At least 1 approving review (when branch protection is enabled)

### 5. After merge

Delete your feature branch. CI handles the rest.

## Code style

### Python conventions

- **Python 3.11+** with type hints on all public API
- **Pydantic v2** for data models
- **Ruff** for linting and formatting (no black, no isort, ruff handles both)
- **mypy** in strict mode for type checking
- **Google-style docstrings** on all public functions and classes
- Prefer functions over classes where a function will do
- UK English in documentation and comments (unless using established energy trading terminology)

### Data handling

- **Decimal** for all prices and volumes. Never float.
- **Timezone-aware datetimes only**. Never naive. Use `zoneinfo.ZoneInfo`.
- **pandas DataFrames** for tabular numerical data returned to the user
- All DataFrame columns should use consistent naming conventions (snake_case)

### Testing

- **pytest** with descriptive test names
- **VCR.py or responses** for HTTP interaction recording. Never hit live APIs in CI.
- **hypothesis** for property-based testing where appropriate
- Fixtures go in `tests/fixtures/`
- Aim for >80% coverage, but prioritise meaningful tests over chasing the number

### Dependencies

Keep them minimal. Every dependency is a maintenance burden. If the standard library
can do it, use the standard library.

Core dependencies should be limited to:

- `httpx` or `aiohttp` for HTTP (pick one, stay consistent)
- `pydantic` for data models
- `pandas` for tabular output
- `tenacity` for retry logic (if needed)

Everything else needs a good justification.

## Commit messages

Use conventional commits when appropriate:

```
feat: add ENTSO-E day-ahead price retrieval
fix: handle 403 rate limit response from Nord Pool API v2
refactor: extract timezone normalisation into shared module
docs: add notebook for cross-border flow analysis
test: add VCR cassettes for EPEX SPOT generation data
chore: update httpx to 0.28
```

The first line should be short (under 72 characters). Add a body if the "why"
is not obvious from the title.

## Working with exchange APIs

### API keys and credentials

- Never commit API keys, tokens, or credentials
- Use environment variables for configuration (e.g. `ENTSOE_API_KEY`)
- Document required environment variables in `.env.example`
- Tests must never require real API credentials

### Recording HTTP fixtures

When adding support for a new endpoint or exchange:

1. Write your client code
2. Run it once against the real API to record the response
3. Save the recorded response as a fixture in `tests/fixtures/`
4. Strip any credentials or sensitive data from the fixture
5. Write tests that use the recorded fixture

### Rate limiting

All exchange APIs have rate limits. When implementing a new data source:

- Document the known rate limits in the module docstring
- Use the shared rate limiting infrastructure in `rate_limit.py`
- Test the rate limiting behaviour with mocked time

## Reporting issues

Open a GitHub issue. Include:

- What you were trying to do
- What happened instead
- Which exchange/data source was involved
- Python version and OS
- Minimal reproduction if possible

## Licence

By contributing, you agree that your contributions will be licensed under
the MIT Licence.
