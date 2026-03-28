# nexa-marketdata

## What this is

A Python library providing a unified API client for European power market
data sources: Nord Pool, EPEX SPOT, ENTSO-E Transparency Platform, and EEX.
Handles 15-minute MTU resolution, rate limiting, response caching, timezone
normalisation, and format differences across exchanges.
Part of the Phase Nexa ecosystem.

## Audience

Quants, data scientists, and developers at energy trading companies who build
their own trading systems. They are experienced Python users who are currently
using fragile community wrappers (entsoe-py, kipe/nordpool) or hand-rolled
API clients. This library replaces all of that with a single, well-maintained
client.

## Code style

- Python 3.11+
- Type hints everywhere, strict mypy compliance
- Pydantic v2 for data models
- pytest for testing
- Ruff for linting and formatting
- No classes where a function will do
- Docstrings on all public API (Google style)
- Tabular numerical data uses pandas DataFrames
- Timezone-aware datetimes only (never naive)
- Utilise the Makefile for common developer actions
- Always prefer UK English unless using existing nomenclature popular in energy trading

## Domain context

- MTU = Market Time Unit. EU power markets transitioned to 15-minute
  MTUs on 30 Sept 2025. Library must handle both 15-min and hourly resolution.
- ENTSO-E = European Network of Transmission System Operators for Electricity.
  Their Transparency Platform provides pan-European generation, load, and price data.
  The API has known reliability issues (403 errors, format inconsistencies,
  breaking changes between v1 and v2). Our client must handle these gracefully.
- Nord Pool = primary power exchange for the Nordic and Baltic regions.
  API v1 was deprecated Oct 2024; we target v2 only (consolidated endpoints).
  Nord Pool considers automated scraping of their website technically disallowed;
  always use their official API.
- EPEX SPOT = Central/Western European power exchange (DE-LU, FR, BE, NL, AT, CH, etc.).
- EEX = European Energy Exchange, primarily futures and derivatives but also spot.
- Bidding zones: e.g. NO1-NO5 (Nordic), SE1-SE4 (Sweden), DE-LU, FR, etc.
  Zone identifiers vary across exchanges. The library normalises these.
- Day-ahead (DA) prices, intraday prices, generation forecasts, actual generation,
  cross-border flows, and load data are all in scope.
- Rate limiting: all exchange APIs have rate limits. The client must respect
  these without the user needing to think about it.
- Caching: repeated requests for the same data (especially historical) should
  be cached locally to reduce API load and improve performance.

## Testing

- pytest with fixtures for common data retrieval scenarios
- Use VCR.py or responses library to record/replay HTTP interactions
  (never make live API calls in CI)
- Property-based testing with hypothesis where appropriate
- All monetary values use Decimal, not float
- Aim for high test coverage
- Run `make test` to run unit tests

## Do not

- Do not use float for prices or volumes. Use Decimal.
- Do not create naive datetimes. Always use timezone-aware.
- Do not add unnecessary dependencies.
- Do not make live API calls in tests. Use recorded fixtures.
- Do not hardcode API keys or credentials. Use environment variables or
  configuration files.
- Do not scrape Nord Pool's website. Use their official API only.

## Workflow (Trunk-Based Development)

- **NEVER push directly to main**. The main branch is protected.
- **Always work on feature branches**: Create a short-lived feature branch for each task (e.g. `feat/add-entsoe-client`, `fix/nordpool-rate-limit`)
- **Create pull requests**: When work is complete, create a PR to merge into main
- **Branch naming conventions**:
  - `feat/description` for new features
  - `fix/description` for bug fixes
  - `refactor/description` for refactoring
  - `docs/description` for documentation updates
  - `test/description` for test improvements
- **Before creating a PR**:
  - Run `make test` to ensure all tests pass
  - Run type checking: `poetry run mypy src`
  - Run linting: `poetry run ruff check src tests`
  - Ensure Definition of Done criteria are met
  - Write clear, descriptive commit messages
- **PR requirements**:
  - Title should clearly describe the change
  - Description should explain why the change is needed
  - All tests must pass
  - Code coverage should meet or exceed 80%
  - At least 1 approving review required (when branch protection is enabled)
- **Commit workflow**:
  - Make focused, atomic commits
  - Each commit should represent a logical unit of work
  - Run tests before committing
  - Use conventional commit messages when appropriate

## Code layout

```text
nexa-marketdata/
  src/nexa_marketdata/
    __init__.py
    client.py          # unified client interface (entry point)
    nordpool.py        # Nord Pool API v2 client
    entsoe.py          # ENTSO-E Transparency Platform client
    epex_spot.py       # EPEX SPOT client
    eex.py             # EEX client
    cache.py           # local response caching layer
    rate_limit.py      # per-source rate limiting
    normalise.py       # timezone and format normalisation
    types.py           # core types/enums (BiddingZone, Resolution, DataSource, etc.)
    exceptions.py      # custom exception hierarchy
  tests/
    conftest.py
    fixtures/          # recorded HTTP responses (VCR cassettes)
    test_client.py
    test_nordpool.py
    test_entsoe.py
    test_epex_spot.py
    test_eex.py
    test_cache.py
    test_rate_limit.py
    test_normalise.py
    test_types.py
  docs/
  examples/
  pyproject.toml
  CLAUDE.md
  CONTRIBUTING.md
  README.md
```

Use pyproject.toml with hatchling or setuptools as the build backend. Don't use setup.py in 2026.

## Definition of Done

- Implementation status updated in @README.md
- Update tests to include new/changed work, aim for >80% code coverage, but prioritise good tests
- Run tests and ensure they pass using `make ci`
- If anything changed that would impact the notebooks, run `make execute-notebooks` to update their output
- Update README and/or docs to document the new behaviour/feature
- Check if Makefile (and `make ci`) is missing any common operations (for new functionality added)
- When adding/changing significant work, add a new notebook to @examples/ documenting the feature
  - If done, execute using `make execute-notebooks` so the output is checked in
- Add anything needed in @.gitignore to avoid checking in secrets, or temp files/logs
- Never commit API keys, tokens, or credentials
