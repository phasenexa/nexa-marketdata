# nexa-marketdata

A unified Python client for European power market data sources: Nord Pool, EPEX SPOT, ENTSO-E Transparency Platform, and EEX.

Handles 15-minute MTU resolution, rate limiting, response caching, timezone normalisation, and format differences across exchanges. Part of the [Phase Nexa](https://phasenexa.com) ecosystem.

## Features

- **Unified interface** — one client, four exchanges
- **MTU-aware** — supports both hourly and 15-minute resolution (EU transition: 30 Sept 2025)
- **Rate limiting** — per-source limits respected automatically
- **Caching** — repeated historical requests served from local cache
- **Timezone normalisation** — all outputs in timezone-aware UTC or local exchange time
- **Type-safe** — strict mypy compliance, Pydantic v2 models
- **No floats for money** — all prices and volumes use `Decimal`

## Supported data sources

| Exchange | Data types |
|---|---|
| Nord Pool | Day-ahead prices, intraday |
| ENTSO-E | Day-ahead prices, generation (actual & forecast), load, cross-border flows |
| EPEX SPOT | Day-ahead prices, intraday |
| EEX | Futures, spot |

## Installation

```bash
pip install nexa-marketdata
```

Or with Poetry:

```bash
poetry add nexa-marketdata
```

## Quickstart

```python
from nexa_marketdata import NexaClient
from nexa_marketdata.types import BiddingZone
import datetime, zoneinfo

client = NexaClient()

# Day-ahead prices for NO2 (Southern Norway)
prices = client.day_ahead_prices(
    zone=BiddingZone.NO2,
    start=datetime.date(2025, 1, 1),
    end=datetime.date(2025, 1, 7),
)
print(prices.head())
```

## Configuration

Set API credentials as environment variables:

```bash
export ENTSOE_API_KEY="your-key-here"
export NORDPOOL_API_KEY="your-key-here"
```

Or use a `.env` file (see `.env.example`).

## Development

### Prerequisites

- Python 3.11+
- [Poetry](https://python-poetry.org/docs/#installation)
- Make

### Setup

```bash
git clone https://github.com/phasenexa/nexa-marketdata.git
cd nexa-marketdata
poetry install
make ci
```

### Common tasks

```bash
make test             # run tests with coverage
make lint             # ruff linting
make format           # ruff formatting
make type-check       # mypy
make ci               # full check suite (lint + format + types + tests)
make execute-notebooks  # re-execute example notebooks
```

### Running tests

```bash
make test
```

Tests use recorded HTTP fixtures (VCR cassettes) — no live API calls required.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full contribution guide.

## Licence

MIT — see [LICENSE](LICENSE).
