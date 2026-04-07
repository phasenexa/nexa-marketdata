# nexa-marketdata

[![CI](https://github.com/phasenexa/nexa-marketdata/actions/workflows/ci.yml/badge.svg)](https://github.com/phasenexa/nexa-marketdata/actions/workflows/ci.yml)
[![codecov](https://codecov.io/gh/phasenexa/nexa-marketdata/graph/badge.svg?token=wuud4Aly4u)](https://codecov.io/gh/phasenexa/nexa-marketdata)

> **Work in progress.** This library is under active development. APIs may change without notice.

A unified Python client for European power market data sources: Nord Pool, EPEX SPOT, ENTSO-E Transparency Platform, and EEX.

🇳🇴 🇸🇪 🇩🇰 🇫🇮 🇪🇪 🇱🇻 🇱🇹 🇩🇪 🇱🇺 🇫🇷 🇧🇪 🇳🇱 🇦🇹 🇨🇭 🇪🇸 🇵🇹 🇨🇿 🇸🇰 🇭🇺 🇷🇴 🇧🇬 🇸🇮 🇭🇷 🇵🇱 🇷🇸 🇧🇦 🇲🇪 🇲🇰 🇦🇱 🇽🇰 🇲🇩 🇮🇹 🇬🇧 🇮🇪 🇨🇾 🇲🇹 🇮🇸 🇬🇪 🇧🇾 🇺🇦 🇹🇷

Handles 15-minute MTU resolution, rate limiting, response caching, timezone normalisation, and format differences across exchanges. Part of the [Phase Nexa](https://phasenexa.github.io) ecosystem.

## Features

- **Unified interface** — one client, four exchanges
- **MTU-aware** — supports both hourly and 15-minute resolution (EU transition: 30 Sept 2025)
- **Rate limiting** — per-source limits respected automatically
- **Caching** — repeated historical requests served from local cache
- **Timezone normalisation** — all outputs in timezone-aware UTC or local exchange time
- **Type-safe** — strict mypy compliance, Pydantic v2 models
- **No floats for money** — all prices and volumes use `Decimal`

## Status

| Component | |
|---|---|
| Nord Pool — day-ahead prices (Market Data API) | ✅ |
| Nord Pool — day-ahead prices (Auction API fallback) | ✅ |
| Core types & exceptions | ✅ |
| Unified `NexaClient` | 🚧 |
| ENTSO-E client | ✅ |
| EPEX SPOT client | ⬜ |
| EEX client | ⬜ |
| Response caching | ⬜ |
| Rate limiting | ⬜ |
| Timezone normalisation | ⬜ |

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
import datetime

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
# For Nord Pool Market Data API subscribers (preferred, no history cap):
export NORDPOOL_MARKETDATA_USERNAME="your-username"
export NORDPOOL_MARKETDATA_PASSWORD="your-password"

# For Nord Pool DA trading participants (Auction API, fallback):
export NORDPOOL_AUCTION_USERNAME="your-username"
export NORDPOOL_AUCTION_PASSWORD="your-password"

# At least one Nord Pool credential set OR the ENTSO-E key is required.
export ENTSOE_API_KEY="your-key-here"
```

Or use a `.env` file (see `.env.example`).

### Source priority and limitations

If both Nord Pool credential sets are configured, the Market Data API is tried first
(it has no history cap and returns data in a single call per day). The Auction API is
used as a fallback for those without a Market Data subscription, but is limited to the
**past 7 days** of data. ENTSO-E is the final fallback and covers all bidding zones.

At least one source must be configured for Nord Pool zones; ENTSO-E alone is sufficient
for zones outside Nord Pool's coverage (e.g. `BiddingZone.GB`, `BiddingZone.IT_NORD`).

> **Note:** `BiddingZone.DE_LU` (Germany-Luxembourg) is not currently resolvable via the
> Auction API and will fall through to ENTSO-E when only Auction credentials are configured.

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

Unit tests use mocked HTTP responses — no live API calls or credentials required.

Integration tests that call live APIs are excluded from the default run. To run them:

```bash
export ENTSOE_API_KEY="your-key-here"
poetry run pytest -m live
```

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for the full contribution guide.

## Licence

MIT — see [LICENSE](LICENSE).
