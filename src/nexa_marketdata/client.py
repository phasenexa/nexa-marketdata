"""Unified client interface for nexa-marketdata."""

from __future__ import annotations

import datetime
import os

import pandas as pd

from nexa_marketdata.entsoe import ENTSOEClient
from nexa_marketdata.exceptions import DataNotAvailableError
from nexa_marketdata.nordpool import NordPoolAuctionClient, NordPoolClient
from nexa_marketdata.types import BiddingZone, Resolution

# Zones served by Nord Pool Market Data API and Auction API.
_NORDPOOL_ZONES: frozenset[BiddingZone] = frozenset(
    {
        BiddingZone.NO1,
        BiddingZone.NO2,
        BiddingZone.NO3,
        BiddingZone.NO4,
        BiddingZone.NO5,
        BiddingZone.SE1,
        BiddingZone.SE2,
        BiddingZone.SE3,
        BiddingZone.SE4,
        BiddingZone.DK1,
        BiddingZone.DK2,
        BiddingZone.FI,
        BiddingZone.DE_LU,
        BiddingZone.AT,
        BiddingZone.BE,
        BiddingZone.NL,
        BiddingZone.FR,
        BiddingZone.PL,
    }
)

# Zones served by the Nord Pool Auction API (subset of _NORDPOOL_ZONES;
# DE_LU is excluded because the CWE product does not expose a "GER" area code).
_NORDPOOL_AUCTION_ZONES: frozenset[BiddingZone] = _NORDPOOL_ZONES - {BiddingZone.DE_LU}

# Zones served by ENTSO-E Transparency Platform (all known bidding zones).
_ENTSOE_ZONES: frozenset[BiddingZone] = frozenset(BiddingZone)

# Priority-ordered source definitions used by the routing logic.
# Each entry: (client attribute name, zone set, display name, credential hint).
# Sources are tried in order; a source is skipped if its client is None
# (i.e. credentials were not provided), not merely because the zone is absent.
_SOURCES: list[tuple[str, frozenset[BiddingZone], str, str]] = [
    (
        "_nordpool",
        _NORDPOOL_ZONES,
        "Nord Pool",
        "Set nordpool_marketdata_username/nordpool_marketdata_password or "
        "NORDPOOL_MARKETDATA_USERNAME/NORDPOOL_MARKETDATA_PASSWORD env vars.",
    ),
    (
        "_nordpool_auction",
        _NORDPOOL_AUCTION_ZONES,
        "Nord Pool Auction",
        "Set nordpool_auction_username/nordpool_auction_password or "
        "NORDPOOL_AUCTION_USERNAME/NORDPOOL_AUCTION_PASSWORD environment variables.",
    ),
    (
        "_entsoe",
        _ENTSOE_ZONES,
        "ENTSO-E",
        "Set entsoe_api_key or the ENTSOE_API_KEY environment variable.",
    ),
    # Future sources (e.g. EXAA) can be appended here without changing the
    # routing logic below.
]


class NexaClient:
    """Unified client for European power market data sources.

    Sources are tried in priority order:

    1. **Nord Pool Market Data API** — preferred when available; single call per
       day, no history cap. Requires a separate paid subscription.
    2. **Nord Pool Auction API** — fallback for DA trading members who do not
       have a Market Data subscription. Limited to the past 7 days.
    3. **ENTSO-E** — final fallback; covers all bidding zones but may differ
       slightly in granularity and availability.

    At least one source must be configured. ENTSO-E alone is sufficient for
    zones outside Nord Pool's coverage.

    Args:
        nordpool_marketdata_username: Nord Pool Market Data API username. Falls
            back to ``NORDPOOL_MARKETDATA_USERNAME`` environment variable.
        nordpool_marketdata_password: Nord Pool Market Data API password. Falls
            back to ``NORDPOOL_MARKETDATA_PASSWORD`` environment variable.
        nordpool_auction_username: Nord Pool Auction API username. Falls back to
            ``NORDPOOL_AUCTION_USERNAME`` environment variable.
        nordpool_auction_password: Nord Pool Auction API password. Falls back to
            ``NORDPOOL_AUCTION_PASSWORD`` environment variable.
        entsoe_api_key: ENTSO-E Transparency Platform security token. Falls
            back to ``ENTSOE_API_KEY`` environment variable.
    """

    def __init__(
        self,
        nordpool_marketdata_username: str | None = None,
        nordpool_marketdata_password: str | None = None,
        nordpool_auction_username: str | None = None,
        nordpool_auction_password: str | None = None,
        entsoe_api_key: str | None = None,
    ) -> None:
        md_user = nordpool_marketdata_username or os.environ.get(
            "NORDPOOL_MARKETDATA_USERNAME"
        )
        md_pass = nordpool_marketdata_password or os.environ.get(
            "NORDPOOL_MARKETDATA_PASSWORD"
        )
        self._nordpool = (
            NordPoolClient(md_user, md_pass) if (md_user and md_pass) else None
        )

        au_user = nordpool_auction_username or os.environ.get(
            "NORDPOOL_AUCTION_USERNAME"
        )
        au_pass = nordpool_auction_password or os.environ.get(
            "NORDPOOL_AUCTION_PASSWORD"
        )
        self._nordpool_auction = (
            NordPoolAuctionClient(au_user, au_pass) if (au_user and au_pass) else None
        )

        self._entsoe_api_key = entsoe_api_key or os.environ.get("ENTSOE_API_KEY")
        self._entsoe = (
            ENTSOEClient(self._entsoe_api_key) if self._entsoe_api_key else None
        )

    def day_ahead_prices(
        self,
        zone: BiddingZone,
        start: datetime.date,
        end: datetime.date,
        resolution: Resolution = Resolution.HOURLY,
    ) -> pd.DataFrame:
        """Retrieve day-ahead electricity prices for a bidding zone.

        Sources are tried in priority order (Nord Pool Market Data first, then
        Nord Pool Auction, then ENTSO-E). A source is skipped when its
        credentials are not configured, allowing automatic fallthrough to the
        next available source.

        Args:
            zone: The bidding zone to retrieve prices for.
            start: Start date (inclusive).
            end: End date (inclusive).
            resolution: Time resolution. Defaults to hourly.

        Returns:
            DataFrame with a timezone-aware DatetimeIndex and column
            ``price_eur_mwh`` (Decimal).

        Raises:
            DataNotAvailableError: If no source supports the zone, or the
                zone is supported but no source is configured.
        """
        capable = [
            (attr, label, hint)
            for attr, zones, label, hint in _SOURCES
            if zone in zones
        ]
        if not capable:
            raise DataNotAvailableError(
                f"No data source available for bidding zone {zone!r}."
            )

        for attr, _label, _hint in capable:
            source_client = getattr(self, attr)
            if source_client is not None:
                result: pd.DataFrame = source_client.day_ahead_prices(
                    zone, start, end, resolution=resolution
                )
                return result

        labels = " or ".join(label for _, label, _ in capable)
        hints = " ".join(hint for _, _, hint in capable)
        raise DataNotAvailableError(
            f"Bidding zone {zone!r} is available via {labels} but no source is "
            f"configured. {hints}"
        )
