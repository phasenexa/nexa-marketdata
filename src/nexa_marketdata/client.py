"""Unified client interface for nexa-marketdata."""

from __future__ import annotations

import datetime

import pandas as pd

from nexa_marketdata.types import BiddingZone, Resolution


class NexaClient:
    """Unified client for European power market data sources.

    Args:
        nordpool_api_key: Nord Pool API v2 key. Falls back to
            ``NORDPOOL_API_KEY`` environment variable.
        entsoe_api_key: ENTSO-E Transparency Platform security token. Falls
            back to ``ENTSOE_API_KEY`` environment variable.
    """

    def __init__(
        self,
        nordpool_api_key: str | None = None,
        entsoe_api_key: str | None = None,
    ) -> None:
        self._nordpool_api_key = nordpool_api_key
        self._entsoe_api_key = entsoe_api_key

    def day_ahead_prices(
        self,
        zone: BiddingZone,
        start: datetime.date,
        end: datetime.date,
        resolution: Resolution = Resolution.HOURLY,
    ) -> pd.DataFrame:
        """Retrieve day-ahead electricity prices for a bidding zone.

        Args:
            zone: The bidding zone to retrieve prices for.
            start: Start date (inclusive).
            end: End date (inclusive).
            resolution: Time resolution. Defaults to hourly.

        Returns:
            DataFrame with a timezone-aware DatetimeIndex and columns:
            ``price_eur_mwh`` (Decimal).

        Raises:
            NotImplementedError: Until implemented.
        """
        raise NotImplementedError
