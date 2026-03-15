"""Unified client interface for nexa-marketdata."""

from __future__ import annotations

import datetime
import os

import pandas as pd

from nexa_marketdata.exceptions import DataNotAvailableError
from nexa_marketdata.nordpool import NordPoolClient
from nexa_marketdata.types import BiddingZone, Resolution

# Zones served by Nord Pool Data Portal
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


class NexaClient:
    """Unified client for European power market data sources.

    Args:
        nordpool_username: Nord Pool account username. Falls back to
            ``NORDPOOL_USERNAME`` environment variable.
        nordpool_password: Nord Pool account password. Falls back to
            ``NORDPOOL_PASSWORD`` environment variable.
        entsoe_api_key: ENTSO-E Transparency Platform security token. Falls
            back to ``ENTSOE_API_KEY`` environment variable.
    """

    def __init__(
        self,
        nordpool_username: str | None = None,
        nordpool_password: str | None = None,
        entsoe_api_key: str | None = None,
    ) -> None:
        username = nordpool_username or os.environ.get("NORDPOOL_USERNAME")
        password = nordpool_password or os.environ.get("NORDPOOL_PASSWORD")
        self._nordpool = (
            NordPoolClient(username, password) if (username and password) else None
        )
        self._entsoe_api_key = entsoe_api_key or os.environ.get("ENTSOE_API_KEY")

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
            DataFrame with a timezone-aware DatetimeIndex and column
            ``price_eur_mwh`` (Decimal).

        Raises:
            DataNotAvailableError: If no client is configured for the zone.
        """
        if zone in _NORDPOOL_ZONES:
            if self._nordpool is None:
                raise DataNotAvailableError(
                    f"No Nord Pool credentials configured for zone {zone!r}. "
                    "Set nordpool_username and nordpool_password or "
                    "NORDPOOL_USERNAME/NORDPOOL_PASSWORD environment variables."
                )
            return self._nordpool.day_ahead_prices(
                zone, start, end, resolution=resolution
            )
        raise DataNotAvailableError(
            f"No data source available for bidding zone {zone!r}."
        )
