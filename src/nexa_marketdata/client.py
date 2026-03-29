"""Unified client interface for nexa-marketdata."""

from __future__ import annotations

import datetime
import os

import pandas as pd

from nexa_marketdata.entsoe import ENTSOEClient
from nexa_marketdata.exceptions import DataNotAvailableError
from nexa_marketdata.nordpool import NordPoolClient
from nexa_marketdata.types import BiddingZone, Resolution

# Zones served by Nord Pool Data Portal.
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
        "Set nordpool_username/nordpool_password or "
        "NORDPOOL_USERNAME/NORDPOOL_PASSWORD environment variables.",
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

        Sources are tried in priority order (Nord Pool first, then ENTSO-E).
        A source is skipped when its credentials are not configured, allowing
        automatic fallthrough to the next available source.

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
