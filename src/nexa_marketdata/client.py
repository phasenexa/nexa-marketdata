"""Unified client interface for nexa-marketdata."""

from __future__ import annotations

import datetime
import os

import pandas as pd

from nexa_marketdata.entsoe import ENTSOEClient
from nexa_marketdata.exaa import EXAAClient
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
        BiddingZone.BE,
        BiddingZone.NL,
        BiddingZone.FR,
        BiddingZone.PL,
    }
)

# Zones only available via ENTSO-E (not on Nord Pool Data Portal)
_ENTSOE_ZONES: frozenset[BiddingZone] = frozenset(
    {
        BiddingZone.CH,
        BiddingZone.GB,
    }
)

# Zones served by EXAA (Energy Exchange Austria).
# AT is EXAA's home market; the Classic auction at 10:15 CET is the
# authoritative Austrian day-ahead price and the only source of 15-minute
# products for AT.
_EXAA_ZONES: frozenset[BiddingZone] = frozenset(
    {
        BiddingZone.AT,
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
        exaa_username: EXAA trading account username. Falls back to
            ``EXAA_USERNAME`` environment variable.
        exaa_password: EXAA trading account password. Falls back to
            ``EXAA_PASSWORD`` environment variable.
        exaa_private_key_path: Path to EXAA RSA private key PEM file. Falls
            back to ``EXAA_PRIVATE_KEY_PATH`` environment variable.
        exaa_certificate_path: Path to EXAA X.509 certificate PEM file. Falls
            back to ``EXAA_CERTIFICATE_PATH`` environment variable.
    """

    def __init__(
        self,
        nordpool_username: str | None = None,
        nordpool_password: str | None = None,
        entsoe_api_key: str | None = None,
        exaa_username: str | None = None,
        exaa_password: str | None = None,
        exaa_private_key_path: str | None = None,
        exaa_certificate_path: str | None = None,
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

        exaa_user = exaa_username or os.environ.get("EXAA_USERNAME")
        exaa_pass = exaa_password or os.environ.get("EXAA_PASSWORD")
        exaa_key = exaa_private_key_path or os.environ.get("EXAA_PRIVATE_KEY_PATH")
        exaa_cert = exaa_certificate_path or os.environ.get("EXAA_CERTIFICATE_PATH")
        self._exaa = (
            EXAAClient(exaa_user, exaa_pass, exaa_key, exaa_cert)
            if (exaa_user and exaa_pass and exaa_key and exaa_cert)
            else None
        )

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
        if zone in _ENTSOE_ZONES:
            if self._entsoe is None:
                raise DataNotAvailableError(
                    f"No ENTSO-E API key configured for zone {zone!r}. "
                    "Set entsoe_api_key or the ENTSOE_API_KEY environment variable."
                )
            return self._entsoe.day_ahead_prices(
                zone, start, end, resolution=resolution
            )
        if zone in _EXAA_ZONES:
            if self._exaa is None:
                raise DataNotAvailableError(
                    f"No EXAA credentials configured for zone {zone!r}. "
                    "Set exaa_username, exaa_password, exaa_private_key_path, and "
                    "exaa_certificate_path or the EXAA_* environment variables."
                )
            return self._exaa.day_ahead_prices(zone, start, end, resolution=resolution)
        raise DataNotAvailableError(
            f"No data source available for bidding zone {zone!r}."
        )
