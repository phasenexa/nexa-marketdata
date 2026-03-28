"""ENTSO-E Transparency Platform client.

Rate limits: ~400 requests/minute per API key (unofficial; subject to change).
Known issues: 403 errors, format inconsistencies between API v1 and v2,
occasional breaking changes. Clients must handle these gracefully.
API base URL: https://web-api.tp.entsoe.eu/api
"""

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any

import pandas as pd
import requests
from entsoe import EntsoePandasClient  # type: ignore[attr-defined]
from entsoe.exceptions import NoMatchingDataError

from nexa_marketdata.exceptions import (
    AuthenticationError,
    DataNotAvailableError,
    ExchangeAPIError,
    RateLimitError,
)
from nexa_marketdata.types import BiddingZone, Resolution

# Map BiddingZone to entsoe-py area identifiers.
# ENTSO-E uses underscored names for sub-national zones (e.g. NO_1, SE_1).
_ZONE_TO_AREA: dict[BiddingZone, str] = {
    BiddingZone.NO1: "NO_1",
    BiddingZone.NO2: "NO_2",
    BiddingZone.NO3: "NO_3",
    BiddingZone.NO4: "NO_4",
    BiddingZone.NO5: "NO_5",
    BiddingZone.SE1: "SE_1",
    BiddingZone.SE2: "SE_2",
    BiddingZone.SE3: "SE_3",
    BiddingZone.SE4: "SE_4",
    BiddingZone.DK1: "DK_1",
    BiddingZone.DK2: "DK_2",
    BiddingZone.FI: "FI",
    BiddingZone.DE_LU: "DE_LU",
    BiddingZone.FR: "FR",
    BiddingZone.BE: "BE",
    BiddingZone.NL: "NL",
    BiddingZone.AT: "AT",
    BiddingZone.CH: "CH",
    BiddingZone.GB: "GB",
    BiddingZone.PL: "PL",
}


class ENTSOEClient:
    """ENTSO-E Transparency Platform client for day-ahead market data.

    Args:
        api_key: ENTSO-E security token. Obtain from the ENTSO-E
            Transparency Platform registration portal.
    """

    def __init__(self, api_key: str) -> None:
        self._api_key = api_key
        self._client: EntsoePandasClient = EntsoePandasClient(api_key=api_key)

    def day_ahead_prices(
        self,
        zone: BiddingZone,
        start: datetime.date,
        end: datetime.date,
        resolution: Resolution = Resolution.HOURLY,
    ) -> pd.DataFrame:
        """Retrieve day-ahead auction prices for a bidding zone.

        Args:
            zone: The bidding zone to retrieve prices for.
            start: Start date (inclusive).
            end: End date (inclusive).
            resolution: Requested time resolution. ENTSO-E provides data at
                its native resolution (hourly prior to the EU MTU transition
                on 30 Sept 2025; 15-minute thereafter). This parameter is
                accepted for interface consistency but is not forwarded to
                the API.

        Returns:
            DataFrame with timezone-aware UTC DatetimeIndex and column
            ``price_eur_mwh`` containing Decimal values (or pd.NA for
            missing periods).

        Raises:
            DataNotAvailableError: If the zone is unsupported or no data
                exists for the requested date range.
            AuthenticationError: If the API key is rejected or forbidden.
            RateLimitError: If the ENTSO-E rate limit is exceeded.
            ExchangeAPIError: For unexpected API errors.
        """
        if zone not in _ZONE_TO_AREA:
            raise DataNotAvailableError(
                f"Bidding zone {zone!r} is not supported on ENTSO-E."
            )

        area = _ZONE_TO_AREA[zone]
        # ENTSO-E query window: start of start_date to start of the day after
        # end_date (the entsoe-py client treats end as exclusive).
        pd_start = pd.Timestamp(start, tz="UTC")
        pd_end = pd.Timestamp(end + datetime.timedelta(days=1), tz="UTC")

        try:
            series: pd.Series[Any] = self._client.query_day_ahead_prices(
                area, start=pd_start, end=pd_end
            )
        except NoMatchingDataError as exc:
            raise DataNotAvailableError(
                f"No day-ahead prices available for {zone!r} between {start} and {end}."
            ) from exc
        except requests.exceptions.HTTPError as exc:
            _raise_for_http_error(exc)
        except Exception as exc:
            raise ExchangeAPIError(f"ENTSO-E API error: {exc}") from exc

        return _series_to_dataframe(series)


def _raise_for_http_error(exc: requests.exceptions.HTTPError) -> None:
    """Map a requests HTTPError to the appropriate nexa exception.

    Args:
        exc: The HTTPError raised by the requests library.

    Raises:
        AuthenticationError: For 401 or 403 responses.
        RateLimitError: For 429 responses.
        ExchangeAPIError: For all other HTTP errors.
    """
    response = exc.response
    if response is not None:
        status = response.status_code
        if status in (401, 403):
            raise AuthenticationError(
                "ENTSO-E API key rejected or access forbidden."
            ) from exc
        if status == 429:
            raise RateLimitError("ENTSO-E rate limit exceeded.") from exc
    raise ExchangeAPIError(f"ENTSO-E API HTTP error: {exc}") from exc


def _series_to_dataframe(series: pd.Series[Any]) -> pd.DataFrame:
    """Convert an entsoe-py price Series to the standard nexa DataFrame format.

    Args:
        series: Price Series returned by EntsoePandasClient with a
            timezone-aware DatetimeIndex and float values (EUR/MWh).

    Returns:
        DataFrame with UTC DatetimeIndex and ``price_eur_mwh`` column
        containing Decimal values (or pd.NA for missing periods).
    """
    index = pd.DatetimeIndex(series.index).tz_convert("UTC")
    prices: list[Any] = [
        Decimal(str(v)) if pd.notna(v) else pd.NA for v in series.to_numpy()
    ]
    return pd.DataFrame({"price_eur_mwh": prices}, index=index)
