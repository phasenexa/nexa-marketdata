"""Nord Pool API v2 client.

Rate limits: TODO — document once confirmed from Nord Pool documentation.
API base URL: https://dataportal-api.nordpoolgroup.com/api
"""

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any

import httpx
import pandas as pd

from nexa_marketdata.exceptions import (
    AuthenticationError,
    DataNotAvailableError,
    ExchangeAPIError,
    RateLimitError,
)
from nexa_marketdata.types import BiddingZone, Resolution

_TOKEN_URL = "https://sts.nordpoolgroup.com/connect/token"
_BASE_URL = "https://dataportal-api.nordpoolgroup.com/api"
_CLIENT_ID = "client_marketdata_api"
_CLIENT_SECRET = "client_marketdata_api"
_SCOPE = "marketdata_api"

_ZONE_TO_AREA: dict[BiddingZone, str] = {
    BiddingZone.NO1: "NO1",
    BiddingZone.NO2: "NO2",
    BiddingZone.NO3: "NO3",
    BiddingZone.NO4: "NO4",
    BiddingZone.NO5: "NO5",
    BiddingZone.SE1: "SE1",
    BiddingZone.SE2: "SE2",
    BiddingZone.SE3: "SE3",
    BiddingZone.SE4: "SE4",
    BiddingZone.DK1: "DK1",
    BiddingZone.DK2: "DK2",
    BiddingZone.FI: "FI",
    BiddingZone.DE_LU: "GER",
    BiddingZone.AT: "AT",
    BiddingZone.BE: "BE",
    BiddingZone.NL: "NL",
    BiddingZone.FR: "FR",
    BiddingZone.PL: "PL",
    # BiddingZone.GB not included — N2EX / not available via Nord Pool Data Portal
    # BiddingZone.CH not included — not listed in Nord Pool area codes
}

_RESOLUTION_PARAM: dict[Resolution, str] = {
    Resolution.HOURLY: "PT60M",
    Resolution.MINUTES_15: "PT15M",
}


class NordPoolClient:
    """Nord Pool API v2 client for day-ahead market data.

    Args:
        username: Nord Pool account username.
        password: Nord Pool account password.
    """

    def __init__(self, username: str, password: str) -> None:
        self._username = username
        self._password = password
        self._http = httpx.Client(timeout=30.0)
        self._token: str | None = None
        self._token_expires_at: datetime.datetime | None = None

    def _refresh_token_if_needed(self) -> None:
        """Fetch a new token if none exists or within 60s of expiry."""
        now = datetime.datetime.now(tz=datetime.UTC)
        if (
            self._token is not None
            and self._token_expires_at is not None
            and now < self._token_expires_at
        ):
            return

        resp = self._http.post(
            _TOKEN_URL,
            data={
                "grant_type": "password",
                "username": self._username,
                "password": self._password,
                "scope": _SCOPE,
            },
            auth=(_CLIENT_ID, _CLIENT_SECRET),
        )
        if resp.status_code == 401:
            raise AuthenticationError("Nord Pool credentials rejected.")
        resp.raise_for_status()

        payload = resp.json()
        self._token = payload["access_token"]
        expires_in: int = int(payload["expires_in"])
        self._token_expires_at = now + datetime.timedelta(seconds=expires_in - 60)

    def _auth_headers(self) -> dict[str, str]:
        """Return authorisation headers, refreshing the token if needed."""
        self._refresh_token_if_needed()
        assert self._token is not None  # guaranteed by _refresh_token_if_needed
        return {"Authorization": f"Bearer {self._token}"}

    def day_ahead_prices(
        self,
        zone: BiddingZone,
        start: datetime.date,
        end: datetime.date,
        currency: str = "EUR",
        resolution: Resolution = Resolution.HOURLY,
    ) -> pd.DataFrame:
        """Retrieve day-ahead auction prices for a bidding zone.

        Args:
            zone: The bidding zone to retrieve prices for.
            start: Start date (inclusive).
            end: End date (inclusive).
            currency: Currency code. Defaults to "EUR".
            resolution: Time resolution (hourly or 15-minute).

        Returns:
            DataFrame with timezone-aware UTC DatetimeIndex and column
            ``price_eur_mwh`` containing Decimal values (or pd.NA for missing).

        Raises:
            DataNotAvailableError: If zone is not supported on Nord Pool.
            AuthenticationError: If credentials are rejected.
            RateLimitError: If the API rate limit is exceeded.
            ExchangeAPIError: For unexpected API errors.
        """
        if zone not in _ZONE_TO_AREA:
            raise DataNotAvailableError(
                f"Bidding zone {zone!r} is not supported on Nord Pool."
            )

        area = _ZONE_TO_AREA[zone]
        resolution_param = _RESOLUTION_PARAM[resolution]

        frames: list[pd.DataFrame] = []
        current = start
        while current <= end:
            df = self._fetch_day(area, current, currency, resolution_param)
            frames.append(df)
            current += datetime.timedelta(days=1)

        if not frames:
            return pd.DataFrame({"price_eur_mwh": pd.Series([], dtype=object)})
        return pd.concat(frames).sort_index()

    def _fetch_day(
        self,
        area: str,
        date: datetime.date,
        currency: str,
        resolution: str,
    ) -> pd.DataFrame:
        """Fetch day-ahead prices for a single delivery date."""
        resp = self._http.get(
            f"{_BASE_URL}/v2/Auction/Prices/ByAreas",
            params={
                "market": "DayAhead",
                "areas": area,
                "date": date.strftime("%Y-%m-%d"),
                "currency": currency,
                "resolution": resolution,
            },
            headers=self._auth_headers(),
        )
        _raise_for_status(resp)
        return _parse_response(resp.json(), area)


def _raise_for_status(resp: httpx.Response) -> None:
    """Map HTTP error responses to nexa exceptions."""
    if resp.status_code == 401:
        raise AuthenticationError("Nord Pool credentials rejected.")
    if resp.status_code == 429:
        raise RateLimitError("Nord Pool rate limit exceeded.")
    if resp.status_code in (400, 404):
        raise DataNotAvailableError(
            f"Nord Pool returned {resp.status_code}: requested data not available."
        )
    if resp.status_code >= 400:
        raise ExchangeAPIError(
            f"Nord Pool API error {resp.status_code}: {resp.text[:200]}"
        )


def _parse_response(data: dict[str, Any], area: str) -> pd.DataFrame:
    """Parse a Nord Pool API response into a DataFrame.

    Args:
        data: Parsed JSON response body.
        area: Nord Pool area code to extract prices for.

    Returns:
        DataFrame with UTC DatetimeIndex and ``price_eur_mwh`` column.
    """
    rows: list[dict[str, Any]] = data.get("rows", [])
    timestamps: list[datetime.datetime] = []
    prices: list[Any] = []

    for row in rows:
        start_str: str = row["startTime"]
        ts = datetime.datetime.fromisoformat(start_str.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.UTC)

        price: Any = pd.NA
        for area_state in row.get("areaStates", []):
            if area_state.get("area") == area:
                raw = area_state.get("value")
                if raw is not None and raw != "Missing":
                    price = Decimal(str(raw))
                break

        timestamps.append(ts)
        prices.append(price)

    index = pd.DatetimeIndex(timestamps)
    return pd.DataFrame({"price_eur_mwh": prices}, index=index)
