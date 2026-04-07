"""Nord Pool API v2 clients: Market Data API and Auction API.

Rate limits: TODO — document once confirmed from Nord Pool documentation.

Market Data API base URL: https://dataportal-api.nordpoolgroup.com/api
Auction API base URL:     https://auctions-api.nordpoolgroup.com/api/v1
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

# Market Data API constants
_BASE_URL = "https://dataportal-api.nordpoolgroup.com/api"
_CLIENT_ID = "client_marketdata_api"
_CLIENT_SECRET = "client_marketdata_api"
_SCOPE = "marketdata_api"

# Auction API constants
_AUCTION_CLIENT_ID = "client_auction_api"
_AUCTION_CLIENT_SECRET = "client_auction_api"
_AUCTION_SCOPE = "auction_api"
_AUCTION_BASE_URL = "https://auctions-api.nordpoolgroup.com/api/v1"

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

# Maps bidding zones to Auction API product IDs.
# Multiple zones may share a single productId (e.g. all Nordic zones use NOR_QH_DA_1).
# QH products return 96 quarter-hourly contracts per day (native 15-min resolution).
# DE_LU is excluded: the CWE product uses TSO-level area codes, not the "GER" zone code.
# TODO: verify CWE area code for DE_LU and add if confirmed.
_ZONE_TO_PRODUCT_ID: dict[BiddingZone, str] = {
    BiddingZone.NO1: "NOR_QH_DA_1",
    BiddingZone.NO2: "NOR_QH_DA_1",
    BiddingZone.NO3: "NOR_QH_DA_1",
    BiddingZone.NO4: "NOR_QH_DA_1",
    BiddingZone.NO5: "NOR_QH_DA_1",
    BiddingZone.SE1: "NOR_QH_DA_1",
    BiddingZone.SE2: "NOR_QH_DA_1",
    BiddingZone.SE3: "NOR_QH_DA_1",
    BiddingZone.SE4: "NOR_QH_DA_1",
    BiddingZone.DK1: "NOR_QH_DA_1",
    BiddingZone.DK2: "NOR_QH_DA_1",
    BiddingZone.FI: "NOR_QH_DA_1",
    BiddingZone.AT: "CWE_QH_DA_1",
    BiddingZone.BE: "CWE_QH_DA_1",
    BiddingZone.NL: "CWE_QH_DA_1",
    BiddingZone.FR: "CWE_QH_DA_1",
    BiddingZone.PL: "PL_QH_DA_1",
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


class NordPoolAuctionClient:
    """Nord Pool Auction API client for day-ahead market data.

    Uses the Auction API included with Nord Pool DA trading membership.
    This is a fallback for users who do not have a Market Data API subscription.

    **Important limitations:**

    - Data is only available for the past 7 days. For older historical data,
      use :class:`NordPoolClient` (Market Data API).
    - ``DE_LU`` (Germany-Luxembourg) is not currently supported via this API;
      use the Market Data API or ENTSO-E instead.
    - Prices are fetched at native 15-minute (QH) resolution and aggregated to
      hourly when ``resolution=Resolution.HOURLY`` is requested.

    Args:
        username: Nord Pool account username (Auction API credentials).
        password: Nord Pool account password (Auction API credentials).
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
                "scope": _AUCTION_SCOPE,
            },
            auth=(_AUCTION_CLIENT_ID, _AUCTION_CLIENT_SECRET),
        )
        if resp.status_code == 401:
            raise AuthenticationError("Nord Pool Auction API credentials rejected.")
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

        Prices are fetched at native 15-minute (QH) resolution. When
        ``resolution=Resolution.HOURLY`` is requested, the four quarter-hourly
        values per clock-hour are averaged into a single Decimal. Periods where
        all four quarter-hourly values are ``pd.NA`` remain ``pd.NA``.

        Args:
            zone: The bidding zone to retrieve prices for.
            start: Start date (inclusive).
            end: End date (inclusive).
            currency: Currency code. Defaults to ``"EUR"``.
            resolution: Time resolution (hourly or 15-minute).

        Returns:
            DataFrame with timezone-aware UTC DatetimeIndex and column
            ``price_eur_mwh`` containing Decimal values (or pd.NA for missing).

        Raises:
            DataNotAvailableError: If zone is not supported, or if data is
                unavailable (e.g. beyond the 7-day retention window).
            AuthenticationError: If credentials are rejected.
            RateLimitError: If the API rate limit is exceeded.
            ExchangeAPIError: For unexpected API errors.
        """
        if zone not in _ZONE_TO_PRODUCT_ID:
            raise DataNotAvailableError(
                f"Bidding zone {zone!r} is not supported by the Nord Pool Auction API."
            )

        area = _ZONE_TO_AREA[zone]
        product_id = _ZONE_TO_PRODUCT_ID[zone]

        frames: list[pd.DataFrame] = []
        current = start
        while current <= end:
            df = self._fetch_day(area, product_id, current, currency)
            frames.append(df)
            current += datetime.timedelta(days=1)

        if not frames:
            return pd.DataFrame({"price_eur_mwh": pd.Series([], dtype=object)})

        combined = pd.concat(frames).sort_index()
        if resolution == Resolution.HOURLY:
            return _aggregate_qh_to_hourly(combined)
        return combined

    def _fetch_day(
        self,
        area: str,
        product_id: str,
        delivery_date: datetime.date,
        currency: str,
    ) -> pd.DataFrame:
        """Fetch day-ahead auction prices for a single delivery date.

        The auction closes for bidding at 12:00 CET on the day before delivery,
        so ``closeForBidDate = delivery_date - 1``.
        """
        close_for_bid_date = delivery_date - datetime.timedelta(days=1)
        auction_id = f"{product_id}-{close_for_bid_date.strftime('%Y%m%d')}"

        resp = self._http.get(
            f"{_AUCTION_BASE_URL}/auctions/{auction_id}/prices",
            headers=self._auth_headers(),
        )
        _raise_for_status(resp)

        data: list[dict[str, Any]] = resp.json()
        if not data:
            raise DataNotAvailableError(
                f"No data returned for auction {auction_id!r}. "
                "The Auction API only retains data for the past 7 days; "
                "use the Market Data API for older historical data."
            )

        return _parse_auction_prices_response(data, area, currency)


def _parse_auction_prices_response(
    data: list[dict[str, Any]],
    area: str,
    currency: str,
) -> pd.DataFrame:
    """Parse a Nord Pool Auction API prices response into a DataFrame.

    Args:
        data: Parsed JSON response body (a list; uses the first element).
        area: Nord Pool area code to extract prices for (e.g. ``"NO1"``).
        currency: Currency code to extract (e.g. ``"EUR"``).

    Returns:
        DataFrame with UTC DatetimeIndex and ``price_eur_mwh`` column at
        the native resolution of the auction (typically 15-minute for QH products).
    """
    contracts: list[dict[str, Any]] = data[0].get("contracts", [])
    timestamps: list[datetime.datetime] = []
    prices: list[Any] = []

    for contract in contracts:
        start_str: str = contract["deliveryStart"]
        ts = datetime.datetime.fromisoformat(start_str.replace("Z", "+00:00"))
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=datetime.UTC)

        price: Any = pd.NA
        for area_entry in contract.get("areas", []):
            if area_entry.get("areaCode") == area:
                for p in area_entry.get("prices", []):
                    if p.get("currencyCode") == currency:
                        raw = p.get("marketPrice")
                        if raw is not None:
                            price = Decimal(str(raw))
                        break
                break

        timestamps.append(ts)
        prices.append(price)

    index = pd.DatetimeIndex(timestamps)
    return pd.DataFrame({"price_eur_mwh": prices}, index=index)


def _aggregate_qh_to_hourly(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate quarter-hourly prices to hourly by averaging.

    Each clock-hour is represented by the average of its four 15-minute values.
    If all four values for a given hour are ``pd.NA``, the hourly value is ``pd.NA``.
    If only some values are ``pd.NA``, the average is computed over the non-NA values.

    Args:
        df: DataFrame with UTC DatetimeIndex at 15-minute resolution.

    Returns:
        DataFrame with UTC DatetimeIndex at hourly resolution.
    """
    if df.empty:
        return df

    dt_index = pd.DatetimeIndex(df.index)
    hourly_index = dt_index.floor("h")
    result_timestamps: list[datetime.datetime] = []
    result_prices: list[Any] = []

    for hour_ts in sorted(set(hourly_index)):
        mask = hourly_index == hour_ts
        hour_prices = [p for p in df.loc[mask, "price_eur_mwh"] if not pd.isna(p)]
        if hour_prices:
            avg: Any = sum(hour_prices) / len(hour_prices)
        else:
            avg = pd.NA
        result_timestamps.append(hour_ts)
        result_prices.append(avg)

    index = pd.DatetimeIndex(result_timestamps)
    return pd.DataFrame({"price_eur_mwh": result_prices}, index=index)
