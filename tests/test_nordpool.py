"""Tests for the Nord Pool client."""

from __future__ import annotations

import datetime
import json
from decimal import Decimal
from pathlib import Path

import httpx
import pandas as pd
import pytest
import respx

from nexa_marketdata.exceptions import (
    AuthenticationError,
    DataNotAvailableError,
    RateLimitError,
)
from nexa_marketdata.nordpool import NordPoolClient
from nexa_marketdata.types import BiddingZone, Resolution

_TOKEN_URL = "https://sts.nordpoolgroup.com/connect/token"
_PRICES_URL = "https://dataportal-api.nordpoolgroup.com/api/v2/Auction/Prices/ByAreas"

_TOKEN_RESPONSE = {
    "access_token": "test-access-token",
    "expires_in": 3600,
    "token_type": "Bearer",
}

_FIXTURE_PATH = Path(__file__).parent / "fixtures" / "nordpool_prices_response.json"


def _load_fixture() -> dict:  # type: ignore[type-arg]
    return json.loads(_FIXTURE_PATH.read_text())


def _make_prices_response(
    area: str,
    delivery_date: str,
    prices: list[str | None],
    resolution_hours: int = 1,
) -> dict:  # type: ignore[type-arg]
    """Build a minimal Nord Pool hourly/15min price response for one delivery date."""
    rows = []
    # CET is UTC+1 in January; delivery date midnight CET = day-1 23:00 UTC
    base = datetime.datetime(
        int(delivery_date[:4]),
        int(delivery_date[5:7]),
        int(delivery_date[8:10]),
        tzinfo=datetime.UTC,
    ) - datetime.timedelta(hours=1)

    step = datetime.timedelta(hours=resolution_hours)
    for i, price in enumerate(prices):
        start = base + step * i
        end = start + step
        area_state: dict[str, str | None] = {"area": area, "state": "Final"}
        area_state["value"] = price
        rows.append(
            {
                "startTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "endTime": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "areaStates": [area_state],
            }
        )
    return {
        "deliveryDateCET": delivery_date,
        "version": 3,
        "updatedAt": f"{delivery_date}T13:00:00Z",
        "deliveryAreas": [area],
        "market": "DayAhead",
        "multiContractRows": [],
        "rows": rows,
    }


@pytest.fixture
def client() -> NordPoolClient:
    return NordPoolClient(username="test-user", password="test-pass")


# ---------------------------------------------------------------------------
# Token and auth
# ---------------------------------------------------------------------------


def test_token_is_fetched_on_first_request(client: NordPoolClient) -> None:
    prices_data = _make_prices_response("NO1", "2025-01-01", ["45.00"] * 24)
    with respx.mock:
        token_route = respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(_PRICES_URL).mock(return_value=httpx.Response(200, json=prices_data))
        client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert token_route.call_count == 1


def test_token_is_cached_across_requests(client: NordPoolClient) -> None:
    prices_data = _make_prices_response("NO1", "2025-01-01", ["45.00"] * 24)
    with respx.mock:
        token_route = respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(_PRICES_URL).mock(return_value=httpx.Response(200, json=prices_data))
        client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
        client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert token_route.call_count == 1


def test_token_refresh_on_expiry(client: NordPoolClient) -> None:
    prices_data = _make_prices_response("NO1", "2025-01-01", ["45.00"] * 24)
    with respx.mock:
        token_route = respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(_PRICES_URL).mock(return_value=httpx.Response(200, json=prices_data))
        client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
        # Force token to appear expired
        assert client._token_expires_at is not None
        client._token_expires_at = datetime.datetime.now(
            tz=datetime.UTC
        ) - datetime.timedelta(seconds=1)
        client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert token_route.call_count == 2


def test_401_on_token_raises_authentication_error(client: NordPoolClient) -> None:
    with respx.mock:
        respx.post(_TOKEN_URL).mock(return_value=httpx.Response(401))
        with pytest.raises(AuthenticationError):
            client.day_ahead_prices(
                BiddingZone.NO1,
                datetime.date(2025, 1, 1),
                datetime.date(2025, 1, 1),
            )


# ---------------------------------------------------------------------------
# DataFrame structure
# ---------------------------------------------------------------------------


def test_day_ahead_prices_returns_dataframe(client: NordPoolClient) -> None:
    prices_data = _load_fixture()
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(_PRICES_URL).mock(return_value=httpx.Response(200, json=prices_data))
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert isinstance(df, pd.DataFrame)
    assert "price_eur_mwh" in df.columns
    assert len(df) == 24


def test_price_column_uses_decimal(client: NordPoolClient) -> None:
    prices_data = _load_fixture()
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(_PRICES_URL).mock(return_value=httpx.Response(200, json=prices_data))
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    non_na = df["price_eur_mwh"].dropna()
    assert all(isinstance(v, Decimal) for v in non_na)


def test_datetimeindex_is_utc_aware(client: NordPoolClient) -> None:
    prices_data = _load_fixture()
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(_PRICES_URL).mock(return_value=httpx.Response(200, json=prices_data))
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index.tz is not None
    assert str(df.index.tz) == "UTC"


def test_first_row_timestamp_is_utc(client: NordPoolClient) -> None:
    """For delivery date 2025-01-01 (CET=UTC+1), first row is 2024-12-31T23:00Z."""
    prices_data = _load_fixture()
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(_PRICES_URL).mock(return_value=httpx.Response(200, json=prices_data))
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    first_ts = df.index[0]
    assert first_ts == pd.Timestamp("2024-12-31T23:00:00", tz="UTC")


def test_price_values_match_fixture(client: NordPoolClient) -> None:
    prices_data = _load_fixture()
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(_PRICES_URL).mock(return_value=httpx.Response(200, json=prices_data))
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert df["price_eur_mwh"].iloc[0] == Decimal("45.23")
    assert df["price_eur_mwh"].iloc[-1] == Decimal("48.76")


# ---------------------------------------------------------------------------
# Multi-day range
# ---------------------------------------------------------------------------


def test_multi_day_range_makes_one_request_per_day(client: NordPoolClient) -> None:
    day1 = _make_prices_response("NO1", "2025-01-01", ["45.00"] * 24)
    day2 = _make_prices_response("NO1", "2025-01-02", ["46.00"] * 24)
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        prices_route = respx.get(_PRICES_URL).mock(
            side_effect=[
                httpx.Response(200, json=day1),
                httpx.Response(200, json=day2),
            ]
        )
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 2),
        )
    assert prices_route.call_count == 2
    assert len(df) == 48


def test_multi_day_result_is_sorted_by_timestamp(client: NordPoolClient) -> None:
    day1 = _make_prices_response("NO1", "2025-01-01", ["45.00"] * 24)
    day2 = _make_prices_response("NO1", "2025-01-02", ["46.00"] * 24)
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(_PRICES_URL).mock(
            side_effect=[
                httpx.Response(200, json=day1),
                httpx.Response(200, json=day2),
            ]
        )
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 2),
        )
    assert df.index.is_monotonic_increasing


# ---------------------------------------------------------------------------
# Missing / NA values
# ---------------------------------------------------------------------------


def test_missing_prices_become_na(client: NordPoolClient) -> None:
    prices = ["45.00"] * 12 + [None] + ["46.00"] * 11
    prices_data = _make_prices_response("NO1", "2025-01-01", prices)  # type: ignore[arg-type]
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(_PRICES_URL).mock(return_value=httpx.Response(200, json=prices_data))
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert pd.isna(df["price_eur_mwh"].iloc[12])
    assert df["price_eur_mwh"].iloc[11] == Decimal("45.00")
    assert df["price_eur_mwh"].iloc[13] == Decimal("46.00")


def test_missing_string_prices_become_na(client: NordPoolClient) -> None:
    prices_data = _make_prices_response("NO1", "2025-01-01", ["45.00"] * 24)
    # Inject a "Missing" sentinel into one row
    prices_data["rows"][5]["areaStates"][0]["value"] = "Missing"
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(_PRICES_URL).mock(return_value=httpx.Response(200, json=prices_data))
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert pd.isna(df["price_eur_mwh"].iloc[5])


# ---------------------------------------------------------------------------
# HTTP error mapping
# ---------------------------------------------------------------------------


def test_401_raises_authentication_error(client: NordPoolClient) -> None:
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(_PRICES_URL).mock(return_value=httpx.Response(401))
        with pytest.raises(AuthenticationError):
            client.day_ahead_prices(
                BiddingZone.NO1,
                datetime.date(2025, 1, 1),
                datetime.date(2025, 1, 1),
            )


def test_429_raises_rate_limit_error(client: NordPoolClient) -> None:
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(_PRICES_URL).mock(return_value=httpx.Response(429))
        with pytest.raises(RateLimitError):
            client.day_ahead_prices(
                BiddingZone.NO1,
                datetime.date(2025, 1, 1),
                datetime.date(2025, 1, 1),
            )


def test_400_raises_data_not_available_error(client: NordPoolClient) -> None:
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(_PRICES_URL).mock(return_value=httpx.Response(400))
        with pytest.raises(DataNotAvailableError):
            client.day_ahead_prices(
                BiddingZone.NO1,
                datetime.date(2025, 1, 1),
                datetime.date(2025, 1, 1),
            )


def test_404_raises_data_not_available_error(client: NordPoolClient) -> None:
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(_PRICES_URL).mock(return_value=httpx.Response(404))
        with pytest.raises(DataNotAvailableError):
            client.day_ahead_prices(
                BiddingZone.NO1,
                datetime.date(2025, 1, 1),
                datetime.date(2025, 1, 1),
            )


# ---------------------------------------------------------------------------
# Unsupported zone
# ---------------------------------------------------------------------------


def test_unsupported_zone_raises_data_not_available(client: NordPoolClient) -> None:
    with pytest.raises(DataNotAvailableError, match="not supported on Nord Pool"):
        client.day_ahead_prices(
            BiddingZone.GB,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )


def test_unsupported_zone_does_not_make_http_requests(
    client: NordPoolClient,
) -> None:
    with respx.mock:
        with pytest.raises(DataNotAvailableError):
            client.day_ahead_prices(
                BiddingZone.GB,
                datetime.date(2025, 1, 1),
                datetime.date(2025, 1, 1),
            )
        assert not respx.calls


# ---------------------------------------------------------------------------
# Resolution
# ---------------------------------------------------------------------------


def test_hourly_resolution_sends_pt60m_param(client: NordPoolClient) -> None:
    prices_data = _make_prices_response("NO1", "2025-01-01", ["45.00"] * 24)
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        prices_route = respx.get(_PRICES_URL).mock(
            return_value=httpx.Response(200, json=prices_data)
        )
        client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
            resolution=Resolution.HOURLY,
        )
    request = prices_route.calls[0].request
    assert "PT60M" in str(request.url)


def test_15min_resolution_sends_pt15m_param(client: NordPoolClient) -> None:
    prices_data = _make_prices_response(
        "NO1", "2025-01-01", ["45.00"] * 96, resolution_hours=1
    )
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        prices_route = respx.get(_PRICES_URL).mock(
            return_value=httpx.Response(200, json=prices_data)
        )
        client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
            resolution=Resolution.MINUTES_15,
        )
    request = prices_route.calls[0].request
    assert "PT15M" in str(request.url)


# ---------------------------------------------------------------------------
# Zone mapping
# ---------------------------------------------------------------------------


def test_de_lu_zone_maps_to_ger(client: NordPoolClient) -> None:
    prices_data = _make_prices_response("GER", "2025-01-01", ["85.00"] * 24)
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        prices_route = respx.get(_PRICES_URL).mock(
            return_value=httpx.Response(200, json=prices_data)
        )
        client.day_ahead_prices(
            BiddingZone.DE_LU,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    request = prices_route.calls[0].request
    assert "GER" in str(request.url)
