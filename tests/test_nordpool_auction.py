"""Tests for the Nord Pool Auction API client."""

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
from nexa_marketdata.nordpool import (
    NordPoolAuctionClient,
    _aggregate_qh_to_hourly,
    _parse_auction_prices_response,
)
from nexa_marketdata.types import BiddingZone, Resolution

_TOKEN_URL = "https://sts.nordpoolgroup.com/connect/token"
_AUCTION_BASE_URL = "https://auctions-api.nordpoolgroup.com/api/v1"

_TOKEN_RESPONSE = {
    "access_token": "test-access-token",
    "expires_in": 3600,
    "token_type": "Bearer",
}

_FIXTURE_PATH = (
    Path(__file__).parent / "fixtures" / "nordpool_auction_prices_response.json"
)


def _load_fixture() -> list:  # type: ignore[type-arg]
    return json.loads(_FIXTURE_PATH.read_text())


def _make_auction_response(
    area: str,
    delivery_date: datetime.date,
    prices: list[float | None],
    resolution_minutes: int = 15,
) -> list:  # type: ignore[type-arg]
    """Build a minimal Nord Pool Auction API price response.

    Args:
        area: Area code (e.g. ``"NO1"``).
        delivery_date: Delivery date; first contract starts at 23:00 UTC on D-1.
        prices: List of prices (``None`` maps to ``null`` / ``pd.NA``).
        resolution_minutes: Contract duration in minutes (15 or 60).
    """
    base = datetime.datetime(
        delivery_date.year,
        delivery_date.month,
        delivery_date.day,
        tzinfo=datetime.UTC,
    ) - datetime.timedelta(hours=1)
    step = datetime.timedelta(minutes=resolution_minutes)
    product_id = "NOR_QH_DA_1"
    close_date = (delivery_date - datetime.timedelta(days=1)).strftime("%Y%m%d")
    auction_id = f"{product_id}-{close_date}"

    contracts = []
    for i, price in enumerate(prices):
        start = base + step * i
        end = start + step
        contracts.append(
            {
                "contractId": (
                    f"{product_id}-{delivery_date.strftime('%Y%m%d')}-{i + 1:02d}"
                ),
                "deliveryStart": start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "deliveryEnd": end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "areas": [
                    {
                        "areaCode": area,
                        "prices": [
                            {
                                "currencyCode": "EUR",
                                "marketPrice": price,
                                "status": "Final",
                            }
                        ],
                    }
                ],
            }
        )

    return [
        {
            "auction": auction_id,
            "auctionDeliveryStart": base.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
            "auctionDeliveryEnd": (base + step * len(prices)).strftime(
                "%Y-%m-%dT%H:%M:%S.000Z"
            ),
            "contracts": contracts,
        }
    ]


def _auction_url(auction_id: str) -> str:
    return f"{_AUCTION_BASE_URL}/auctions/{auction_id}/prices"


@pytest.fixture
def client() -> NordPoolAuctionClient:
    return NordPoolAuctionClient(username="test-user", password="test-pass")


# ---------------------------------------------------------------------------
# Token and auth
# ---------------------------------------------------------------------------


def test_token_is_fetched_on_first_request(client: NordPoolAuctionClient) -> None:
    auction_data = _make_auction_response("NO1", datetime.date(2025, 1, 1), [45.0] * 96)
    with respx.mock:
        token_route = respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(200, json=auction_data)
        )
        client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert token_route.call_count == 1


def test_token_uses_auction_credentials(client: NordPoolAuctionClient) -> None:
    """Token request must use client_auction_api credentials and auction_api scope."""
    auction_data = _make_auction_response("NO1", datetime.date(2025, 1, 1), [45.0] * 96)
    with respx.mock:
        token_route = respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(200, json=auction_data)
        )
        client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    request = token_route.calls[0].request
    body = request.content.decode()
    assert "auction_api" in body
    # Basic auth header should use client_auction_api
    import base64

    auth_header = request.headers.get("authorization", "")
    decoded = base64.b64decode(auth_header.split(" ")[1]).decode()
    assert decoded == "client_auction_api:client_auction_api"


def test_token_is_cached_across_requests(client: NordPoolAuctionClient) -> None:
    auction_data = _make_auction_response("NO1", datetime.date(2025, 1, 1), [45.0] * 96)
    with respx.mock:
        token_route = respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(200, json=auction_data)
        )
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


def test_token_refresh_on_expiry(client: NordPoolAuctionClient) -> None:
    auction_data = _make_auction_response("NO1", datetime.date(2025, 1, 1), [45.0] * 96)
    with respx.mock:
        token_route = respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(200, json=auction_data)
        )
        client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
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


def test_401_on_token_raises_authentication_error(
    client: NordPoolAuctionClient,
) -> None:
    with respx.mock:
        respx.post(_TOKEN_URL).mock(return_value=httpx.Response(401))
        with pytest.raises(AuthenticationError):
            client.day_ahead_prices(
                BiddingZone.NO1,
                datetime.date(2025, 1, 1),
                datetime.date(2025, 1, 1),
            )


# ---------------------------------------------------------------------------
# Auction ID construction
# ---------------------------------------------------------------------------


def test_auction_id_uses_close_for_bid_date(client: NordPoolAuctionClient) -> None:
    """For delivery 2025-01-01, closeForBidDate=2024-12-31 → NOR_QH_DA_1-20241231."""
    auction_data = _make_auction_response("NO1", datetime.date(2025, 1, 1), [45.0] * 96)
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        prices_route = respx.get(_auction_url("NOR_QH_DA_1-20241231")).mock(
            return_value=httpx.Response(200, json=auction_data)
        )
        client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert prices_route.call_count == 1


def test_auction_id_for_different_delivery_date(client: NordPoolAuctionClient) -> None:
    """For delivery 2025-06-15, closeForBidDate=2025-06-14 → NOR_QH_DA_1-20250614."""
    auction_data = _make_auction_response(
        "NO1", datetime.date(2025, 6, 15), [50.0] * 96
    )
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        prices_route = respx.get(_auction_url("NOR_QH_DA_1-20250614")).mock(
            return_value=httpx.Response(200, json=auction_data)
        )
        client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 6, 15),
            datetime.date(2025, 6, 15),
        )
    assert prices_route.call_count == 1


# ---------------------------------------------------------------------------
# DataFrame structure
# ---------------------------------------------------------------------------


def test_day_ahead_prices_returns_dataframe(client: NordPoolAuctionClient) -> None:
    fixture = _load_fixture()
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(200, json=fixture)
        )
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert isinstance(df, pd.DataFrame)
    assert "price_eur_mwh" in df.columns


def test_hourly_resolution_returns_24_rows(client: NordPoolAuctionClient) -> None:
    """96 QH contracts aggregated to hourly should give 24 rows."""
    fixture = _load_fixture()
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(200, json=fixture)
        )
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
            resolution=Resolution.HOURLY,
        )
    assert len(df) == 24


def test_15min_resolution_returns_96_rows(client: NordPoolAuctionClient) -> None:
    """Native QH product returns 96 contracts for a standard day."""
    fixture = _load_fixture()
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(200, json=fixture)
        )
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
            resolution=Resolution.MINUTES_15,
        )
    assert len(df) == 96


def test_price_column_uses_decimal(client: NordPoolAuctionClient) -> None:
    fixture = _load_fixture()
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(200, json=fixture)
        )
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
            resolution=Resolution.MINUTES_15,
        )
    non_na = df["price_eur_mwh"].dropna()
    assert all(isinstance(v, Decimal) for v in non_na)


def test_datetimeindex_is_utc_aware(client: NordPoolAuctionClient) -> None:
    fixture = _load_fixture()
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(200, json=fixture)
        )
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index.tz is not None
    assert str(df.index.tz) == "UTC"


def test_first_qh_timestamp_is_utc(client: NordPoolAuctionClient) -> None:
    """For delivery 2025-01-01 (CET=UTC+1), first QH starts 2024-12-31T23:00Z."""
    fixture = _load_fixture()
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(200, json=fixture)
        )
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
            resolution=Resolution.MINUTES_15,
        )
    assert df.index[0] == pd.Timestamp("2024-12-31T23:00:00", tz="UTC")


def test_first_hourly_timestamp_is_utc(client: NordPoolAuctionClient) -> None:
    """Aggregated hourly first row should still be 2024-12-31T23:00Z."""
    fixture = _load_fixture()
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(200, json=fixture)
        )
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
            resolution=Resolution.HOURLY,
        )
    assert df.index[0] == pd.Timestamp("2024-12-31T23:00:00", tz="UTC")


def test_price_values_match_fixture(client: NordPoolAuctionClient) -> None:
    """First and last QH contracts match fixture-defined prices."""
    fixture = _load_fixture()
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(200, json=fixture)
        )
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
            resolution=Resolution.MINUTES_15,
        )
    assert df["price_eur_mwh"].iloc[0] == Decimal("45.23")
    assert df["price_eur_mwh"].iloc[95] == Decimal("48.76")


# ---------------------------------------------------------------------------
# Area filtering
# ---------------------------------------------------------------------------


def test_area_filtering_picks_correct_zone(client: NordPoolAuctionClient) -> None:
    """Parser extracts prices for the requested area only."""
    no1_price = 45.0
    no2_price = 50.0
    delivery_date = datetime.date(2025, 1, 1)
    base = datetime.datetime(2024, 12, 31, 23, 0, 0, tzinfo=datetime.UTC)
    step = datetime.timedelta(minutes=15)

    contracts = []
    for i in range(96):
        start = base + step * i
        end = start + step
        contracts.append(
            {
                "contractId": f"NOR_QH_DA_1-20250101-{i + 1:02d}",
                "deliveryStart": start.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "deliveryEnd": end.strftime("%Y-%m-%dT%H:%M:%S.000Z"),
                "areas": [
                    {
                        "areaCode": "NO1",
                        "prices": [
                            {
                                "currencyCode": "EUR",
                                "marketPrice": no1_price,
                                "status": "Final",
                            }
                        ],
                    },
                    {
                        "areaCode": "NO2",
                        "prices": [
                            {
                                "currencyCode": "EUR",
                                "marketPrice": no2_price,
                                "status": "Final",
                            }
                        ],
                    },
                ],
            }
        )

    data = [
        {
            "auction": "NOR_QH_DA_1-20241231",
            "auctionDeliveryStart": "2024-12-31T23:00:00.000Z",
            "auctionDeliveryEnd": "2025-01-01T23:00:00.000Z",
            "contracts": contracts,
        }
    ]

    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(200, json=data)
        )
        df_no1 = client.day_ahead_prices(
            BiddingZone.NO1,
            delivery_date,
            delivery_date,
            resolution=Resolution.MINUTES_15,
        )

    assert all(df_no1["price_eur_mwh"].dropna() == Decimal("45.0"))


# ---------------------------------------------------------------------------
# Missing / NA values
# ---------------------------------------------------------------------------


def test_null_market_price_becomes_na(client: NordPoolAuctionClient) -> None:
    """null marketPrice in JSON maps to pd.NA."""
    fixture = _load_fixture()
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(200, json=fixture)
        )
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
            resolution=Resolution.MINUTES_15,
        )
    # Fixture index 4 has marketPrice: null
    assert pd.isna(df["price_eur_mwh"].iloc[4])
    assert df["price_eur_mwh"].iloc[3] is not pd.NA
    assert df["price_eur_mwh"].iloc[5] is not pd.NA


def test_negative_prices_are_valid(client: NordPoolAuctionClient) -> None:
    """Negative market prices are accepted (common in some markets)."""
    fixture = _load_fixture()
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(200, json=fixture)
        )
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
            resolution=Resolution.MINUTES_15,
        )
    # Fixture index 7 has marketPrice: -5.50
    assert df["price_eur_mwh"].iloc[7] == Decimal("-5.50")


# ---------------------------------------------------------------------------
# Hourly aggregation
# ---------------------------------------------------------------------------


def test_hourly_aggregation_averages_four_qh_values() -> None:
    """_aggregate_qh_to_hourly averages 4 QH values per clock-hour."""
    base = pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
    index = pd.date_range(start=base, periods=4, freq="15min")
    df = pd.DataFrame(
        {"price_eur_mwh": [Decimal("10"), Decimal("20"), Decimal("30"), Decimal("40")]},
        index=index,
    )
    result = _aggregate_qh_to_hourly(df)
    assert len(result) == 1
    assert result["price_eur_mwh"].iloc[0] == Decimal("25")


def test_hourly_aggregation_partial_na_uses_available_values() -> None:
    """If only some QH values are NA, the average uses the non-NA values."""
    base = pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
    index = pd.date_range(start=base, periods=4, freq="15min")
    df = pd.DataFrame(
        {"price_eur_mwh": [Decimal("10"), pd.NA, Decimal("30"), pd.NA]},
        index=index,
    )
    result = _aggregate_qh_to_hourly(df)
    assert result["price_eur_mwh"].iloc[0] == Decimal("20")


def test_hourly_aggregation_all_na_stays_na() -> None:
    """If all four QH values are NA, the hourly value is pd.NA."""
    base = pd.Timestamp("2025-01-01 00:00:00", tz="UTC")
    index = pd.date_range(start=base, periods=4, freq="15min")
    df = pd.DataFrame({"price_eur_mwh": [pd.NA, pd.NA, pd.NA, pd.NA]}, index=index)
    result = _aggregate_qh_to_hourly(df)
    assert pd.isna(result["price_eur_mwh"].iloc[0])


def test_hourly_aggregation_empty_dataframe() -> None:
    """Empty DataFrame passes through unchanged."""
    df = pd.DataFrame({"price_eur_mwh": pd.Series([], dtype=object)})
    result = _aggregate_qh_to_hourly(df)
    assert result.empty


# ---------------------------------------------------------------------------
# Multi-day range
# ---------------------------------------------------------------------------


def test_multi_day_range_makes_one_request_per_day(
    client: NordPoolAuctionClient,
) -> None:
    day1 = _make_auction_response("NO1", datetime.date(2025, 1, 1), [45.0] * 96)
    day2 = _make_auction_response("NO1", datetime.date(2025, 1, 2), [46.0] * 96)
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        prices_route = respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            side_effect=[
                httpx.Response(200, json=day1),
                httpx.Response(200, json=day2),
            ]
        )
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 2),
            resolution=Resolution.MINUTES_15,
        )
    assert prices_route.call_count == 2
    assert len(df) == 192  # 96 × 2


def test_multi_day_correct_auction_ids(client: NordPoolAuctionClient) -> None:
    """Each delivery date uses closeForBidDate = deliveryDate - 1."""
    day1 = _make_auction_response("NO1", datetime.date(2025, 1, 1), [45.0] * 96)
    day2 = _make_auction_response("NO1", datetime.date(2025, 1, 2), [46.0] * 96)
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        route_day1 = respx.get(_auction_url("NOR_QH_DA_1-20241231")).mock(
            return_value=httpx.Response(200, json=day1)
        )
        route_day2 = respx.get(_auction_url("NOR_QH_DA_1-20250101")).mock(
            return_value=httpx.Response(200, json=day2)
        )
        client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 2),
        )
    assert route_day1.call_count == 1
    assert route_day2.call_count == 1


def test_multi_day_result_is_sorted_by_timestamp(
    client: NordPoolAuctionClient,
) -> None:
    day1 = _make_auction_response("NO1", datetime.date(2025, 1, 1), [45.0] * 96)
    day2 = _make_auction_response("NO1", datetime.date(2025, 1, 2), [46.0] * 96)
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            side_effect=[
                httpx.Response(200, json=day1),
                httpx.Response(200, json=day2),
            ]
        )
        df = client.day_ahead_prices(
            BiddingZone.NO1,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 2),
            resolution=Resolution.MINUTES_15,
        )
    assert df.index.is_monotonic_increasing


# ---------------------------------------------------------------------------
# HTTP error mapping
# ---------------------------------------------------------------------------


def test_401_raises_authentication_error(client: NordPoolAuctionClient) -> None:
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(401)
        )
        with pytest.raises(AuthenticationError):
            client.day_ahead_prices(
                BiddingZone.NO1,
                datetime.date(2025, 1, 1),
                datetime.date(2025, 1, 1),
            )


def test_429_raises_rate_limit_error(client: NordPoolAuctionClient) -> None:
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(429)
        )
        with pytest.raises(RateLimitError):
            client.day_ahead_prices(
                BiddingZone.NO1,
                datetime.date(2025, 1, 1),
                datetime.date(2025, 1, 1),
            )


def test_400_raises_data_not_available_error(client: NordPoolAuctionClient) -> None:
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(400)
        )
        with pytest.raises(DataNotAvailableError):
            client.day_ahead_prices(
                BiddingZone.NO1,
                datetime.date(2025, 1, 1),
                datetime.date(2025, 1, 1),
            )


def test_404_raises_data_not_available_error(client: NordPoolAuctionClient) -> None:
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(404)
        )
        with pytest.raises(DataNotAvailableError):
            client.day_ahead_prices(
                BiddingZone.NO1,
                datetime.date(2025, 1, 1),
                datetime.date(2025, 1, 1),
            )


def test_empty_response_raises_data_not_available_error(
    client: NordPoolAuctionClient,
) -> None:
    """Empty array response (e.g. beyond 7-day window) raises DataNotAvailableError."""
    with respx.mock:
        respx.post(_TOKEN_URL).mock(
            return_value=httpx.Response(200, json=_TOKEN_RESPONSE)
        )
        respx.get(url__startswith=_AUCTION_BASE_URL).mock(
            return_value=httpx.Response(200, json=[])
        )
        with pytest.raises(DataNotAvailableError, match="7 days"):
            client.day_ahead_prices(
                BiddingZone.NO1,
                datetime.date(2024, 1, 1),  # well outside 7-day window
                datetime.date(2024, 1, 1),
            )


# ---------------------------------------------------------------------------
# Unsupported zones
# ---------------------------------------------------------------------------


def test_unsupported_zone_raises_data_not_available(
    client: NordPoolAuctionClient,
) -> None:
    with pytest.raises(DataNotAvailableError, match="not supported"):
        client.day_ahead_prices(
            BiddingZone.GB,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )


def test_de_lu_raises_data_not_available(client: NordPoolAuctionClient) -> None:
    """DE_LU is excluded from Auction API support (CWE TSO area codes unresolved)."""
    with pytest.raises(DataNotAvailableError, match="not supported"):
        client.day_ahead_prices(
            BiddingZone.DE_LU,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )


def test_unsupported_zone_does_not_make_http_requests(
    client: NordPoolAuctionClient,
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
# Parser unit tests
# ---------------------------------------------------------------------------


def test_parse_auction_prices_response_extracts_correct_area() -> None:
    """Parser ignores areas that don't match the requested area code."""
    data = [
        {
            "auction": "NOR_QH_DA_1-20241231",
            "contracts": [
                {
                    "contractId": "c1",
                    "deliveryStart": "2024-12-31T23:00:00.000Z",
                    "deliveryEnd": "2024-12-31T23:15:00.000Z",
                    "areas": [
                        {
                            "areaCode": "NO1",
                            "prices": [
                                {
                                    "currencyCode": "EUR",
                                    "marketPrice": 45.0,
                                    "status": "Final",
                                }
                            ],
                        },
                        {
                            "areaCode": "NO2",
                            "prices": [
                                {
                                    "currencyCode": "EUR",
                                    "marketPrice": 99.0,
                                    "status": "Final",
                                }
                            ],
                        },
                    ],
                }
            ],
        }
    ]
    df = _parse_auction_prices_response(data, area="NO1", currency="EUR")
    assert df["price_eur_mwh"].iloc[0] == Decimal("45.0")


def test_parse_auction_prices_response_extracts_correct_currency() -> None:
    """Parser ignores currencies that don't match the requested currency code."""
    data = [
        {
            "auction": "NOR_QH_DA_1-20241231",
            "contracts": [
                {
                    "contractId": "c1",
                    "deliveryStart": "2024-12-31T23:00:00.000Z",
                    "deliveryEnd": "2024-12-31T23:15:00.000Z",
                    "areas": [
                        {
                            "areaCode": "NO1",
                            "prices": [
                                {
                                    "currencyCode": "NOK",
                                    "marketPrice": 500.0,
                                    "status": "Final",
                                },
                                {
                                    "currencyCode": "EUR",
                                    "marketPrice": 45.0,
                                    "status": "Final",
                                },
                            ],
                        }
                    ],
                }
            ],
        }
    ]
    df = _parse_auction_prices_response(data, area="NO1", currency="EUR")
    assert df["price_eur_mwh"].iloc[0] == Decimal("45.0")


def test_parse_auction_prices_response_null_becomes_na() -> None:
    data = [
        {
            "auction": "NOR_QH_DA_1-20241231",
            "contracts": [
                {
                    "contractId": "c1",
                    "deliveryStart": "2024-12-31T23:00:00.000Z",
                    "deliveryEnd": "2024-12-31T23:15:00.000Z",
                    "areas": [
                        {
                            "areaCode": "NO1",
                            "prices": [
                                {
                                    "currencyCode": "EUR",
                                    "marketPrice": None,
                                    "status": "Final",
                                }
                            ],
                        }
                    ],
                }
            ],
        }
    ]
    df = _parse_auction_prices_response(data, area="NO1", currency="EUR")
    assert pd.isna(df["price_eur_mwh"].iloc[0])
