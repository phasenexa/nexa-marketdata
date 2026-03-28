"""Tests for the ENTSO-E Transparency Platform client."""

from __future__ import annotations

import datetime
import os
from decimal import Decimal
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from nexa_marketdata.entsoe import ENTSOEClient
from nexa_marketdata.exceptions import (
    AuthenticationError,
    DataNotAvailableError,
    ExchangeAPIError,
    RateLimitError,
)
from nexa_marketdata.types import BiddingZone

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_LIVE = pytest.mark.skipif(
    not os.environ.get("ENTSOE_API_KEY"),
    reason="ENTSOE_API_KEY environment variable not set",
)


def _make_price_series(
    start: str = "2025-01-01 00:00",
    periods: int = 24,
    freq: str = "h",
    price: float = 50.0,
    tz: str = "Europe/Brussels",
) -> pd.Series:  # type: ignore[type-arg]
    """Build a minimal price Series mimicking entsoe-py output."""
    idx = pd.date_range(start, periods=periods, freq=freq, tz=tz)
    return pd.Series([price] * periods, index=idx, dtype=float)


@pytest.fixture
def client() -> ENTSOEClient:
    return ENTSOEClient(api_key="test-api-key-0000-0000-0000")


# ---------------------------------------------------------------------------
# DataFrame structure
# ---------------------------------------------------------------------------


def test_day_ahead_prices_returns_dataframe(client: ENTSOEClient) -> None:
    series = _make_price_series()
    with patch.object(client._client, "query_day_ahead_prices", return_value=series):
        df = client.day_ahead_prices(
            BiddingZone.DE_LU,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert isinstance(df, pd.DataFrame)
    assert "price_eur_mwh" in df.columns
    assert len(df) == 24


def test_price_column_uses_decimal(client: ENTSOEClient) -> None:
    series = _make_price_series(price=75.50)
    with patch.object(client._client, "query_day_ahead_prices", return_value=series):
        df = client.day_ahead_prices(
            BiddingZone.FR,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    non_na = df["price_eur_mwh"].dropna()
    assert all(isinstance(v, Decimal) for v in non_na)


def test_datetimeindex_is_utc_aware(client: ENTSOEClient) -> None:
    series = _make_price_series()
    with patch.object(client._client, "query_day_ahead_prices", return_value=series):
        df = client.day_ahead_prices(
            BiddingZone.BE,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index.tz is not None
    assert str(df.index.tz) == "UTC"


def test_non_utc_source_index_is_converted_to_utc(client: ENTSOEClient) -> None:
    """Index from entsoe-py (e.g. Europe/Brussels) must be normalised to UTC."""
    series = _make_price_series(start="2025-01-01 00:00", tz="Europe/Brussels")
    with patch.object(client._client, "query_day_ahead_prices", return_value=series):
        df = client.day_ahead_prices(
            BiddingZone.BE,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert str(df.index.tz) == "UTC"
    # 2025-01-01 00:00 Europe/Brussels (UTC+1) == 2024-12-31 23:00 UTC
    assert df.index[0] == pd.Timestamp("2024-12-31 23:00:00", tz="UTC")


def test_price_values_are_correct(client: ENTSOEClient) -> None:
    series = _make_price_series(price=99.99)
    with patch.object(client._client, "query_day_ahead_prices", return_value=series):
        df = client.day_ahead_prices(
            BiddingZone.FR,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert df["price_eur_mwh"].iloc[0] == Decimal("99.99")
    assert df["price_eur_mwh"].iloc[-1] == Decimal("99.99")


def test_nan_values_become_na(client: ENTSOEClient) -> None:
    prices = [50.0] * 12 + [float("nan")] + [51.0] * 11
    idx = pd.date_range("2025-01-01 00:00", periods=24, freq="h", tz="UTC")
    series = pd.Series(prices, index=idx, dtype=float)
    with patch.object(client._client, "query_day_ahead_prices", return_value=series):
        df = client.day_ahead_prices(
            BiddingZone.NL,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    assert pd.isna(df["price_eur_mwh"].iloc[12])
    assert df["price_eur_mwh"].iloc[11] == Decimal("50.0")
    assert df["price_eur_mwh"].iloc[13] == Decimal("51.0")


# ---------------------------------------------------------------------------
# Date range / query parameters
# ---------------------------------------------------------------------------


def test_query_uses_inclusive_end_date(client: ENTSOEClient) -> None:
    """end date must be converted to an exclusive bound (start of next day)."""
    series = _make_price_series(periods=48, tz="UTC")
    mock_query = MagicMock(return_value=series)
    with patch.object(client._client, "query_day_ahead_prices", mock_query):
        client.day_ahead_prices(
            BiddingZone.AT,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 2),
        )
    _, kwargs = mock_query.call_args
    assert kwargs["start"] == pd.Timestamp("2025-01-01", tz="UTC")
    assert kwargs["end"] == pd.Timestamp("2025-01-03", tz="UTC")


def test_query_passes_correct_area_for_gb(client: ENTSOEClient) -> None:
    series = _make_price_series(tz="UTC")
    mock_query = MagicMock(return_value=series)
    with patch.object(client._client, "query_day_ahead_prices", mock_query):
        client.day_ahead_prices(
            BiddingZone.GB,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    area_arg = mock_query.call_args[0][0]
    assert area_arg == "GB"


def test_query_passes_correct_area_for_ch(client: ENTSOEClient) -> None:
    series = _make_price_series(tz="UTC")
    mock_query = MagicMock(return_value=series)
    with patch.object(client._client, "query_day_ahead_prices", mock_query):
        client.day_ahead_prices(
            BiddingZone.CH,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )
    area_arg = mock_query.call_args[0][0]
    assert area_arg == "CH"


# ---------------------------------------------------------------------------
# Error handling
# ---------------------------------------------------------------------------


def test_no_matching_data_raises_data_not_available(client: ENTSOEClient) -> None:
    from entsoe.exceptions import NoMatchingDataError  # type: ignore[import-untyped]

    with (
        patch.object(
            client._client,
            "query_day_ahead_prices",
            side_effect=NoMatchingDataError("no data"),
        ),
        pytest.raises(DataNotAvailableError, match="No day-ahead prices"),
    ):
        client.day_ahead_prices(
            BiddingZone.FR,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )


def test_http_401_raises_authentication_error(client: ENTSOEClient) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 401
    exc = requests.exceptions.HTTPError(response=mock_response)
    with (
        patch.object(client._client, "query_day_ahead_prices", side_effect=exc),
        pytest.raises(AuthenticationError),
    ):
        client.day_ahead_prices(
            BiddingZone.FR,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )


def test_http_403_raises_authentication_error(client: ENTSOEClient) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 403
    exc = requests.exceptions.HTTPError(response=mock_response)
    with (
        patch.object(client._client, "query_day_ahead_prices", side_effect=exc),
        pytest.raises(AuthenticationError),
    ):
        client.day_ahead_prices(
            BiddingZone.FR,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )


def test_http_429_raises_rate_limit_error(client: ENTSOEClient) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 429
    exc = requests.exceptions.HTTPError(response=mock_response)
    with (
        patch.object(client._client, "query_day_ahead_prices", side_effect=exc),
        pytest.raises(RateLimitError),
    ):
        client.day_ahead_prices(
            BiddingZone.FR,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )


def test_http_500_raises_exchange_api_error(client: ENTSOEClient) -> None:
    mock_response = MagicMock()
    mock_response.status_code = 500
    exc = requests.exceptions.HTTPError(response=mock_response)
    with (
        patch.object(client._client, "query_day_ahead_prices", side_effect=exc),
        pytest.raises(ExchangeAPIError),
    ):
        client.day_ahead_prices(
            BiddingZone.FR,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )


def test_unexpected_exception_raises_exchange_api_error(client: ENTSOEClient) -> None:
    with (
        patch.object(
            client._client,
            "query_day_ahead_prices",
            side_effect=ValueError("unexpected"),
        ),
        pytest.raises(ExchangeAPIError, match="ENTSO-E API error"),
    ):
        client.day_ahead_prices(
            BiddingZone.FR,
            datetime.date(2025, 1, 1),
            datetime.date(2025, 1, 1),
        )


def test_unsupported_zone_raises_data_not_available(client: ENTSOEClient) -> None:
    from nexa_marketdata.entsoe import _ZONE_TO_AREA

    assert BiddingZone.FR in _ZONE_TO_AREA  # sanity check
    backup = _ZONE_TO_AREA.pop(BiddingZone.FR)
    try:
        with pytest.raises(DataNotAvailableError, match="not supported on ENTSO-E"):
            client.day_ahead_prices(
                BiddingZone.FR,
                datetime.date(2025, 1, 1),
                datetime.date(2025, 1, 1),
            )
    finally:
        _ZONE_TO_AREA[BiddingZone.FR] = backup


# ---------------------------------------------------------------------------
# Integration tests — skipped unless ENTSOE_API_KEY is set
# ---------------------------------------------------------------------------


@_LIVE
def test_live_connection_returns_dataframe() -> None:
    """Verify the live ENTSO-E API connection returns valid day-ahead prices."""
    api_key = os.environ["ENTSOE_API_KEY"]
    live_client = ENTSOEClient(api_key=api_key)

    # France is reliably covered; use a fixed historical date to avoid
    # availability gaps near the publication cut-off.
    df = live_client.day_ahead_prices(
        BiddingZone.FR,
        start=datetime.date(2025, 1, 1),
        end=datetime.date(2025, 1, 1),
    )

    assert isinstance(df, pd.DataFrame)
    assert "price_eur_mwh" in df.columns
    assert len(df) > 0


@_LIVE
def test_live_connection_utc_index() -> None:
    """Live response must have a UTC-aware DatetimeIndex."""
    api_key = os.environ["ENTSOE_API_KEY"]
    live_client = ENTSOEClient(api_key=api_key)

    df = live_client.day_ahead_prices(
        BiddingZone.FR,
        start=datetime.date(2025, 1, 1),
        end=datetime.date(2025, 1, 1),
    )

    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index.tz is not None
    assert str(df.index.tz) == "UTC"


@_LIVE
def test_live_connection_decimal_prices() -> None:
    """Live prices must be Decimal, not float."""
    api_key = os.environ["ENTSOE_API_KEY"]
    live_client = ENTSOEClient(api_key=api_key)

    df = live_client.day_ahead_prices(
        BiddingZone.FR,
        start=datetime.date(2025, 1, 1),
        end=datetime.date(2025, 1, 1),
    )

    non_na = df["price_eur_mwh"].dropna()
    assert len(non_na) > 0
    assert all(isinstance(v, Decimal) for v in non_na)


@_LIVE
def test_live_ch_zone() -> None:
    """CH (Switzerland) is only reachable via ENTSO-E; verify live data."""
    api_key = os.environ["ENTSOE_API_KEY"]
    live_client = ENTSOEClient(api_key=api_key)

    df = live_client.day_ahead_prices(
        BiddingZone.CH,
        start=datetime.date(2025, 1, 1),
        end=datetime.date(2025, 1, 1),
    )

    assert isinstance(df, pd.DataFrame)
    assert len(df) > 0
