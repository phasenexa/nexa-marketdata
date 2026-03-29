"""Tests for the unified NexaClient."""

from __future__ import annotations

import datetime
from decimal import Decimal
from unittest.mock import MagicMock

import pandas as pd
import pytest

from nexa_marketdata import NexaClient
from nexa_marketdata.exceptions import DataNotAvailableError
from nexa_marketdata.types import BiddingZone, Resolution


def test_client_instantiation() -> None:
    client = NexaClient()
    assert client is not None


def test_client_instantiation_with_nordpool_creds() -> None:
    client = NexaClient(nordpool_username="user", nordpool_password="pass")
    assert client._nordpool is not None


def test_client_instantiation_without_nordpool_creds() -> None:
    client = NexaClient()
    assert client._nordpool is None


def test_client_reads_nordpool_creds_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("NORDPOOL_USERNAME", "env-user")
    monkeypatch.setenv("NORDPOOL_PASSWORD", "env-pass")
    client = NexaClient()
    assert client._nordpool is not None


def test_client_partial_nordpool_creds_no_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv("NORDPOOL_USERNAME", raising=False)
    monkeypatch.delenv("NORDPOOL_PASSWORD", raising=False)
    client = NexaClient(nordpool_username="user-only")
    assert client._nordpool is None


def test_day_ahead_prices_raises_without_any_creds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Zone supported by multiple sources raises when none are configured."""
    monkeypatch.delenv("NORDPOOL_USERNAME", raising=False)
    monkeypatch.delenv("NORDPOOL_PASSWORD", raising=False)
    monkeypatch.delenv("ENTSOE_API_KEY", raising=False)
    client = NexaClient()
    with pytest.raises(DataNotAvailableError, match="available via Nord Pool"):
        client.day_ahead_prices(
            zone=BiddingZone.NO2,
            start=datetime.date(2025, 1, 1),
            end=datetime.date(2025, 1, 7),
        )


def test_day_ahead_prices_raises_without_entsoe_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GB zone raises when no ENTSO-E API key is configured (only ENTSO-E source)."""
    monkeypatch.delenv("NORDPOOL_USERNAME", raising=False)
    monkeypatch.delenv("NORDPOOL_PASSWORD", raising=False)
    monkeypatch.delenv("ENTSOE_API_KEY", raising=False)
    client = NexaClient()
    with pytest.raises(DataNotAvailableError, match="available via ENTSO-E"):
        client.day_ahead_prices(
            zone=BiddingZone.GB,
            start=datetime.date(2025, 1, 1),
            end=datetime.date(2025, 1, 1),
        )


def test_day_ahead_prices_raises_for_zone_in_neither_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Zone absent from all routing sets raises DataNotAvailableError."""
    import nexa_marketdata.client as client_module

    monkeypatch.setattr(client_module, "_SOURCES", [])
    client = NexaClient()
    with pytest.raises(DataNotAvailableError, match="No data source available"):
        client.day_ahead_prices(
            zone=BiddingZone.NO1,
            start=datetime.date(2025, 1, 1),
            end=datetime.date(2025, 1, 1),
        )


def test_day_ahead_prices_delegates_to_entsoe_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_df = pd.DataFrame(
        {"price_eur_mwh": [Decimal("60.00")] * 24},
        index=pd.date_range("2025-01-01 00:00", periods=24, freq="h", tz="UTC"),
    )
    monkeypatch.setenv("ENTSOE_API_KEY", "test-key")
    client = NexaClient()
    assert client._entsoe is not None
    client._entsoe.day_ahead_prices = MagicMock(return_value=mock_df)  # type: ignore[method-assign]

    result = client.day_ahead_prices(
        zone=BiddingZone.GB,
        start=datetime.date(2025, 1, 1),
        end=datetime.date(2025, 1, 1),
    )

    client._entsoe.day_ahead_prices.assert_called_once_with(
        BiddingZone.GB,
        datetime.date(2025, 1, 1),
        datetime.date(2025, 1, 1),
        resolution=Resolution.HOURLY,
    )
    assert result is mock_df


def test_day_ahead_prices_delegates_to_nordpool_client() -> None:
    mock_df = pd.DataFrame(
        {"price_eur_mwh": [Decimal("45.00")] * 24},
        index=pd.date_range("2024-12-31 23:00", periods=24, freq="h", tz="UTC"),
    )
    client = NexaClient(nordpool_username="user", nordpool_password="pass")
    assert client._nordpool is not None
    client._nordpool.day_ahead_prices = MagicMock(return_value=mock_df)  # type: ignore[method-assign]

    result = client.day_ahead_prices(
        zone=BiddingZone.NO1,
        start=datetime.date(2025, 1, 1),
        end=datetime.date(2025, 1, 1),
    )

    client._nordpool.day_ahead_prices.assert_called_once_with(
        BiddingZone.NO1,
        datetime.date(2025, 1, 1),
        datetime.date(2025, 1, 1),
        resolution=Resolution.HOURLY,
    )
    assert result is mock_df


def test_day_ahead_prices_falls_through_to_entsoe_when_nordpool_unconfigured(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Nord Pool zone falls through to ENTSO-E when only ENTSO-E is configured."""
    mock_df = pd.DataFrame(
        {"price_eur_mwh": [Decimal("55.00")] * 24},
        index=pd.date_range("2025-01-01 00:00", periods=24, freq="h", tz="UTC"),
    )
    monkeypatch.delenv("NORDPOOL_USERNAME", raising=False)
    monkeypatch.delenv("NORDPOOL_PASSWORD", raising=False)
    monkeypatch.setenv("ENTSOE_API_KEY", "test-key")
    client = NexaClient()
    assert client._nordpool is None
    assert client._entsoe is not None
    client._entsoe.day_ahead_prices = MagicMock(return_value=mock_df)  # type: ignore[method-assign]

    result = client.day_ahead_prices(
        zone=BiddingZone.NO1,
        start=datetime.date(2025, 1, 1),
        end=datetime.date(2025, 1, 1),
    )

    client._entsoe.day_ahead_prices.assert_called_once_with(
        BiddingZone.NO1,
        datetime.date(2025, 1, 1),
        datetime.date(2025, 1, 1),
        resolution=Resolution.HOURLY,
    )
    assert result is mock_df


def test_day_ahead_prices_entsoe_only_zone_routes_to_entsoe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Zone only available via ENTSO-E (e.g. IT-NORD) is routed correctly."""
    mock_df = pd.DataFrame(
        {"price_eur_mwh": [Decimal("70.00")] * 24},
        index=pd.date_range("2025-01-01 00:00", periods=24, freq="h", tz="UTC"),
    )
    monkeypatch.setenv("ENTSOE_API_KEY", "test-key")
    client = NexaClient()
    assert client._entsoe is not None
    client._entsoe.day_ahead_prices = MagicMock(return_value=mock_df)  # type: ignore[method-assign]

    result = client.day_ahead_prices(
        zone=BiddingZone.IT_NORD,
        start=datetime.date(2025, 1, 1),
        end=datetime.date(2025, 1, 1),
    )

    client._entsoe.day_ahead_prices.assert_called_once_with(
        BiddingZone.IT_NORD,
        datetime.date(2025, 1, 1),
        datetime.date(2025, 1, 1),
        resolution=Resolution.HOURLY,
    )
    assert result is mock_df
