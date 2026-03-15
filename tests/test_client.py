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


def test_day_ahead_prices_raises_without_nordpool_creds() -> None:
    client = NexaClient()
    with pytest.raises(DataNotAvailableError, match="No Nord Pool credentials"):
        client.day_ahead_prices(
            zone=BiddingZone.NO2,
            start=datetime.date(2025, 1, 1),
            end=datetime.date(2025, 1, 7),
        )


def test_day_ahead_prices_raises_for_unsupported_zone() -> None:
    """GB is not served by any configured source."""
    client = NexaClient()
    with pytest.raises(DataNotAvailableError):
        client.day_ahead_prices(
            zone=BiddingZone.GB,
            start=datetime.date(2025, 1, 1),
            end=datetime.date(2025, 1, 1),
        )


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
