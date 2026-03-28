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


def test_client_instantiation_with_exaa_creds() -> None:
    client = NexaClient(
        exaa_username="user",
        exaa_password="pass",
        exaa_private_key_path="/key.pem",
        exaa_certificate_path="/cert.pem",
    )
    assert client._exaa is not None


def test_client_instantiation_without_exaa_creds() -> None:
    client = NexaClient()
    assert client._exaa is None


def test_client_reads_exaa_creds_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("EXAA_USERNAME", "env-user")
    monkeypatch.setenv("EXAA_PASSWORD", "env-pass")
    monkeypatch.setenv("EXAA_PRIVATE_KEY_PATH", "/env/key.pem")
    monkeypatch.setenv("EXAA_CERTIFICATE_PATH", "/env/cert.pem")
    client = NexaClient()
    assert client._exaa is not None


def test_client_partial_exaa_creds_no_client(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("EXAA_USERNAME", raising=False)
    monkeypatch.delenv("EXAA_PASSWORD", raising=False)
    monkeypatch.delenv("EXAA_PRIVATE_KEY_PATH", raising=False)
    monkeypatch.delenv("EXAA_CERTIFICATE_PATH", raising=False)
    client = NexaClient(exaa_username="user-only")
    assert client._exaa is None


def test_day_ahead_prices_raises_without_nordpool_creds() -> None:
    client = NexaClient()
    with pytest.raises(DataNotAvailableError, match="No Nord Pool credentials"):
        client.day_ahead_prices(
            zone=BiddingZone.NO2,
            start=datetime.date(2025, 1, 1),
            end=datetime.date(2025, 1, 7),
        )


def test_day_ahead_prices_raises_without_entsoe_key(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """GB zone raises when no ENTSO-E API key is configured."""
    monkeypatch.delenv("ENTSOE_API_KEY", raising=False)
    client = NexaClient()
    with pytest.raises(DataNotAvailableError, match="No ENTSO-E API key"):
        client.day_ahead_prices(
            zone=BiddingZone.GB,
            start=datetime.date(2025, 1, 1),
            end=datetime.date(2025, 1, 1),
        )


def test_day_ahead_prices_raises_without_exaa_creds(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """AT zone raises when no EXAA credentials are configured."""
    monkeypatch.delenv("EXAA_USERNAME", raising=False)
    monkeypatch.delenv("EXAA_PASSWORD", raising=False)
    monkeypatch.delenv("EXAA_PRIVATE_KEY_PATH", raising=False)
    monkeypatch.delenv("EXAA_CERTIFICATE_PATH", raising=False)
    client = NexaClient()
    with pytest.raises(DataNotAvailableError, match="No EXAA credentials"):
        client.day_ahead_prices(
            zone=BiddingZone.AT,
            start=datetime.date(2025, 1, 1),
            end=datetime.date(2025, 1, 1),
        )


def test_day_ahead_prices_raises_for_zone_in_neither_set(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Zone not in any routing set raises DataNotAvailableError."""
    import nexa_marketdata.client as client_module

    monkeypatch.setattr(client_module, "_NORDPOOL_ZONES", frozenset())
    monkeypatch.setattr(client_module, "_ENTSOE_ZONES", frozenset())
    monkeypatch.setattr(client_module, "_EXAA_ZONES", frozenset())
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


def test_day_ahead_prices_delegates_to_exaa_client() -> None:
    """AT zone is routed to the EXAA client."""
    mock_df = pd.DataFrame(
        {"price_eur_mwh": [Decimal("55.00")] * 24},
        index=pd.date_range("2025-12-31 23:00", periods=24, freq="h", tz="UTC"),
    )
    client = NexaClient(
        exaa_username="user",
        exaa_password="pass",
        exaa_private_key_path="/key.pem",
        exaa_certificate_path="/cert.pem",
    )
    assert client._exaa is not None
    client._exaa.day_ahead_prices = MagicMock(return_value=mock_df)  # type: ignore[method-assign]

    result = client.day_ahead_prices(
        zone=BiddingZone.AT,
        start=datetime.date(2026, 1, 1),
        end=datetime.date(2026, 1, 1),
    )

    client._exaa.day_ahead_prices.assert_called_once_with(
        BiddingZone.AT,
        datetime.date(2026, 1, 1),
        datetime.date(2026, 1, 1),
        resolution=Resolution.HOURLY,
    )
    assert result is mock_df


def test_at_zone_is_not_in_nordpool_zones() -> None:
    """AT must be routed to EXAA, not Nord Pool."""
    import nexa_marketdata.client as client_module

    assert BiddingZone.AT not in client_module._NORDPOOL_ZONES
    assert BiddingZone.AT in client_module._EXAA_ZONES
