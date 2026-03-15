"""Tests for the unified NexaClient."""

import datetime

import pytest

from nexa_marketdata import NexaClient
from nexa_marketdata.types import BiddingZone


def test_client_instantiation() -> None:
    client = NexaClient()
    assert client is not None


def test_day_ahead_prices_not_implemented() -> None:
    client = NexaClient()
    with pytest.raises(NotImplementedError):
        client.day_ahead_prices(
            zone=BiddingZone.NO2,
            start=datetime.date(2025, 1, 1),
            end=datetime.date(2025, 1, 7),
        )
