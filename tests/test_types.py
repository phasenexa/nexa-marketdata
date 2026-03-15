"""Tests for core types and enumerations."""

from nexa_marketdata.types import BiddingZone, DataSource, Resolution


def test_bidding_zone_values() -> None:
    assert BiddingZone.NO1 == "NO1"
    assert BiddingZone.DE_LU == "DE-LU"


def test_resolution_values() -> None:
    assert Resolution.HOURLY == "PT60M"
    assert Resolution.MINUTES_15 == "PT15M"


def test_data_source_values() -> None:
    assert DataSource.NORDPOOL == "nordpool"
    assert DataSource.ENTSOE == "entsoe"
