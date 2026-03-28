"""Tests for the EXAA market data client."""

from __future__ import annotations

import datetime
from decimal import Decimal

import pytest
from nexa_connect_exaa import (  # type: ignore[import-untyped]  # type: ignore[import-untyped]
    AuctionState,
    AuctionType,
    DeliveryTimePeriod,
    MarketResult,
    ProductInfo,
)
from nexa_connect_exaa.models.auction import Auction  # type: ignore[import-untyped]
from nexa_connect_exaa.testing import FakeEXAAClient  # type: ignore[import-untyped]

from nexa_marketdata.exaa import EXAAClient
from nexa_marketdata.exceptions import AuthenticationError, DataNotAvailableError
from nexa_marketdata.types import BiddingZone, Resolution

# AT operates on CET (UTC+1 in winter). Day 2026-01-01 starts 2025-12-31 23:00 UTC.
_DELIVERY_DATE = datetime.date(2026, 1, 1)
_AUCTION_ID = "Classic_2026-01-01"
_AT_DAY_START_UTC = datetime.datetime(2025, 12, 31, 23, 0, 0, tzinfo=datetime.UTC)


def _make_hourly_product(product_id: str, hour_offset: int) -> ProductInfo:
    start = _AT_DAY_START_UTC + datetime.timedelta(hours=hour_offset)
    return ProductInfo(
        product_id=product_id,
        delivery_time_periods=[
            DeliveryTimePeriod(start=start, end=start + datetime.timedelta(hours=1))
        ],
    )


def _make_qh_product(product_id: str, slot_offset: int) -> ProductInfo:
    start = _AT_DAY_START_UTC + datetime.timedelta(minutes=15 * slot_offset)
    return ProductInfo(
        product_id=product_id,
        delivery_time_periods=[
            DeliveryTimePeriod(start=start, end=start + datetime.timedelta(minutes=15))
        ],
    )


def _make_market_result(
    product_id: str, price: str, price_zone: str = "AT"
) -> MarketResult:
    return MarketResult(
        product_id=product_id,
        price_zone=price_zone,
        price=Decimal(price),
        volume=Decimal("100.0"),
    )


def _make_auction(
    state: AuctionState = AuctionState.FINALIZED,
    hourly_products: list[ProductInfo] | None = None,
    quarter_hourly_products: list[ProductInfo] | None = None,
) -> Auction:
    if hourly_products is None:
        hourly_products = [
            _make_hourly_product(f"hEXA{i + 1:02d}", i) for i in range(24)
        ]
    return Auction(
        id=_AUCTION_ID,
        auction_type=AuctionType.CLASSIC,
        state=state,
        delivery_day=_DELIVERY_DATE,
        trading_day=datetime.date(2025, 12, 31),
        hourly_products=hourly_products,
        quarter_hourly_products=quarter_hourly_products or [],
    )


def _make_exaa_client() -> EXAAClient:
    return EXAAClient(
        username="user",
        password="pass",
        private_key_path="/fake/key.pem",
        certificate_path="/fake/cert.pem",
    )


def test_day_ahead_prices_returns_dataframe(monkeypatch: pytest.MonkeyPatch) -> None:
    auction = _make_auction()
    results = [
        _make_market_result(f"hEXA{i + 1:02d}", f"{50 + i}.00") for i in range(24)
    ]

    fake = FakeEXAAClient(
        auctions=[auction],
        market_results={_AUCTION_ID: results},
    )
    client = _make_exaa_client()
    monkeypatch.setattr(client, "_make_client", lambda: fake)

    df = client.day_ahead_prices(
        zone=BiddingZone.AT,
        start=_DELIVERY_DATE,
        end=_DELIVERY_DATE,
    )

    assert len(df) == 24
    assert "price_eur_mwh" in df.columns


def test_day_ahead_prices_index_is_utc(monkeypatch: pytest.MonkeyPatch) -> None:
    auction = _make_auction()
    results = [_make_market_result("hEXA01", "55.00")]
    fake = FakeEXAAClient(auctions=[auction], market_results={_AUCTION_ID: results})
    client = _make_exaa_client()
    monkeypatch.setattr(client, "_make_client", lambda: fake)

    df = client.day_ahead_prices(BiddingZone.AT, _DELIVERY_DATE, _DELIVERY_DATE)

    assert df.index.tz is not None
    assert str(df.index.tz) == "UTC"
    # First hourly slot starts at 23:00 UTC on Dec 31 (= 00:00 CET Jan 1)
    assert df.index[0] == _AT_DAY_START_UTC


def test_day_ahead_prices_prices_are_decimal(monkeypatch: pytest.MonkeyPatch) -> None:
    auction = _make_auction()
    results = [_make_market_result("hEXA01", "72.53")]
    fake = FakeEXAAClient(auctions=[auction], market_results={_AUCTION_ID: results})
    client = _make_exaa_client()
    monkeypatch.setattr(client, "_make_client", lambda: fake)

    df = client.day_ahead_prices(BiddingZone.AT, _DELIVERY_DATE, _DELIVERY_DATE)

    assert isinstance(df["price_eur_mwh"].iloc[0], Decimal)
    assert df["price_eur_mwh"].iloc[0] == Decimal("72.53")


def test_day_ahead_prices_multiday_range(monkeypatch: pytest.MonkeyPatch) -> None:
    date1 = datetime.date(2026, 1, 1)
    date2 = datetime.date(2026, 1, 2)
    day1_start = datetime.datetime(2025, 12, 31, 23, 0, tzinfo=datetime.UTC)
    day2_start = datetime.datetime(2026, 1, 1, 23, 0, tzinfo=datetime.UTC)

    def _products(day_start: datetime.datetime) -> list[ProductInfo]:
        return [
            ProductInfo(
                product_id=f"hEXA{i + 1:02d}",
                delivery_time_periods=[
                    DeliveryTimePeriod(
                        start=day_start + datetime.timedelta(hours=i),
                        end=day_start + datetime.timedelta(hours=i + 1),
                    )
                ],
            )
            for i in range(24)
        ]

    auction1 = Auction(
        id="Classic_2026-01-01",
        auction_type=AuctionType.CLASSIC,
        state=AuctionState.FINALIZED,
        delivery_day=date1,
        trading_day=datetime.date(2025, 12, 31),
        hourly_products=_products(day1_start),
    )
    auction2 = Auction(
        id="Classic_2026-01-02",
        auction_type=AuctionType.CLASSIC,
        state=AuctionState.FINALIZED,
        delivery_day=date2,
        trading_day=date1,
        hourly_products=_products(day2_start),
    )

    results1 = [_make_market_result(f"hEXA{i + 1:02d}", "50.00") for i in range(24)]
    results2 = [_make_market_result(f"hEXA{i + 1:02d}", "60.00") for i in range(24)]

    fake = FakeEXAAClient(
        auctions=[auction1, auction2],
        market_results={
            "Classic_2026-01-01": results1,
            "Classic_2026-01-02": results2,
        },
    )
    client = _make_exaa_client()
    monkeypatch.setattr(client, "_make_client", lambda: fake)

    df = client.day_ahead_prices(BiddingZone.AT, date1, date2)

    assert len(df) == 48
    assert df.index.is_monotonic_increasing


def test_day_ahead_prices_15min_resolution(monkeypatch: pytest.MonkeyPatch) -> None:
    qh_products = [_make_qh_product(f"QH{i + 1:03d}", i) for i in range(96)]
    auction = _make_auction(quarter_hourly_products=qh_products)
    results = [_make_market_result(f"QH{i + 1:03d}", "45.00") for i in range(96)]
    fake = FakeEXAAClient(auctions=[auction], market_results={_AUCTION_ID: results})
    client = _make_exaa_client()
    monkeypatch.setattr(client, "_make_client", lambda: fake)

    df = client.day_ahead_prices(
        BiddingZone.AT,
        _DELIVERY_DATE,
        _DELIVERY_DATE,
        resolution=Resolution.MINUTES_15,
    )

    assert len(df) == 96


def test_day_ahead_prices_15min_excludes_hourly_products(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """15-minute resolution must not include hourly product rows."""
    qh_products = [_make_qh_product(f"QH{i + 1:03d}", i) for i in range(96)]
    auction = _make_auction(quarter_hourly_products=qh_products)
    # Results include both hourly and QH products
    results = [_make_market_result(f"hEXA{i + 1:02d}", "50.00") for i in range(24)] + [
        _make_market_result(f"QH{i + 1:03d}", "45.00") for i in range(96)
    ]
    fake = FakeEXAAClient(auctions=[auction], market_results={_AUCTION_ID: results})
    client = _make_exaa_client()
    monkeypatch.setattr(client, "_make_client", lambda: fake)

    df = client.day_ahead_prices(
        BiddingZone.AT,
        _DELIVERY_DATE,
        _DELIVERY_DATE,
        resolution=Resolution.MINUTES_15,
    )

    assert len(df) == 96


def test_day_ahead_prices_not_finalized_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    auction = _make_auction(state=AuctionState.AUCTIONING)
    fake = FakeEXAAClient(auctions=[auction], market_results={_AUCTION_ID: []})
    client = _make_exaa_client()
    monkeypatch.setattr(client, "_make_client", lambda: fake)

    with pytest.raises(DataNotAvailableError, match="not yet finalised"):
        client.day_ahead_prices(BiddingZone.AT, _DELIVERY_DATE, _DELIVERY_DATE)


def test_day_ahead_prices_no_auction_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    fake = FakeEXAAClient(auctions=[])
    client = _make_exaa_client()
    monkeypatch.setattr(client, "_make_client", lambda: fake)

    with pytest.raises(DataNotAvailableError, match="No.*auction found"):
        client.day_ahead_prices(BiddingZone.AT, _DELIVERY_DATE, _DELIVERY_DATE)


def test_day_ahead_prices_unsupported_zone_raises() -> None:
    """Unsupported zone raises without making any API calls."""
    client = _make_exaa_client()

    with pytest.raises(DataNotAvailableError, match="not supported on EXAA"):
        client.day_ahead_prices(BiddingZone.NO1, _DELIVERY_DATE, _DELIVERY_DATE)


def test_day_ahead_prices_auth_error_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    from nexa_connect_exaa import EXAAAuthError  # type: ignore[import-untyped]

    class FailingFake(FakeEXAAClient):
        def get_auctions(self, **_: object) -> list[object]:  # type: ignore[override]
            raise EXAAAuthError(code="A001", message="Invalid credentials")

    client = _make_exaa_client()
    monkeypatch.setattr(client, "_make_client", lambda: FailingFake())

    with pytest.raises(AuthenticationError, match="EXAA credentials rejected"):
        client.day_ahead_prices(BiddingZone.AT, _DELIVERY_DATE, _DELIVERY_DATE)


def test_day_ahead_prices_ignores_other_price_zones(monkeypatch) -> None:
    """Results for DE/NL price zones are ignored when fetching AT prices."""
    auction = _make_auction()
    results = [
        _make_market_result("hEXA01", "50.00", price_zone="AT"),
        _make_market_result("hEXA01", "48.00", price_zone="DE"),
        _make_market_result("hEXA01", "49.00", price_zone="NL"),
    ]
    fake = FakeEXAAClient(auctions=[auction], market_results={_AUCTION_ID: results})
    client = _make_exaa_client()
    monkeypatch.setattr(client, "_make_client", lambda: fake)

    df = client.day_ahead_prices(BiddingZone.AT, _DELIVERY_DATE, _DELIVERY_DATE)

    assert len(df) == 1
    assert df["price_eur_mwh"].iloc[0] == Decimal("50.00")
