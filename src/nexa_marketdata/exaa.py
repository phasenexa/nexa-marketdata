"""EXAA (Energy Exchange Austria) market data client.

Uses the Classic day-ahead auction (10:15 CET) for AT prices, which is
the authoritative Austrian day-ahead price and supports 15-minute MTU
products in addition to hourly.

Rate limits: Governed by nexa-connect-exaa connection management.
API base URL: https://trade.exaa.at
"""

from __future__ import annotations

import datetime
from decimal import Decimal
from typing import Any

import pandas as pd
from nexa_connect_exaa import CertificateAuth
from nexa_connect_exaa import EXAAClient as _ExternalEXAAClient
from nexa_connect_exaa.models.auction import AuctionState, AuctionType

from nexa_marketdata.exceptions import (
    AuthenticationError,
    DataNotAvailableError,
    ExchangeAPIError,
)
from nexa_marketdata.types import BiddingZone, Resolution

_ZONE_TO_PRICE_ZONE: dict[BiddingZone, str] = {
    BiddingZone.AT: "AT",
}

_ZONE_TO_AUCTION_TYPE: dict[BiddingZone, Any] = {
    BiddingZone.AT: AuctionType.CLASSIC,
}


class EXAAClient:
    """EXAA day-ahead market data client.

    Fetches clearing prices from EXAA's Classic auction (AT) using
    certificate-based authentication. The Classic auction runs at 10:15 CET
    and is the authoritative Austrian day-ahead price source.

    Args:
        username: EXAA trading account username.
        password: EXAA trading account password.
        private_key_path: Path to RSA private key PEM file.
        certificate_path: Path to X.509 certificate PEM file.
    """

    def __init__(
        self,
        username: str,
        password: str,
        private_key_path: str,
        certificate_path: str,
    ) -> None:
        self._auth = CertificateAuth(
            username=username,
            password=password,
            private_key_path=private_key_path,
            certificate_path=certificate_path,
        )

    def _make_client(self) -> Any:
        """Return a new nexa-connect-exaa EXAAClient context manager."""
        return _ExternalEXAAClient(auth=self._auth)

    def day_ahead_prices(
        self,
        zone: BiddingZone,
        start: datetime.date,
        end: datetime.date,
        resolution: Resolution = Resolution.HOURLY,
    ) -> pd.DataFrame:
        """Retrieve day-ahead auction clearing prices for a bidding zone.

        Args:
            zone: The bidding zone. Only ``BiddingZone.AT`` is supported.
            start: Start date (inclusive).
            end: End date (inclusive).
            resolution: Time resolution. ``MINUTES_15`` is only available for
                AT via the Classic auction.

        Returns:
            DataFrame with UTC-aware DatetimeIndex and column
            ``price_eur_mwh`` (Decimal, or pd.NA for missing periods).

        Raises:
            DataNotAvailableError: If the zone is unsupported, no auction
                exists for the date, or results are not yet finalised.
            AuthenticationError: If EXAA credentials are rejected.
            ExchangeAPIError: For unexpected API errors.
        """
        if zone not in _ZONE_TO_PRICE_ZONE:
            raise DataNotAvailableError(
                f"Bidding zone {zone!r} is not supported on EXAA. "
                f"Supported zones: {sorted(_ZONE_TO_PRICE_ZONE)}."
            )

        price_zone = _ZONE_TO_PRICE_ZONE[zone]
        auction_type = _ZONE_TO_AUCTION_TYPE[zone]
        frames: list[pd.DataFrame] = []
        current = start

        with self._make_client() as client:
            while current <= end:
                df = self._fetch_day(
                    client, current, price_zone, auction_type, resolution
                )
                frames.append(df)
                current += datetime.timedelta(days=1)

        if not frames:
            return pd.DataFrame({"price_eur_mwh": pd.Series([], dtype=object)})
        return pd.concat(frames).sort_index()

    def _fetch_day(
        self,
        client: Any,
        delivery_date: datetime.date,
        price_zone: str,
        auction_type: Any,
        resolution: Resolution,
    ) -> pd.DataFrame:
        """Fetch clearing prices for a single delivery date."""
        try:
            auctions = client.get_auctions(delivery_day=delivery_date)
        except Exception as exc:
            _raise_for_exaa_error(exc, delivery_date)
            raise  # unreachable — satisfies mypy

        target = next(
            (a for a in auctions if a.auction_type == auction_type),
            None,
        )
        if target is None:
            raise DataNotAvailableError(
                f"No {auction_type} auction found for {delivery_date} on EXAA."
            )

        try:
            # get_auction returns the full detail including product delivery periods
            auction = client.get_auction(target.id)
            results = client.get_market_results(target.id)
        except Exception as exc:
            _raise_for_exaa_error(exc, delivery_date)
            raise  # unreachable — satisfies mypy

        if auction.state != AuctionState.FINALIZED:
            raise DataNotAvailableError(
                f"EXAA {auction_type} auction for {delivery_date} is not yet "
                f"finalised (state: {auction.state})."
            )

        # Select the correct product list for the requested resolution
        if resolution == Resolution.MINUTES_15:
            products = auction.quarter_hourly_products
        else:
            products = auction.hourly_products

        # Build productId → UTC start datetime lookup from product delivery periods
        period_map: dict[str, datetime.datetime] = {
            p.product_id: p.delivery_time_periods[0].start.astimezone(datetime.UTC)
            for p in products
            if p.delivery_time_periods
        }
        valid_ids = {p.product_id for p in products}

        timestamps: list[datetime.datetime] = []
        prices: list[Any] = []

        for result in results:
            if result.price_zone != price_zone:
                continue
            if result.product_id not in valid_ids:
                continue
            ts = period_map.get(result.product_id)
            if ts is None:
                continue
            timestamps.append(ts)
            price = Decimal(str(result.price)) if result.price is not None else pd.NA
            prices.append(price)

        index = pd.DatetimeIndex(timestamps, tz=datetime.UTC)
        return pd.DataFrame({"price_eur_mwh": prices}, index=index)


def _raise_for_exaa_error(exc: Exception, date: datetime.date) -> None:
    """Map nexa-connect-exaa exceptions to nexa-marketdata exceptions."""
    exc_type = type(exc).__name__
    if "Auth" in exc_type:
        raise AuthenticationError(f"EXAA credentials rejected: {exc}") from exc
    if "NotFound" in exc_type or "Functional" in exc_type:
        raise DataNotAvailableError(
            f"EXAA data not available for {date}: {exc}"
        ) from exc
    if "Server" in exc_type or "Connection" in exc_type:
        raise ExchangeAPIError(f"EXAA API error for {date}: {exc}") from exc
