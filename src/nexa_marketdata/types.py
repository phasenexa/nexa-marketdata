"""Core types and enumerations for nexa-marketdata."""

from enum import StrEnum


class DataSource(StrEnum):
    """Supported market data sources."""

    NORDPOOL = "nordpool"
    ENTSOE = "entsoe"
    EPEX_SPOT = "epex_spot"
    EEX = "eex"


class Resolution(StrEnum):
    """Market time unit resolution."""

    MINUTES_15 = "PT15M"
    HOURLY = "PT60M"


class BiddingZone(StrEnum):
    """European power market bidding zones."""

    # Nordic
    NO1 = "NO1"
    NO2 = "NO2"
    NO3 = "NO3"
    NO4 = "NO4"
    NO5 = "NO5"
    SE1 = "SE1"
    SE2 = "SE2"
    SE3 = "SE3"
    SE4 = "SE4"
    DK1 = "DK1"
    DK2 = "DK2"
    FI = "FI"

    # Central Western Europe
    DE_LU = "DE-LU"
    FR = "FR"
    BE = "BE"
    NL = "NL"
    AT = "AT"
    CH = "CH"

    # Other
    GB = "GB"
    PL = "PL"
