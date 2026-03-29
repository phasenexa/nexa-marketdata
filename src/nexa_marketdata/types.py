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

    # Baltic
    EE = "EE"
    LV = "LV"
    LT = "LT"

    # Central Western Europe
    DE_LU = "DE-LU"
    FR = "FR"
    BE = "BE"
    NL = "NL"
    AT = "AT"
    CH = "CH"

    # Iberian Peninsula
    ES = "ES"
    PT = "PT"

    # Central & Eastern Europe
    CZ = "CZ"
    SK = "SK"
    HU = "HU"
    RO = "RO"
    BG = "BG"
    SI = "SI"
    HR = "HR"
    PL = "PL"

    # Western Balkans
    RS = "RS"
    BA = "BA"
    ME = "ME"
    MK = "MK"
    AL = "AL"
    XK = "XK"
    MD = "MD"

    # Italy (separate bidding zones)
    IT_NORD = "IT-NORD"
    IT_CNOR = "IT-CNOR"
    IT_CSUD = "IT-CSUD"
    IT_SUD = "IT-SUD"
    IT_SARD = "IT-SARD"
    IT_SICI = "IT-SICI"
    IT_CALA = "IT-CALA"

    # British Isles
    GB = "GB"
    IE_SEM = "IE-SEM"

    # Islands / small markets
    CY = "CY"
    MT = "MT"
    IS = "IS"

    # Other ENTSO-E members
    GE = "GE"
    BY = "BY"
    UA = "UA"
    TR = "TR"
