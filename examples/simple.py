import datetime

from nexa_marketdata import NexaClient
from nexa_marketdata.types import BiddingZone, Resolution

def main():
    client = NexaClient()
    prices = client.day_ahead_prices(
        zone=BiddingZone.NO2,
        start=datetime.date(2026, 3, 16),
        end=datetime.date(2026, 3, 16),
    )

    print(f"Rows: {len(prices)}")
    print(f"Index timezone: {prices.index.tz}")
    print(f"Dtype: {prices['price_eur_mwh'].dtype}")
    prices.head()

if __name__ == '__main__':
    main()