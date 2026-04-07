# Nord Pool

## Market Data API

- [Market Data Swagger Doc](https://data-api.nordpoolgroup.com/swagger/v2/swagger.json)
- [Market Data Guide](https://developers.nordpoolgroup.com/reference/market-data-using-api)
- [15min MTU](https://developers.nordpoolgroup.com/reference/15-minute-mtu)
- [Auth: General Authentication Guide](https://developers.nordpoolgroup.com/reference/auth-introduction)
- [Auth: Client Auth and Scopes](https://developers.nordpoolgroup.com/reference/clients-and-scopes)
- [Auth: Get Token Endpoint](https://developers.nordpoolgroup.com/reference/get-token)
- [Auth: Handle token expiration](https://developers.nordpoolgroup.com/reference/handling-token-expiration)

### Auth (Market Data API)

`client_id` and `client_secret` are shared (see docs above) and needed for acquiring an
auth token. In addition, the 'get token' endpoint also requires `username` and `password`
fields which are user-provided and required. Read 'Client Auth and Scopes' above for more info.

Set credentials via `NORDPOOL_MARKETDATA_USERNAME` / `NORDPOOL_MARKETDATA_PASSWORD`
environment variables, or pass them as `nordpool_marketdata_username` /
`nordpool_marketdata_password` to `NexaClient`.

| Property | Value |
|---|---|
| Token URL | `https://sts.nordpoolgroup.com/connect/token` |
| Client ID / Secret | `client_marketdata_api` / `client_marketdata_api` |
| Scope | `marketdata_api` |
| Base URL | `https://dataportal-api.nordpoolgroup.com/api` |
| Price endpoint | `GET /v2/Auction/Prices/ByAreas` |
| Query params | `market=DayAhead`, `areas=NO1`, `date=2025-01-01`, `currency=EUR`, `resolution=PT60M` |
| Access model | Separate paid subscription |

---

## Auction API

- [Postman Collection](https://github.com/NordPool/AuctionAPI-PostmanCollection)
- [Products & Submit Orders](https://developers.nordpoolgroup.com/reference/submit-and-modify-orders)

The Auction API is included with Nord Pool DA trading membership and provides an
alternative source of day-ahead prices for users who do not have a Market Data API
subscription. It is used as a fallback in `NexaClient` when Market Data credentials
are not configured.

Set credentials via `NORDPOOL_AUCTION_USERNAME` / `NORDPOOL_AUCTION_PASSWORD`
environment variables, or pass them as `nordpool_auction_username` /
`nordpool_auction_password` to `NexaClient`.

| Property | Value |
|---|---|
| Token URL | `https://sts.nordpoolgroup.com/connect/token` |
| Client ID / Secret | `client_auction_api` / `client_auction_api` |
| Scope | `auction_api` |
| Base URL | `https://auctions-api.nordpoolgroup.com/api/v1` |
| Price endpoint | `GET /auctions/{auctionId}/prices` |
| Access model | Included with DA trading membership |

Test environment base URLs (for manual testing):
- Token: `https://sts.test.nordpoolgroup.com/connect/token`
- API: `https://auctions-api.test.nordpoolgroup.com/api/v1`

### Auction ID construction

The `auctionId` path parameter is constructed directly from the product ID and the
close-for-bid date — no prior API lookup is needed:

```
auctionId = "{productId}-{closeForBidDate}"
```

For day-ahead delivery on date D, bidding closes at 12:00 CET on D-1, so
`closeForBidDate = D - 1` formatted as `YYYYMMDD`.

Example: for delivery 2026-04-05, `closeForBidDate = 20260404`, giving
`NOR_QH_DA_1-20260404`.

### Product IDs (zone mapping)

| BiddingZone | Area code | productId | Notes |
|---|---|---|---|
| NO1–NO5 | NO1–NO5 | `NOR_QH_DA_1` | Nordic-Baltic DA |
| SE1–SE4 | SE1–SE4 | `NOR_QH_DA_1` | |
| DK1, DK2 | DK1, DK2 | `NOR_QH_DA_1` | |
| FI | FI | `NOR_QH_DA_1` | |
| AT | AT | `CWE_QH_DA_1` | CWE DA |
| BE | BE | `CWE_QH_DA_1` | |
| NL | NL | `CWE_QH_DA_1` | |
| FR | FR | `CWE_QH_DA_1` | |
| PL | PL | `PL_QH_DA_1` | Poland DA |
| DE_LU | GER | — | Not supported; CWE uses TSO-level codes, not "GER" |

### Resolution

The `_QH_` products return 96 quarter-hourly contracts per day. The `NordPoolAuctionClient`
always fetches at native 15-minute resolution and aggregates when hourly is requested:

- `Resolution.MINUTES_15` → returns 96 rows per day (native QH)
- `Resolution.HOURLY` → averages the 4 quarter-hourly values per clock-hour into 24 rows

### 7-day data retention

The Auction API only retains data for the **past 7 days**. Requests for older dates will
receive an empty response, which `NordPoolAuctionClient` converts to a
`DataNotAvailableError`. Use the Market Data API (`NordPoolClient`) for historical data.

### Price response schema

`GET /auctions/{auctionId}/prices` returns a JSON array. Each element represents one
auction and contains a `contracts` array:

```json
[
  {
    "auction": "NOR_QH_DA_1-20241231",
    "auctionDeliveryStart": "2024-12-31T23:00:00.000Z",
    "auctionDeliveryEnd": "2025-01-01T23:00:00.000Z",
    "contracts": [
      {
        "contractId": "NOR_QH_DA_1-20250101-01",
        "deliveryStart": "2024-12-31T23:00:00.000Z",
        "deliveryEnd": "2024-12-31T23:15:00.000Z",
        "areas": [
          {
            "areaCode": "NO1",
            "prices": [
              { "currencyCode": "EUR", "marketPrice": 45.23, "status": "Final" },
              { "currencyCode": "NOK", "marketPrice": 519.00, "status": "Final" }
            ]
          }
        ]
      }
    ]
  }
]
```

`marketPrice: null` maps to `pd.NA`, consistent with how the Market Data client handles
the `"Missing"` sentinel. Area codes are the same as the Market Data API (NO1, NO2, GER, etc.).
