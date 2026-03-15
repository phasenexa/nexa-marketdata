# Nord Pool

- [Market Data Swagger Doc](https://data-api.nordpoolgroup.com/swagger/v2/swagger.json)
- [Market Data Guide](https://developers.nordpoolgroup.com/reference/market-data-using-api)
- [15min MTU](https://developers.nordpoolgroup.com/reference/15-minute-mtu)
- [Auth: General Authentication Guide](https://developers.nordpoolgroup.com/reference/auth-introduction)
- [Auth: Client Auth and Scopes](https://developers.nordpoolgroup.com/reference/clients-and-scopes)
- [Auth: Get Token Endpoint](https://developers.nordpoolgroup.com/reference/get-token)
- [Auth: Handle token expiration](https://developers.nordpoolgroup.com/reference/handling-token-expiration)

# Auth

`client_id` and `client_secret` are shared (see docs above) and needed for acquiring an auth token. In addition, the 'get token' endpoint also requires `username` and `password` fields which are user-provided and required. Read 'Client Auth and Scopes' above for more info.