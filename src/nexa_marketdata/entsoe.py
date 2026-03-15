"""ENTSO-E Transparency Platform client.

Rate limits: ~400 requests/minute per API key (unofficial; subject to change).
Known issues: 403 errors, format inconsistencies between API v1 and v2,
occasional breaking changes. Clients must handle these gracefully.
API base URL: https://web-api.tp.entsoe.eu/api
"""
