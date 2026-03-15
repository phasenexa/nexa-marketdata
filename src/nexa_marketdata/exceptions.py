"""Custom exception hierarchy for nexa-marketdata."""


class NexaError(Exception):
    """Base exception for all nexa-marketdata errors."""


class AuthenticationError(NexaError):
    """Raised when an API key is missing or rejected."""


class RateLimitError(NexaError):
    """Raised when an exchange API rate limit is exceeded."""


class DataNotAvailableError(NexaError):
    """Raised when requested data does not exist for the given parameters."""


class ExchangeAPIError(NexaError):
    """Raised when an exchange API returns an unexpected error response."""
