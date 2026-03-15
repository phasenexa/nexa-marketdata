"""Per-source rate limiting.

Each exchange has different rate limits. This module provides shared
infrastructure so individual exchange clients don't need to implement
their own rate limiting logic.
"""
