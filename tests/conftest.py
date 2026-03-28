"""Shared pytest fixtures."""

from __future__ import annotations

from pathlib import Path

from dotenv import load_dotenv

# Load .env from the project root so that integration tests can read
# ENTSOE_API_KEY (and other credentials) without manual env exports.
load_dotenv(Path(__file__).parent.parent / ".env")
