import os
import time
from datetime import datetime, timedelta
from typing import Any

import requests
from dotenv import load_dotenv

from src.utils.logger import get_logger
from src.config import OPENSKY_CLIENT_ID, OPENSKY_CLIENT_SECRET

logger = get_logger(__name__)

load_dotenv()

BASE_URL = "https://opensky-network.org/api"
TOKEN_URL = "https://auth.opensky-network.org/auth/realms/opensky-network/protocol/openid-connect/token"
TOKEN_REFRESH_MARGIN = 30


class OpenSkyTokenManager:
    """
    Manage OAuth2 access tokens for the OpenSky API.
    """

    def __init__(self, client_id: str | None, client_secret: str | None) -> None:
        self.client_id = client_id
        self.client_secret = client_secret
        self.token: str | None = None
        self.expires_at: datetime | None = None

    def get_token(self) -> str:
        """
        Return a valid access token, refreshing it if needed.
        """
        if self.token and self.expires_at and datetime.now() < self.expires_at:
            return self.token
        return self._refresh()

    def _refresh(self) -> str:
        """
        Request a new access token from the OpenSky authentication server.
        """
        if not self.client_id or not self.client_secret:
            raise ValueError(
                "Missing OpenSky credentials. Set OPENSKY_CLIENT_ID and OPENSKY_CLIENT_SECRET in your .env file."
            )

        response = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "client_credentials",
                "client_id": self.client_id,
                "client_secret": self.client_secret,
            },
            timeout=30,
        )
        response.raise_for_status()

        data = response.json()
        self.token = data["access_token"]

        expires_in = data.get("expires_in", 1800)
        self.expires_at = datetime.now() + timedelta(
            seconds=expires_in - TOKEN_REFRESH_MARGIN
        )

        return self.token

    def get_headers(self) -> dict[str, str]:
        """
        Return request headers with a valid Bearer token.
        """
        return {"Authorization": f"Bearer {self.get_token()}"}


token_manager = OpenSkyTokenManager(
    client_id=OPENSKY_CLIENT_ID,
    client_secret=OPENSKY_CLIENT_SECRET,
)


def _make_request(endpoint: str, params: dict[str, Any]) -> list[dict[str, Any]]:
    """
    Send an authenticated GET request to an OpenSky endpoint and return the JSON response.
    """
    url = BASE_URL + endpoint
    max_retries = 3
    sleep_seconds = 5

    for attempt in range(1, max_retries + 1):
        try:
            logger.info(
                f"Sending OpenSky request endpoint={endpoint} "
                f"attempt={attempt}/{max_retries}"
            )

            response = requests.get(
                url,
                params=params,
                headers=token_manager.get_headers(),
                timeout=30,
            )

            if response.status_code == 404:
                logger.warning(
                    f"OpenSky returned 404 for endpoint={endpoint}. Returning empty list."
                )
                return []

            response.raise_for_status()

            logger.info(
                f"OpenSky request succeeded endpoint={endpoint} "
                f"attempt={attempt}/{max_retries}"
            )
            return response.json()

        except Exception as error:
            if attempt == max_retries:
                logger.error(
                    f"OpenSky request failed after {max_retries} attempts "
                    f"endpoint={endpoint} error={error}"
                )
                raise

            logger.warning(
                f"OpenSky request failed endpoint={endpoint} "
                f"attempt={attempt}/{max_retries} error={error}. "
                f"Retrying in {sleep_seconds}s"
            )
            time.sleep(sleep_seconds)

    raise RuntimeError(f"Unexpected retry flow for endpoint={endpoint}")


def get_arrivals_by_airport(
    airport_icao: str, begin: int, end: int
) -> list[dict[str, Any]]:
    """
    Retrieve arrival flights for a given airport and time interval.
    """
    params = {
        "airport": airport_icao,
        "begin": begin,
        "end": end,
    }
    return _make_request("/flights/arrival", params)


def get_departures_by_airport(
    airport_icao: str, begin: int, end: int
) -> list[dict[str, Any]]:
    """
    Retrieve departure flights for a given airport and time interval.
    """
    params = {
        "airport": airport_icao,
        "begin": begin,
        "end": end,
    }
    return _make_request("/flights/departure", params)