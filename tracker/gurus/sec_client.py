"""SEC-safe HTTP client with conservative pacing, retries, and local caching."""

from __future__ import annotations

import json
import random
import time
from dataclasses import dataclass
from pathlib import Path

import requests

from common.logging import Logger
from defs import DATA_DIR_PATH

logger = Logger('gurus.sec_client').get_logger()

RETRYABLE_STATUS_CODES = {403, 429, 500, 502, 503, 504}


@dataclass(slots=True)
class SECRequestConfig:
    """Runtime configuration for polite SEC traffic."""

    user_agent: str = 'GuruTracker/0.1 your_email@example.com'
    timeout_seconds: int = 20
    max_retries: int = 5
    base_delay_seconds: float = 1.0
    backoff_base_seconds: float = 2.0
    max_backoff_seconds: float = 60.0
    enable_cache: bool = True


class SECRequestClient:
    """Shared SEC client for JSON/XML/text fetches."""

    def __init__(self, config: SECRequestConfig, cache_dir: Path | None = None):
        self.config = config
        self.cache_dir = cache_dir or DATA_DIR_PATH.joinpath('sec_cache')
        self.session = requests.Session()
        self.session.headers.update(
            {
                'User-Agent': self.config.user_agent,
                'Accept-Encoding': 'gzip, deflate',
                'Host': 'www.sec.gov',
            }
        )
        self.last_request_at = 0.0
        self.consecutive_failures = 0
        self.cache_hits = 0

    def get_json(self, url: str, cache_key: str | None = None) -> dict:
        cached_content = self._read_cache(cache_key)
        if cached_content is not None:
            logger.info('SEC cache hit: %s', url)
            self.cache_hits += 1
            return json.loads(cached_content.decode('utf-8'))

        response = self._get_with_retries(url)
        payload = response.json()
        if cache_key and self.config.enable_cache:
            self._write_cache(cache_key, response.content)
        return payload

    def get_content(self, url: str, cache_key: str | None = None) -> bytes:
        cached_content = self._read_cache(cache_key)
        if cached_content is not None:
            logger.info('SEC cache hit: %s', url)
            self.cache_hits += 1
            return cached_content

        response = self._get_with_retries(url)
        if cache_key and self.config.enable_cache:
            self._write_cache(cache_key, response.content)
        return response.content

    def _get_with_retries(self, url: str) -> requests.Response:
        self._throttle()
        response: requests.Response | None = None

        for attempt in range(self.config.max_retries):
            response = None
            try:
                response = self.session.get(url, timeout=self.config.timeout_seconds)
                if response.status_code not in RETRYABLE_STATUS_CODES:
                    response.raise_for_status()
                    self.consecutive_failures = 0
                    return response
                error = RuntimeError(f'retryable status code={response.status_code}')
            except (requests.Timeout, requests.ConnectionError, requests.HTTPError, RuntimeError) as error:
                if isinstance(error, requests.HTTPError) and (
                    response is not None and response.status_code not in RETRYABLE_STATUS_CODES
                ):
                    raise
                self.consecutive_failures += 1
                if attempt >= self.config.max_retries - 1:
                    logger.error('SEC request failed after retries: %s (%s)', url, error)
                    raise
                sleep_seconds = self._compute_backoff(attempt)
                logger.warning(
                    'Retrying SEC request (attempt %s/%s) after %.2fs: %s',
                    attempt + 1,
                    self.config.max_retries,
                    sleep_seconds,
                    url,
                )
                time.sleep(sleep_seconds)

        if response is None:
            raise RuntimeError(f'SEC request failed: {url}')
        return response

    def _compute_backoff(self, attempt: int) -> float:
        base_sleep = min(
            self.config.max_backoff_seconds,
            self.config.backoff_base_seconds * (2 ** attempt),
        )
        jitter = random.uniform(0.0, 0.5)
        cooldown = 0.0
        if self.consecutive_failures >= 3:
            cooldown = min(
                self.config.max_backoff_seconds,
                self.config.backoff_base_seconds * self.consecutive_failures,
            )
            logger.warning('SEC cooldown engaged for %.2fs after repeated failures', cooldown)
        return min(self.config.max_backoff_seconds, base_sleep + jitter + cooldown)

    def _throttle(self) -> None:
        elapsed = time.monotonic() - self.last_request_at
        if elapsed < self.config.base_delay_seconds:
            time.sleep(self.config.base_delay_seconds - elapsed)
        self.last_request_at = time.monotonic()

    def _read_cache(self, cache_key: str | None) -> bytes | None:
        if not self.config.enable_cache or cache_key is None:
            return None
        path = self.cache_dir.joinpath(cache_key)
        if path.exists():
            return path.read_bytes()
        return None

    def _write_cache(self, cache_key: str, content: bytes) -> None:
        path = self.cache_dir.joinpath(cache_key)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_bytes(content)
