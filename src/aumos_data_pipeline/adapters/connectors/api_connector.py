"""REST API source connector.

Ingests data from a paginated REST API endpoint. Handles common
pagination patterns (page/offset, cursor, link header).
"""

from typing import Any, AsyncIterator

import httpx
import pandas as pd

from aumos_common.observability import get_logger

from aumos_data_pipeline.core.interfaces import IngestorProtocol

logger = get_logger(__name__)

_DEFAULT_TIMEOUT = 30.0
_MAX_PAGES = 10_000  # Safety limit to prevent infinite pagination


class ApiConnector:
    """Ingests data from a REST API endpoint.

    Implements IngestorProtocol. Supports:
    - Page-based pagination: ?page=N&page_size=M
    - Offset-based pagination: ?offset=N&limit=M
    - Cursor-based pagination: ?cursor=TOKEN
    - Single response (non-paginated)

    Expected source_config keys:
        url: Base API URL
        method: HTTP method (default: GET)
        headers: HTTP headers dict (for auth tokens, etc.)
        params: Base query parameters
        data_key: JSON key containing the records array (default: 'data')
        pagination_type: 'page' | 'offset' | 'cursor' | 'none' (default: none)
        page_param: Page number parameter name (default: 'page')
        page_size_param: Page size parameter name (default: 'page_size')
        cursor_key: JSON key containing the next cursor (for cursor pagination)
        total_key: JSON key containing total count (for early termination)
        auth_token: Bearer token (alternative to setting in headers)
    """

    async def ingest(
        self,
        source_config: dict[str, Any],
        chunk_size: int = 10_000,
    ) -> AsyncIterator[pd.DataFrame]:
        """Fetch data from a REST API, handling pagination.

        Args:
            source_config: API endpoint configuration.
            chunk_size: Yield a DataFrame chunk every N rows.

        Yields:
            DataFrame chunks.
        """
        url = source_config["url"]
        method = source_config.get("method", "GET").upper()
        headers: dict[str, str] = dict(source_config.get("headers", {}))
        base_params: dict[str, Any] = dict(source_config.get("params", {}))
        data_key = source_config.get("data_key", "data")
        pagination_type = source_config.get("pagination_type", "none")

        if "auth_token" in source_config:
            headers["Authorization"] = f"Bearer {source_config['auth_token']}"

        logger.info("API ingestion starting", url=url, pagination_type=pagination_type)

        async with httpx.AsyncClient(timeout=_DEFAULT_TIMEOUT, headers=headers) as client:
            accumulated: list[dict[str, Any]] = []

            if pagination_type == "none":
                response = await client.request(method, url, params=base_params)
                response.raise_for_status()
                records = self._extract_records(response.json(), data_key)
                if records:
                    yield pd.DataFrame(records)
                return

            # Paginated ingestion
            page = 1
            offset = 0
            cursor: str | None = None

            for _ in range(_MAX_PAGES):
                params = dict(base_params)

                if pagination_type == "page":
                    params[source_config.get("page_param", "page")] = page
                    params[source_config.get("page_size_param", "page_size")] = chunk_size
                elif pagination_type == "offset":
                    params["offset"] = offset
                    params["limit"] = chunk_size
                elif pagination_type == "cursor" and cursor:
                    params["cursor"] = cursor

                response = await client.request(method, url, params=params)
                response.raise_for_status()
                body = response.json()
                records = self._extract_records(body, data_key)

                if not records:
                    break

                accumulated.extend(records)

                if len(accumulated) >= chunk_size:
                    yield pd.DataFrame(accumulated[:chunk_size])
                    accumulated = accumulated[chunk_size:]

                # Advance pagination
                if pagination_type == "page":
                    page += 1
                elif pagination_type == "offset":
                    offset += len(records)
                elif pagination_type == "cursor":
                    cursor_key = source_config.get("cursor_key", "next_cursor")
                    cursor = body.get(cursor_key)
                    if not cursor:
                        break

                # Stop if returned fewer records than page size (last page)
                if len(records) < chunk_size and pagination_type != "cursor":
                    break

            if accumulated:
                yield pd.DataFrame(accumulated)

    def _extract_records(self, body: Any, data_key: str) -> list[dict[str, Any]]:
        """Extract the records list from an API response body.

        Args:
            body: Parsed JSON response (dict or list).
            data_key: Key to extract from dict responses.

        Returns:
            List of record dicts.
        """
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            records = body.get(data_key, [])
            return records if isinstance(records, list) else []
        return []

    async def validate_connection(self, source_config: dict[str, Any]) -> bool:
        """Test that the API endpoint is reachable.

        Args:
            source_config: Must include 'url'.

        Returns:
            True if a HEAD/GET request succeeds with 2xx status.
        """
        try:
            headers: dict[str, str] = dict(source_config.get("headers", {}))
            if "auth_token" in source_config:
                headers["Authorization"] = f"Bearer {source_config['auth_token']}"

            async with httpx.AsyncClient(timeout=10.0, headers=headers) as client:
                response = await client.head(source_config["url"])
                return response.status_code < 400
        except Exception as exc:
            logger.warning("API connection validation failed", error=str(exc))
            return False
