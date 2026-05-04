from __future__ import annotations

import json
import re
from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

BASE_URL = "https://data-api.polymarket.com"
DEFAULT_HEADERS = {
    "Accept": "application/json",
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/124.0 Safari/537.36",
}
ADDRESS_RE = re.compile(r"^0x[a-fA-F0-9]{40}$")


class ApiError(RuntimeError):
    """Raised when the Polymarket public API request fails."""


def is_wallet_address(value: str) -> bool:
    return bool(ADDRESS_RE.fullmatch(value.strip()))


def format_timestamp(timestamp: int | float | None) -> str:
    if not timestamp:
        return "-"
    return datetime.fromtimestamp(float(timestamp), tz=UTC).strftime("%Y-%m-%d %H:%M:%S UTC")


def coerce_items(payload: Any) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        if isinstance(payload.get("value"), list):
            return [item for item in payload["value"] if isinstance(item, dict)]
        return [payload]
    return []


def summarize_trades(trades: Iterable[dict[str, Any]]) -> dict[str, Any]:
    items = list(trades)
    buy_notional = 0.0
    sell_notional = 0.0
    buy_count = 0
    sell_count = 0

    for trade in items:
        side = str(trade.get("side", "")).upper()
        size = float(trade.get("size") or 0)
        price = float(trade.get("price") or 0)
        notional = size * price
        if side == "BUY":
            buy_count += 1
            buy_notional += notional
        elif side == "SELL":
            sell_count += 1
            sell_notional += notional

    timestamps = [int(item["timestamp"]) for item in items if item.get("timestamp")]
    last_trade_at = max(timestamps) if timestamps else None

    return {
        "count": len(items),
        "buy_count": buy_count,
        "sell_count": sell_count,
        "buy_notional": round(buy_notional, 4),
        "sell_notional": round(sell_notional, 4),
        "net_notional": round(buy_notional - sell_notional, 4),
        "last_trade_at": last_trade_at,
    }


class PolymarketClient:
    def __init__(self, base_url: str = BASE_URL, timeout: int = 20):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        query = urlencode({k: v for k, v in (params or {}).items() if v is not None})
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"

        request = Request(url, headers=DEFAULT_HEADERS)
        try:
            with urlopen(request, timeout=self.timeout) as response:
                return json.load(response)
        except HTTPError as exc:
            body = exc.read().decode("utf-8", errors="replace")
            message = body.strip() or exc.reason
            raise ApiError(f"HTTP {exc.code} for {url}: {message}") from exc
        except URLError as exc:
            raise ApiError(f"Request failed for {url}: {exc.reason}") from exc

    def get_trades(self, *, user: str | None = None, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        payload = self._request_json("/trades", {"user": user, "limit": limit, "offset": offset})
        return coerce_items(payload)

    def get_activity(self, *, user: str, limit: int = 100, offset: int = 0) -> list[dict[str, Any]]:
        payload = self._request_json("/activity", {"user": user, "limit": limit, "offset": offset})
        return coerce_items(payload)

    def get_positions(self, *, user: str, limit: int = 25, offset: int = 0) -> list[dict[str, Any]]:
        payload = self._request_json("/positions", {"user": user, "limit": limit, "offset": offset})
        return coerce_items(payload)

    def get_value(self, *, user: str) -> float | None:
        payload = self._request_json("/value", {"user": user})
        items = coerce_items(payload)
        if not items:
            return None
        value = items[0].get("value")
        return float(value) if value is not None else None

    def search_recent_users(self, query: str, *, page_size: int = 100, pages: int = 5) -> list[dict[str, Any]]:
        query_lower = query.strip().lower()
        if not query_lower:
            return []

        matches: dict[str, dict[str, Any]] = {}
        for page in range(max(0, pages)):
            batch = self.get_trades(limit=page_size, offset=page * page_size)
            if not batch:
                break
            for trade in batch:
                name = str(trade.get("name") or "")
                pseudonym = str(trade.get("pseudonym") or "")
                fields = {"name": name.lower(), "pseudonym": pseudonym.lower()}
                matched_fields = [field for field, value in fields.items() if query_lower in value and value]
                if not matched_fields:
                    continue

                wallet = str(trade.get("proxyWallet") or "")
                if not wallet:
                    continue

                exact_match = query_lower in {fields["name"], fields["pseudonym"]}
                existing = matches.get(wallet)
                candidate = {
                    "proxyWallet": wallet,
                    "name": name,
                    "pseudonym": pseudonym,
                    "lastSeen": trade.get("timestamp"),
                    "matchedFields": matched_fields,
                    "exactMatch": exact_match,
                }
                if existing is None or exact_match and not existing["exactMatch"]:
                    matches[wallet] = candidate
                elif existing is not None and trade.get("timestamp", 0) > (existing.get("lastSeen") or 0):
                    existing["lastSeen"] = trade.get("timestamp")

        return sorted(
            matches.values(),
            key=lambda item: (
                not item["exactMatch"],
                -(item.get("lastSeen") or 0),
                item.get("name") or item.get("pseudonym") or item["proxyWallet"],
            ),
        )

