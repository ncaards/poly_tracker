from __future__ import annotations

import json
import math
import statistics
from collections import defaultdict, deque
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import UTC, date, datetime, timedelta
from functools import lru_cache
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .api import DEFAULT_HEADERS, ApiError, PolymarketClient, is_wallet_address

GAMMA_BASE_URL = "https://gamma-api.polymarket.com"
GENERIC_TAGS = {"all", "featured", "breaking", "new"}
YES_OUTCOME_HINTS = {"yes", "up", "true", "for"}
NO_OUTCOME_HINTS = {"no", "down", "false", "against"}
ODDS_BUCKETS = [
    (0.0, 0.05, "0.00-0.05"),
    (0.05, 0.10, "0.05-0.10"),
    (0.10, 0.20, "0.10-0.20"),
    (0.20, 0.40, "0.20-0.40"),
    (0.40, 0.60, "0.40-0.60"),
    (0.60, 0.80, "0.60-0.80"),
    (0.80, 0.95, "0.80-0.95"),
    (0.95, 1.01, "0.95-1.00"),
]


class GammaClient:
    def __init__(self, base_url: str = GAMMA_BASE_URL, timeout: int = 20):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    def _request_json(self, path: str, params: dict[str, Any] | None = None) -> Any:
        query = urlencode({key: value for key, value in (params or {}).items() if value is not None})
        url = f"{self.base_url}{path}"
        if query:
            url = f"{url}?{query}"
        request = Request(url, headers=DEFAULT_HEADERS)
        try:
            with urlopen(request, timeout=self.timeout) as response:
                return json.load(response)
        except Exception as exc:  # pragma: no cover - network exception branch
            raise ApiError(f"Gamma request failed for {url}: {exc}") from exc

    @lru_cache(maxsize=512)
    def get_event_by_slug(self, slug: str) -> dict[str, Any] | None:
        if not slug:
            return None
        payload = self._request_json("/events", {"slug": slug})
        if isinstance(payload, list) and payload:
            return payload[0]
        return None


def parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    normalized = value.replace("Z", "+00:00")
    try:
        dt = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    return dt if dt.tzinfo else dt.replace(tzinfo=UTC)


def unix_to_date_string(timestamp: int | float | None) -> str | None:
    if not timestamp:
        return None
    return datetime.fromtimestamp(float(timestamp), tz=UTC).date().isoformat()


def date_range(start: date, end: date) -> list[date]:
    days = []
    current = start
    while current <= end:
        days.append(current)
        current += timedelta(days=1)
    return days


def safe_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def parse_jsonish_list(value: Any) -> list[Any]:
    if isinstance(value, list):
        return value
    if value in (None, ""):
        return []
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except json.JSONDecodeError:
            return []
        return parsed if isinstance(parsed, list) else []
    return []


def choose_category(event: dict[str, Any] | None, fallback_title: str = "") -> tuple[str, list[str]]:
    tags = event.get("tags") if isinstance(event, dict) else []
    labels = []
    for tag in tags or []:
        label = str(tag.get("label") or "").strip()
        if label:
            labels.append(label)
    preferred = [label for label in labels if label.lower() not in GENERIC_TAGS]
    if preferred:
        return preferred[0], preferred

    title = fallback_title.lower()
    heuristics = [
        ("weather", ["weather", "hurricane", "temperature", "rain", "snow"]),
        ("politics", ["election", "trump", "biden", "senate", "house", "ukraine", "government"]),
        ("crypto", ["bitcoin", "ethereum", "solana", "btc", "eth", "crypto"]),
        ("sports", ["nba", "nfl", "mlb", "soccer", "football", "tennis", "ufc"]),
        ("culture", ["movie", "gta", "song", "album", "celebrity", "show"]),
        ("business", ["tesla", "apple", "stock", "fed", "economy", "tariff"]),
    ]
    for label, needles in heuristics:
        if any(needle in title for needle in needles):
            return label.title(), []
    return "Uncategorized", []


def bucket_for_price(price: float) -> str:
    for low, high, label in ODDS_BUCKETS:
        if low <= price < high:
            return label
    return "Other"


def fetch_all_trades(
    client: PolymarketClient,
    wallet: str,
    *,
    limit: int | None = 10000,
    page_size: int = 1000,
) -> tuple[list[dict[str, Any]], bool]:
    trades: list[dict[str, Any]] = []
    offset = 0
    hit_offset_cap = False
    while True:
        if limit is None:
            batch_size = page_size
        else:
            if len(trades) >= limit:
                break
            batch_size = min(page_size, limit - len(trades))
        try:
            batch = client.get_trades(user=wallet, limit=batch_size, offset=offset)
        except ApiError as exc:
            if "max historical activity offset" in str(exc).lower():
                hit_offset_cap = True
                break
            raise
        if not batch:
            break
        trades.extend(batch)
        offset += len(batch)
        if len(batch) < batch_size:
            break
    return (trades if limit is None else trades[:limit]), hit_offset_cap


def fetch_all_positions(client: PolymarketClient, wallet: str, *, limit: int = 200, page_size: int = 100) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    offset = 0
    while len(positions) < limit:
        batch_size = min(page_size, limit - len(positions))
        batch = client.get_positions(user=wallet, limit=batch_size, offset=offset)
        if not batch:
            break
        positions.extend(batch)
        offset += len(batch)
        if len(batch) < batch_size:
            break
    return positions[:limit]


def fetch_event_map(gamma_client: GammaClient, event_slugs: set[str]) -> dict[str, dict[str, Any]]:
    event_map: dict[str, dict[str, Any]] = {}
    if not event_slugs:
        return event_map

    with ThreadPoolExecutor(max_workers=8) as pool:
        future_map = {
            pool.submit(gamma_client.get_event_by_slug, slug): slug
            for slug in sorted(event_slugs)
            if slug
        }
        for future in as_completed(future_map):
            slug = future_map[future]
            try:
                event = future.result()
            except ApiError:
                continue
            if event:
                event_map[slug] = event
    return event_map


def build_market_catalog(event_map: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    catalog: dict[str, dict[str, Any]] = {}
    for event_slug, event in event_map.items():
        category, tag_labels = choose_category(event, str(event.get("title") or event_slug))
        for market in event.get("markets") or []:
            condition_id = str(market.get("conditionId") or "")
            if not condition_id:
                continue
            outcomes = [str(item) for item in parse_jsonish_list(market.get("outcomes"))]
            outcome_prices = [safe_float(item) for item in parse_jsonish_list(market.get("outcomePrices"))]
            price_map = {outcomes[index]: outcome_prices[index] for index in range(min(len(outcomes), len(outcome_prices)))}
            resolution_status = str(market.get("umaResolutionStatus") or "").lower()
            closed = bool(market.get("closed") or event.get("closed"))
            resolved = resolution_status == "resolved" or (closed and any(price in (0.0, 1.0) for price in price_map.values()))
            catalog[condition_id] = {
                "conditionId": condition_id,
                "title": market.get("question") or event.get("title") or event_slug,
                "eventSlug": event_slug,
                "category": category,
                "tags": tag_labels,
                "endDate": market.get("endDate") or event.get("endDate"),
                "endTimestamp": parse_iso_datetime(market.get("endDate") or event.get("endDate")).timestamp()
                if parse_iso_datetime(market.get("endDate") or event.get("endDate"))
                else None,
                "closed": closed,
                "resolved": resolved,
                "outcomes": outcomes,
                "outcomePrices": price_map,
            }
    return catalog


def build_position_map(positions: list[dict[str, Any]]) -> dict[tuple[str, str], dict[str, Any]]:
    mapping: dict[tuple[str, str], dict[str, Any]] = {}
    for position in positions:
        key = (str(position.get("conditionId") or ""), str(position.get("outcome") or ""))
        if key[0] and key[1]:
            mapping[key] = position
    return mapping


def _init_bucket_map() -> dict[str, dict[str, float | int | str]]:
    return {
        label: {"bucket": label, "count": 0, "notional": 0.0}
        for _, _, label in ODDS_BUCKETS
    }


def _init_side_outcome_bucket_map() -> dict[str, dict[str, dict[str, float | int | str]]]:
    def _side_template() -> dict[str, dict[str, float | int | str]]:
        return {
            label: {
                "bucket": label,
                "yesNotional": 0.0,
                "noNotional": 0.0,
                "otherNotional": 0.0,
                "yesCount": 0,
                "noCount": 0,
                "otherCount": 0,
                "totalNotional": 0.0,
            }
            for _, _, label in ODDS_BUCKETS
        }

    return {"BUY": _side_template(), "SELL": _side_template()}


def classify_outcome_label(outcome: str, outcomes: list[str]) -> str:
    normalized = (outcome or "").strip().lower()
    if normalized in YES_OUTCOME_HINTS:
        return "yes"
    if normalized in NO_OUTCOME_HINTS:
        return "no"

    normalized_outcomes = [str(item).strip().lower() for item in outcomes]
    if len(normalized_outcomes) >= 2:
        if normalized == normalized_outcomes[0]:
            return "yes"
        if normalized == normalized_outcomes[1]:
            return "no"

    return "other"


def _annualized_sharpe(returns: list[float]) -> float | None:
    if len(returns) < 2:
        return None
    if all(abs(value) < 1e-12 for value in returns):
        return 0.0
    deviation = statistics.pstdev(returns)
    if deviation < 1e-12:
        return 0.0
    return statistics.fmean(returns) / deviation * math.sqrt(365)


def analyze_wallet(wallet: str, *, trade_limit: int | None = 10000, timeout: int = 20) -> dict[str, Any]:
    if not is_wallet_address(wallet):
        raise ApiError("Please enter a valid 0x... wallet address.")

    client = PolymarketClient(timeout=timeout)
    gamma_client = GammaClient(timeout=timeout)

    trades, hit_offset_cap = fetch_all_trades(client, wallet.lower(), limit=trade_limit)
    positions = fetch_all_positions(client, wallet.lower(), limit=300)
    current_value = client.get_value(user=wallet.lower())

    if not trades:
        return {
            "wallet": wallet.lower(),
            "tradeCount": 0,
            "positionsCount": len(positions),
            "currentValue": current_value,
            "warnings": ["No public trades were returned for this wallet."],
            "generatedAt": datetime.now(tz=UTC).isoformat(),
        }

    event_slugs = {str(trade.get("eventSlug") or trade.get("slug") or "") for trade in trades}
    event_slugs.update(str(position.get("eventSlug") or position.get("slug") or "") for position in positions)
    event_map = fetch_event_map(gamma_client, event_slugs)
    market_catalog = build_market_catalog(event_map)
    position_map = build_position_map(positions)

    sorted_trades = sorted(trades, key=lambda item: (int(item.get("timestamp") or 0), str(item.get("transactionHash") or "")))
    category_map: dict[str, dict[str, Any]] = defaultdict(lambda: {"category": "", "buyCount": 0, "buyNotional": 0.0})
    market_map: dict[str, dict[str, Any]] = defaultdict(
        lambda: {
            "conditionId": "",
            "title": "",
            "category": "",
            "buyCount": 0,
            "sellCount": 0,
            "buyNotional": 0.0,
            "sellNotional": 0.0,
            "realizedPnl": 0.0,
            "unrealizedPnl": 0.0,
            "resolved": False,
            "endDate": None,
        }
    )
    odds_bucket_map = _init_bucket_map()
    odds_by_side_bucket_map = _init_side_outcome_bucket_map()
    lots: dict[tuple[str, str], deque[dict[str, Any]]] = defaultdict(deque)
    daily_pnl: dict[str, float] = defaultdict(float)
    daily_capital_delta: dict[str, float] = defaultdict(float)
    hold_seconds: list[float] = []
    operations: list[dict[str, Any]] = []
    operation_pnl: dict[int, float] = defaultdict(float)
    operation_matched_qty: dict[int, float] = defaultdict(float)
    operation_unmatched_sell_qty: dict[int, float] = defaultdict(float)
    operation_buy_remaining_qty: dict[int, float] = defaultdict(float)
    held_to_close_operation_ids: set[int] = set()
    missing_basis_sell_count = 0
    missing_basis_quantity = 0.0
    resolved_condition_pnls: dict[str, dict[str, Any]] = defaultdict(lambda: {"pnl": 0.0, "cost": 0.0})
    held_to_close_conditions: set[str] = set()
    closed_before_close_conditions: set[str] = set()

    total_buy_notional = 0.0
    total_sell_notional = 0.0
    weighted_buy_price_numerator = 0.0
    weighted_buy_size = 0.0

    for operation_index, trade in enumerate(sorted_trades):
        condition_id = str(trade.get("conditionId") or "")
        outcome = str(trade.get("outcome") or "")
        title = str(trade.get("title") or trade.get("slug") or condition_id)
        timestamp = int(trade.get("timestamp") or 0)
        side = str(trade.get("side") or "").upper()
        size = safe_float(trade.get("size"))
        price = safe_float(trade.get("price"))
        notional = size * price
        meta = market_catalog.get(condition_id, {})
        category = str(meta.get("category") or choose_category(None, title)[0])
        trade_date = unix_to_date_string(timestamp)
        market_entry = market_map[condition_id]
        outcome_label = classify_outcome_label(outcome, list(meta.get("outcomes") or []))
        operation = {
            "index": operation_index,
            "timestamp": timestamp,
            "date": trade_date,
            "side": side,
            "outcome": outcome,
            "outcomeClass": outcome_label,
            "title": title,
            "conditionId": condition_id,
            "category": category,
            "price": round(price, 6),
            "size": round(size, 6),
            "notional": round(notional, 6),
            "resolved": bool(meta.get("resolved")),
            "endDate": meta.get("endDate"),
        }
        operations.append(operation)

        side_buckets = odds_by_side_bucket_map.get(side)
        if side_buckets is not None:
            side_bucket = side_buckets[bucket_for_price(price)]
            side_bucket["totalNotional"] += notional
            side_bucket[f"{outcome_label}Notional"] += notional
            side_bucket[f"{outcome_label}Count"] += 1

        market_entry.update(
            {
                "conditionId": condition_id,
                "title": title,
                "category": category,
                "resolved": bool(meta.get("resolved")),
                "endDate": meta.get("endDate"),
            }
        )

        if side == "BUY":
            operation_buy_remaining_qty[operation_index] = size
            total_buy_notional += notional
            weighted_buy_price_numerator += price * size
            weighted_buy_size += size
            category_entry = category_map[category]
            category_entry["category"] = category
            category_entry["buyCount"] += 1
            category_entry["buyNotional"] += notional
            bucket = odds_bucket_map[bucket_for_price(price)]
            bucket["count"] += 1
            bucket["notional"] += notional
            market_entry["buyCount"] += 1
            market_entry["buyNotional"] += notional
            lots[(condition_id, outcome)].append(
                {
                    "qty": size,
                    "price": price,
                    "timestamp": timestamp,
                    "title": title,
                    "conditionId": condition_id,
                    "outcome": outcome,
                    "category": category,
                    "operationIndex": operation_index,
                }
            )
            if trade_date:
                daily_capital_delta[trade_date] += notional
        elif side == "SELL":
            operation_unmatched_sell_qty[operation_index] = size
            total_sell_notional += notional
            market_entry["sellCount"] += 1
            market_entry["sellNotional"] += notional
            remaining = size
            inventory = lots[(condition_id, outcome)]
            while remaining > 1e-12 and inventory:
                lot = inventory[0]
                matched_qty = min(remaining, lot["qty"])
                cost_basis = matched_qty * lot["price"]
                proceeds = matched_qty * price
                pnl = proceeds - cost_basis
                close_date = trade_date
                if close_date:
                    daily_pnl[close_date] += pnl
                    daily_capital_delta[close_date] -= cost_basis
                open_operation_index = int(lot.get("operationIndex"))
                operation_pnl[open_operation_index] += pnl
                operation_pnl[operation_index] += pnl
                operation_matched_qty[open_operation_index] += matched_qty
                operation_matched_qty[operation_index] += matched_qty
                operation_buy_remaining_qty[open_operation_index] = max(
                    0.0, operation_buy_remaining_qty[open_operation_index] - matched_qty
                )
                operation_unmatched_sell_qty[operation_index] = max(
                    0.0, operation_unmatched_sell_qty[operation_index] - matched_qty
                )
                hold_seconds.append(max(0.0, float(timestamp - lot["timestamp"])))
                market_entry["realizedPnl"] += pnl
                if meta.get("resolved"):
                    resolved_condition_pnls[condition_id]["pnl"] += pnl
                    resolved_condition_pnls[condition_id]["cost"] += cost_basis
                lot["qty"] -= matched_qty
                remaining -= matched_qty
                if lot["qty"] <= 1e-12:
                    inventory.popleft()
            if remaining > 1e-12:
                missing_basis_sell_count += 1
                missing_basis_quantity += remaining
                closed_before_close_conditions.add(condition_id)

    unrealized_pnl = 0.0
    realized_settled_total = sum(daily_pnl.values())
    open_positions_summary = []

    for key, inventory in lots.items():
        condition_id, outcome = key
        meta = market_catalog.get(condition_id, {})
        position = position_map.get(key, {})
        current_mark = None
        if outcome:
            current_mark = meta.get("outcomePrices", {}).get(outcome)
        if current_mark is None and position:
            current_mark = position.get("curPrice")
        current_mark = safe_float(current_mark, default=0.0)
        resolved = bool(meta.get("resolved"))
        end_timestamp = meta.get("endTimestamp")
        end_date = unix_to_date_string(end_timestamp)

        total_qty = 0.0
        total_cost = 0.0
        total_market_pnl = 0.0
        for lot in inventory:
            qty = safe_float(lot.get("qty"))
            cost = qty * safe_float(lot.get("price"))
            total_qty += qty
            total_cost += cost
            pnl = qty * (current_mark - safe_float(lot.get("price")))
            total_market_pnl += pnl
            if resolved and end_date:
                daily_pnl[end_date] += pnl
                daily_capital_delta[end_date] -= cost
                open_operation_index = int(lot.get("operationIndex"))
                operation_pnl[open_operation_index] += pnl
                operation_matched_qty[open_operation_index] += qty
                operation_buy_remaining_qty[open_operation_index] = max(
                    0.0, operation_buy_remaining_qty[open_operation_index] - qty
                )
                held_to_close_operation_ids.add(open_operation_index)
                hold_seconds.append(max(0.0, float((end_timestamp or 0) - lot["timestamp"])))
                held_to_close_conditions.add(condition_id)
                resolved_condition_pnls[condition_id]["pnl"] += pnl
                resolved_condition_pnls[condition_id]["cost"] += cost
        market_map[condition_id]["unrealizedPnl"] += 0.0 if resolved else total_market_pnl
        if resolved:
            realized_settled_total += total_market_pnl
        else:
            unrealized_pnl += total_market_pnl
            open_positions_summary.append(
                {
                    "conditionId": condition_id,
                    "title": str(meta.get("title") or position.get("title") or condition_id),
                    "category": str(meta.get("category") or position.get("category") or "Uncategorized"),
                    "outcome": outcome,
                    "size": round(total_qty, 4),
                    "avgPrice": round(total_cost / total_qty, 4) if total_qty else 0.0,
                    "currentPrice": round(current_mark, 4),
                    "unrealizedPnl": round(total_market_pnl, 4),
                }
            )

    pnl_start_date = datetime.fromtimestamp(int(sorted_trades[0].get("timestamp") or 0), tz=UTC).date()
    today = datetime.now(tz=UTC).date()
    cumulative = 0.0
    capital_at_risk = 0.0
    pnl_timeline = []
    daily_returns = []
    for current_day in date_range(pnl_start_date, today):
        day_key = current_day.isoformat()
        start_capital = capital_at_risk
        pnl_value = daily_pnl.get(day_key, 0.0)
        daily_return = pnl_value / start_capital if start_capital > 1e-12 else 0.0
        daily_returns.append(daily_return)
        cumulative += pnl_value
        capital_at_risk = max(0.0, capital_at_risk + daily_capital_delta.get(day_key, 0.0))
        pnl_timeline.append(
            {
                "date": day_key,
                "dailyPnl": round(pnl_value, 4),
                "cumulativePnl": round(cumulative, 4),
                "capitalAtRisk": round(capital_at_risk, 4),
                "dailyReturn": round(daily_return, 6),
            }
        )

    now_dt = datetime.now(tz=UTC)
    seven_days_ago = (now_dt - timedelta(days=7)).date().isoformat()
    ytd_start = date(now_dt.year, 1, 1).isoformat()
    pnl_last_7d = sum(item["dailyPnl"] for item in pnl_timeline if item["date"] >= seven_days_ago)
    pnl_ytd = sum(item["dailyPnl"] for item in pnl_timeline if item["date"] >= ytd_start)

    resolved_markets = [item for item in resolved_condition_pnls.values() if safe_float(item.get("cost")) > 0]
    profitable_resolved = [item for item in resolved_markets if safe_float(item.get("pnl")) > 0]
    resolved_count = len(resolved_markets)
    profitable_rate = (len(profitable_resolved) / resolved_count * 100.0) if resolved_count else None

    resolved_condition_ids = {condition_id for condition_id, values in resolved_condition_pnls.items() if safe_float(values.get("cost")) > 0}
    hold_to_close_rate = (
        len(held_to_close_conditions & resolved_condition_ids) / len(resolved_condition_ids) * 100.0
        if resolved_condition_ids
        else None
    )

    top_categories = sorted(category_map.values(), key=lambda item: item["buyNotional"], reverse=True)
    odds_distribution = [odds_bucket_map[label] for _, _, label in ODDS_BUCKETS]
    top_markets = sorted(
        (
            {
                **item,
                "buyNotional": round(float(item["buyNotional"]), 4),
                "sellNotional": round(float(item["sellNotional"]), 4),
                "realizedPnl": round(float(item["realizedPnl"]), 4),
                "unrealizedPnl": round(float(item["unrealizedPnl"]), 4),
            }
            for item in market_map.values()
            if item["conditionId"]
        ),
        key=lambda item: (item["buyNotional"] + item["sellNotional"]),
        reverse=True,
    )

    weighted_average_buy_price = weighted_buy_price_numerator / weighted_buy_size if weighted_buy_size else None
    low_odds_notional = sum(item["notional"] for item in odds_distribution if item["bucket"] in {"0.00-0.05", "0.05-0.10"})
    low_odds_share = (low_odds_notional / total_buy_notional * 100.0) if total_buy_notional else None
    average_hold_hours = statistics.fmean(hold_seconds) / 3600 if hold_seconds else None
    sharpe_ratio = _annualized_sharpe(daily_returns)

    warnings = [
        f"Analytics are based on the last {len(sorted_trades)} public trades returned by Polymarket, not the wallet's full lifetime history."
    ]
    if hit_offset_cap:
        warnings.append(
            "Polymarket's public trade endpoint currently enforces a historical offset cap; this wallet may have additional older trades that are not queryable through this API."
        )
    if missing_basis_sell_count:
        warnings.append(
            f"{missing_basis_sell_count} sell trades ({round(missing_basis_quantity, 4)} shares) had no matching buy inside the fetched window, so realized PnL is understated for those exits."
        )
    warnings.append(
        "Unrealized PnL uses the latest public outcome prices, while the PnL timeline only includes realized sells and resolved-at-close settlements."
    )
    warnings.append(
        "Hit rate and hold-to-close are calculated only on resolved markets where enough trade history was fetched to build a cost basis."
    )
    warnings.append(
        "Sharpe is an estimate based on daily realized/settled returns and current cost basis at risk, annualized using 365 days."
    )

    operations_desc = sorted(operations, key=lambda item: (item["timestamp"], item["index"]), reverse=True)
    for operation in operations_desc:
        operation_index = int(operation["index"])
        pnl_value = operation_pnl.get(operation_index, 0.0)
        matched_qty = operation_matched_qty.get(operation_index, 0.0)
        remaining_buy_qty = operation_buy_remaining_qty.get(operation_index, 0.0)
        unmatched_sell_qty = operation_unmatched_sell_qty.get(operation_index, 0.0)
        side = operation.get("side")

        if side == "BUY":
            if remaining_buy_qty > 1e-9 and not operation.get("resolved"):
                result = "Open"
            elif pnl_value > 1e-9:
                result = "Hit"
            elif pnl_value < -1e-9:
                result = "Miss"
            else:
                result = "Flat"
        else:
            if unmatched_sell_qty > 1e-9:
                result = "Unknown"
            elif pnl_value > 1e-9:
                result = "Hit"
            elif pnl_value < -1e-9:
                result = "Miss"
            elif matched_qty > 1e-9:
                result = "Flat"
            else:
                result = "Unknown"

        operation["pnl"] = round(pnl_value, 6)
        operation["matchedQty"] = round(matched_qty, 6)
        operation["remainingQty"] = round(remaining_buy_qty if side == "BUY" else unmatched_sell_qty, 6)
        operation["result"] = result
        operation["heldToClose"] = bool(operation_index in held_to_close_operation_ids)

    odds_distribution = [odds_bucket_map[label] for _, _, label in ODDS_BUCKETS]
    buy_total_notional = sum(item["totalNotional"] for item in odds_by_side_bucket_map["BUY"].values())
    sell_total_notional = sum(item["totalNotional"] for item in odds_by_side_bucket_map["SELL"].values())

    odds_by_side = {
        "buy": [
            {
                "bucket": item["bucket"],
                "yesNotional": round(float(item["yesNotional"]), 4),
                "noNotional": round(float(item["noNotional"]), 4),
                "otherNotional": round(float(item["otherNotional"]), 4),
                "yesCount": int(item["yesCount"]),
                "noCount": int(item["noCount"]),
                "otherCount": int(item["otherCount"]),
                "totalNotional": round(float(item["totalNotional"]), 4),
                "share": round(float(item["totalNotional"]) / buy_total_notional * 100.0, 2)
                if buy_total_notional
                else 0.0,
            }
            for item in (odds_by_side_bucket_map["BUY"][label] for _, _, label in ODDS_BUCKETS)
        ],
        "sell": [
            {
                "bucket": item["bucket"],
                "yesNotional": round(float(item["yesNotional"]), 4),
                "noNotional": round(float(item["noNotional"]), 4),
                "otherNotional": round(float(item["otherNotional"]), 4),
                "yesCount": int(item["yesCount"]),
                "noCount": int(item["noCount"]),
                "otherCount": int(item["otherCount"]),
                "totalNotional": round(float(item["totalNotional"]), 4),
                "share": round(float(item["totalNotional"]) / sell_total_notional * 100.0, 2)
                if sell_total_notional
                else 0.0,
            }
            for item in (odds_by_side_bucket_map["SELL"][label] for _, _, label in ODDS_BUCKETS)
        ],
    }

    return {
        "wallet": wallet.lower(),
        "generatedAt": now_dt.isoformat(),
        "tradeCount": len(sorted_trades),
        "positionsCount": len(positions),
        "currentValue": current_value,
        "summary": {
            "totalBuyNotional": round(total_buy_notional, 4),
            "totalSellNotional": round(total_sell_notional, 4),
            "weightedAverageBuyPrice": round(weighted_average_buy_price, 4) if weighted_average_buy_price is not None else None,
            "estimatedRealizedSettledPnl": round(sum(item["dailyPnl"] for item in pnl_timeline), 4),
            "estimatedOpenUnrealizedPnl": round(unrealized_pnl, 4),
            "estimatedTotalPnl": round(sum(item["dailyPnl"] for item in pnl_timeline) + unrealized_pnl, 4),
            "pnlLast7d": round(pnl_last_7d, 4),
            "pnlYtd": round(pnl_ytd, 4),
            "resolvedMarketHitRate": round(profitable_rate, 2) if profitable_rate is not None else None,
            "holdToCloseRate": round(hold_to_close_rate, 2) if hold_to_close_rate is not None else None,
            "sharpeRatio": round(sharpe_ratio, 3) if sharpe_ratio is not None else None,
            "averageHoldHours": round(average_hold_hours, 2) if average_hold_hours is not None else None,
            "lowOddsBuyShare": round(low_odds_share, 2) if low_odds_share is not None else None,
            "resolvedMarketsCount": resolved_count,
        },
        "categoryDistribution": [
            {
                "category": item["category"],
                "buyCount": int(item["buyCount"]),
                "buyNotional": round(float(item["buyNotional"]), 4),
                "buyShare": round(float(item["buyNotional"]) / total_buy_notional * 100.0, 2) if total_buy_notional else 0.0,
            }
            for item in top_categories
        ],
        "oddsDistribution": [
            {
                "bucket": str(item["bucket"]),
                "count": int(item["count"]),
                "notional": round(float(item["notional"]), 4),
                "share": round(float(item["notional"]) / total_buy_notional * 100.0, 2) if total_buy_notional else 0.0,
            }
            for item in odds_distribution
        ],
        "oddsBySide": odds_by_side,
        "operations": operations_desc,
        "pnlTimeline": pnl_timeline,
        "topMarkets": top_markets[:12],
        "openPositions": sorted(open_positions_summary, key=lambda item: item["unrealizedPnl"], reverse=True)[:12],
        "warnings": warnings,
    }

