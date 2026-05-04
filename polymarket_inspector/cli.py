from __future__ import annotations

import argparse
import csv
import json
from pathlib import Path
from typing import Any

from .api import ApiError, PolymarketClient, format_timestamp, is_wallet_address, summarize_trades


def _format_money(value: float | None) -> str:
    if value is None:
        return "-"
    return f"${value:,.2f}"


def _truncate(text: Any, width: int) -> str:
    value = str(text)
    if len(value) <= width:
        return value
    return value[: width - 1] + "…"


def _print_table(rows: list[dict[str, Any]], columns: list[tuple[str, str]], *, empty_message: str) -> None:
    if not rows:
        print(empty_message)
        return

    widths: dict[str, int] = {}
    for key, title in columns:
        widths[key] = max(len(title), *(len(str(row.get(key, ""))) for row in rows))
        widths[key] = min(widths[key], 42)

    header = " | ".join(title.ljust(widths[key]) for key, title in columns)
    divider = "-+-".join("-" * widths[key] for key, _ in columns)
    print(header)
    print(divider)
    for row in rows:
        print(" | ".join(_truncate(row.get(key, ""), widths[key]).ljust(widths[key]) for key, _ in columns))


def _resolve_identifier(client: PolymarketClient, identifier: str, *, pages: int, page_size: int) -> tuple[str, str]:
    cleaned = identifier.strip()
    if is_wallet_address(cleaned):
        return cleaned.lower(), "wallet"

    matches = client.search_recent_users(cleaned, pages=pages, page_size=page_size)
    exact = [item for item in matches if item["exactMatch"]]
    if len(exact) == 1:
        return exact[0]["proxyWallet"].lower(), f"recent exact match on {', '.join(exact[0]['matchedFields'])}"

    if exact:
        message_lines = [
            f"The identifier '{identifier}' matched multiple recent users. Please retry with a wallet address:",
        ]
        for match in exact[:10]:
            display = match["name"] or match["pseudonym"] or "(unnamed)"
            message_lines.append(f"- {display}: {match['proxyWallet']}")
        raise ApiError("\n".join(message_lines))

    if matches:
        suggestions = ", ".join(
            f"{item['name'] or item['pseudonym'] or item['proxyWallet']} ({item['proxyWallet']})"
            for item in matches[:5]
        )
        raise ApiError(
            f"No exact recent username/pseudonym match for '{identifier}'. Closest recent matches: {suggestions}"
        )

    raise ApiError(
        f"Could not resolve '{identifier}'. Use a wallet address, or try 'search-users {identifier}' first."
    )


def _fetch_paged_trades(client: PolymarketClient, wallet: str, *, limit: int, page_size: int) -> list[dict[str, Any]]:
    remaining = limit
    offset = 0
    all_trades: list[dict[str, Any]] = []

    while remaining > 0:
        batch_size = min(page_size, remaining)
        batch = client.get_trades(user=wallet, limit=batch_size, offset=offset)
        if not batch:
            break
        all_trades.extend(batch)
        if len(batch) < batch_size:
            break
        offset += len(batch)
        remaining -= len(batch)

    return all_trades[:limit]


def _trade_rows(trades: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for trade in trades:
        size = float(trade.get("size") or 0)
        price = float(trade.get("price") or 0)
        rows.append(
            {
                "time": format_timestamp(trade.get("timestamp")),
                "side": trade.get("side") or "-",
                "market": trade.get("title") or trade.get("slug") or "-",
                "outcome": trade.get("outcome") or "-",
                "size": f"{size:.4f}",
                "price": f"{price:.4f}",
                "notional": f"{size * price:.4f}",
                "tx": trade.get("transactionHash") or "-",
            }
        )
    return rows


def _position_rows(positions: list[dict[str, Any]]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for position in positions:
        rows.append(
            {
                "market": position.get("title") or position.get("slug") or "-",
                "outcome": position.get("outcome") or "-",
                "size": f"{float(position.get('size') or 0):.4f}",
                "avg_price": f"{float(position.get('avgPrice') or 0):.4f}",
                "current_value": f"{float(position.get('currentValue') or 0):.4f}",
                "cash_pnl": f"{float(position.get('cashPnl') or 0):.4f}",
                "end_date": position.get("endDate") or "-",
            }
        )
    return rows


def _write_csv(path: Path, trades: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "timestamp",
        "datetime_utc",
        "side",
        "title",
        "slug",
        "outcome",
        "size",
        "price",
        "notional",
        "proxyWallet",
        "transactionHash",
        "conditionId",
        "asset",
    ]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for trade in trades:
            size = float(trade.get("size") or 0)
            price = float(trade.get("price") or 0)
            writer.writerow(
                {
                    "timestamp": trade.get("timestamp") or "",
                    "datetime_utc": format_timestamp(trade.get("timestamp")),
                    "side": trade.get("side") or "",
                    "title": trade.get("title") or "",
                    "slug": trade.get("slug") or "",
                    "outcome": trade.get("outcome") or "",
                    "size": size,
                    "price": price,
                    "notional": round(size * price, 8),
                    "proxyWallet": trade.get("proxyWallet") or "",
                    "transactionHash": trade.get("transactionHash") or "",
                    "conditionId": trade.get("conditionId") or "",
                    "asset": trade.get("asset") or "",
                }
            )


def command_summary(args: argparse.Namespace) -> int:
    client = PolymarketClient(timeout=args.timeout)
    wallet, resolution = _resolve_identifier(
        client,
        args.identifier,
        pages=args.search_pages,
        page_size=args.search_page_size,
    )
    value = client.get_value(user=wallet)
    positions = client.get_positions(user=wallet, limit=args.positions_limit, offset=0)
    trades = client.get_trades(user=wallet, limit=args.trade_limit, offset=0)
    summary = summarize_trades(trades)

    print(f"Resolved wallet: {wallet} ({resolution})")
    print(f"Current portfolio value: {_format_money(value)}")
    print(f"Open/redeemable positions fetched: {len(positions)}")
    print(f"Recent trades fetched: {summary['count']}")
    print(
        "Recent trade flow: "
        f"BUY {summary['buy_count']} / {summary['sell_count']} SELL | "
        f"Buy notional {_format_money(summary['buy_notional'])} | "
        f"Sell notional {_format_money(summary['sell_notional'])} | "
        f"Net {_format_money(summary['net_notional'])}"
    )
    print(f"Last fetched trade: {format_timestamp(summary['last_trade_at'])}")
    print()
    print("Top positions")
    _print_table(
        _position_rows(positions[: args.show_positions]),
        [
            ("market", "Market"),
            ("outcome", "Outcome"),
            ("size", "Size"),
            ("avg_price", "Avg Price"),
            ("current_value", "Current Value"),
            ("cash_pnl", "Cash PnL"),
            ("end_date", "End Date"),
        ],
        empty_message="No positions returned.",
    )
    print()
    print("Recent trades")
    _print_table(
        _trade_rows(trades[: args.show_trades]),
        [
            ("time", "Time"),
            ("side", "Side"),
            ("market", "Market"),
            ("outcome", "Outcome"),
            ("size", "Size"),
            ("price", "Price"),
            ("notional", "Notional"),
        ],
        empty_message="No trades returned.",
    )
    print()
    print("Note: username resolution is best-effort and only searches recent public trade activity.")
    return 0


def command_trades(args: argparse.Namespace) -> int:
    client = PolymarketClient(timeout=args.timeout)
    wallet, resolution = _resolve_identifier(
        client,
        args.identifier,
        pages=args.search_pages,
        page_size=args.search_page_size,
    )
    trades = _fetch_paged_trades(client, wallet, limit=args.limit, page_size=args.page_size)

    if args.json:
        print(json.dumps(trades, indent=2))
    else:
        print(f"Resolved wallet: {wallet} ({resolution})")
        print(f"Fetched trades: {len(trades)}")
        _print_table(
            _trade_rows(trades),
            [
                ("time", "Time"),
                ("side", "Side"),
                ("market", "Market"),
                ("outcome", "Outcome"),
                ("size", "Size"),
                ("price", "Price"),
                ("notional", "Notional"),
                ("tx", "Transaction Hash"),
            ],
            empty_message="No trades returned.",
        )

    if args.csv:
        output_path = Path(args.csv)
        _write_csv(output_path, trades)
        print(f"\nSaved CSV to {output_path}")

    return 0


def command_search_users(args: argparse.Namespace) -> int:
    client = PolymarketClient(timeout=args.timeout)
    matches = client.search_recent_users(args.query, page_size=args.page_size, pages=args.pages)
    print(
        "Searching recent public trades only. If the user has not traded recently, "
        "you may need their wallet address."
    )
    _print_table(
        [
            {
                "wallet": match["proxyWallet"],
                "name": match["name"] or "-",
                "pseudonym": match["pseudonym"] or "-",
                "matched_on": ", ".join(match["matchedFields"]),
                "exact": "yes" if match["exactMatch"] else "no",
                "last_seen": format_timestamp(match.get("lastSeen")),
            }
            for match in matches[: args.limit]
        ],
        [
            ("wallet", "Wallet"),
            ("name", "Name"),
            ("pseudonym", "Pseudonym"),
            ("matched_on", "Matched On"),
            ("exact", "Exact"),
            ("last_seen", "Last Seen"),
        ],
        empty_message="No recent users matched that query.",
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="polymarket-inspector",
        description="Inspect Polymarket wallets, positions, and historical trades from public endpoints.",
    )
    parser.add_argument("--timeout", type=int, default=20, help="HTTP timeout in seconds.")

    subparsers = parser.add_subparsers(dest="command", required=True)

    summary_parser = subparsers.add_parser("summary", help="Show a wallet summary with positions and recent trades.")
    summary_parser.add_argument("identifier", help="Wallet address or recent exact username/pseudonym match.")
    summary_parser.add_argument("--trade-limit", type=int, default=20, help="Recent trades to fetch for summary stats.")
    summary_parser.add_argument("--positions-limit", type=int, default=10, help="Positions to fetch.")
    summary_parser.add_argument("--show-trades", type=int, default=10, help="Trades to display in the summary table.")
    summary_parser.add_argument("--show-positions", type=int, default=5, help="Positions to display in the summary table.")
    summary_parser.add_argument("--search-pages", type=int, default=3, help="Recent trade pages to scan when resolving usernames.")
    summary_parser.add_argument("--search-page-size", type=int, default=100, help="Records per recent trade page for username search.")
    summary_parser.set_defaults(func=command_summary)

    trades_parser = subparsers.add_parser("trades", help="Fetch historical trades for a wallet.")
    trades_parser.add_argument("identifier", help="Wallet address or recent exact username/pseudonym match.")
    trades_parser.add_argument("--limit", type=int, default=25, help="Maximum number of trades to fetch.")
    trades_parser.add_argument("--page-size", type=int, default=100, help="Page size for API pagination.")
    trades_parser.add_argument("--csv", help="Optional CSV export path.")
    trades_parser.add_argument("--json", action="store_true", help="Print the raw trade list as JSON.")
    trades_parser.add_argument("--search-pages", type=int, default=3, help="Recent trade pages to scan when resolving usernames.")
    trades_parser.add_argument("--search-page-size", type=int, default=100, help="Records per recent trade page for username search.")
    trades_parser.set_defaults(func=command_trades)

    search_parser = subparsers.add_parser("search-users", help="Search recent public trades for usernames or pseudonyms.")
    search_parser.add_argument("query", help="Case-insensitive search string.")
    search_parser.add_argument("--pages", type=int, default=3, help="How many recent trade pages to scan.")
    search_parser.add_argument("--page-size", type=int, default=100, help="Records per page to scan.")
    search_parser.add_argument("--limit", type=int, default=15, help="Maximum matches to display.")
    search_parser.set_defaults(func=command_search_users)

    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    for name in ("trade_limit", "positions_limit", "show_trades", "show_positions", "limit", "page_size", "search_pages", "search_page_size", "pages"):
        value = getattr(args, name, None)
        if value is not None and value <= 0:
            parser.error(f"{name.replace('_', '-')} must be greater than zero")

    try:
        return int(args.func(args))
    except ApiError as exc:
        print(f"Error: {exc}")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

