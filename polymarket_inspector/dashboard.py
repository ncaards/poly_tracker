from __future__ import annotations

from pathlib import Path

from flask import Flask, jsonify, request, send_file

from .analytics import analyze_wallet
from .api import ApiError

app = Flask(__name__)


@app.get("/")
def index():
    index_path = Path(__file__).resolve().parents[1] / "index.html"
    return send_file(index_path)


@app.get("/api/analyze")
def api_analyze():
    wallet = str(request.args.get("wallet") or "").strip()
    # Polymarket public API: offset cap 3000 + max 1000 per page → absolute ceiling 4000.
    # "all" = unbounded (fetch until the API stops returning data or hits its own offset cap).
    raw_limit = str(request.args.get("limit") or "all").strip().lower()
    if raw_limit == "all":
        limit = None
    else:
        try:
            limit = int(raw_limit)
        except (TypeError, ValueError):
            limit = None
        if limit is not None:
            limit = max(10, limit)  # no upper clamp — the API itself enforces 4k at source

    if not wallet:
        return jsonify({"error": "Missing wallet query parameter."}), 400

    try:
        payload = analyze_wallet(wallet, trade_limit=limit)
    except ApiError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:  # pragma: no cover - defensive runtime guard
        return jsonify({"error": f"Unexpected server error: {exc}"}), 500

    return jsonify(payload)

