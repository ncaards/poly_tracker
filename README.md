# Polymarket inspector

This repo now contains two things:

1. a **Python CLI** for quick wallet and trade inspection
2. a **simple Flask web dashboard** that is easy to deploy to **Vercel**

The dashboard fetches all API-available trades for a wallet. **Important:** Polymarket's public trade endpoint enforces an offset hard-cap, meaning the maximum retrievable via this API is ~4,000 trades per wallet regardless of how much history exists on-chain. The dashboard fetches all of those automatically when "All available" is selected. Metrics include:

- what categories the wallet usually trades
- odds distribution split in **two charts**:
  - buy-side odds buckets (Yes/No/Other)
  - sell-side odds buckets (Yes/No/Other)
- estimated realized / settled PnL over time
- estimated open unrealized PnL
- PnL over the last 7 days and YTD
- resolved-market hit rate
- hold-to-close behavior
- estimated Sharpe ratio
- top markets and open positions
- full operation log (every fetched buy/sell with price, size, notional, estimated PnL, hit/miss)
- one-click CSV download for all fetched operations in the dashboard

## Web dashboard quick start

Install the Python dependency and run the local server:

```powershell
python -m pip install -r requirements.txt
python app.py
```

Then open:

```text
http://127.0.0.1:8000
```

Root `index.html` is now the main dashboard page used both locally (served by Flask) and on Vercel.

The page calls:

```text
/api/analyze?wallet=0x...&limit=10000
```

You can also request all API-reachable history:

```text
/api/analyze?wallet=0x...&limit=all
```

## Deploy to Vercel

This repo includes:

- `api/index.py` as the Vercel Python entrypoint
- `vercel.json` routing everything to the Flask app
- `requirements.txt` for Flask

Typical deploy flow:

```powershell
vercel
vercel --prod
```

If you deploy from the Vercel dashboard, import the repo and keep the root as this folder.

## Analytics definitions and caveats

This is a useful **first version**, but some metrics are necessarily estimates because public Polymarket endpoints do not expose a full historical account equity curve.

### Category mix

Pulled from Polymarket event tags when available. If tags are missing, the app falls back to simple title-based heuristics.

### Odds distribution

Shown as two stacked charts (buy and sell), each grouped into buckets like `0.00-0.05`, `0.05-0.10`, `0.10-0.20`, etc., and split by inferred Yes/No/Other outcome classes.

### Operation log

Each fetched operation row includes side, outcome, price, size, notional, estimated PnL, hit/miss/open state, and whether that buy was held to close.

### PnL timeline

The chart shows **estimated realized / settled PnL** only:

- realized PnL from matched sells using FIFO cost basis
- settled PnL for lots that were still open when a market resolved

Open-position unrealized PnL is shown separately in the summary cards.

### Last 7 days / YTD PnL

These are computed from the same **realized / settled** timeline above.

### Hit rate

Reported as the percentage of **resolved markets in the fetched sample** that ended with positive total PnL, where enough trade history existed to build cost basis.

### Hold-to-close

Among resolved markets in the fetched sample, this measures how often the wallet still had inventory when the market resolved.

### Sharpe ratio

This is an **estimate**, annualized on **daily realized / settled returns** using a zero risk-free rate and current cost basis at risk as the denominator.

It is informative, but it is not a perfect institutional-grade mark-to-market Sharpe.

### Most important limitation

If the wallet has more history than the last 10,000 public trades, then:

- some sells may not have matching buys in the fetched window
- realized PnL can be understated
- hit rate / hold-to-close only apply to the visible sample

The app surfaces these caveats directly in the UI.

## CLI quick start

From the project root:

```powershell
python main.py --help
python main.py summary 0xe9fa488d214b3f73fdc81b7587a06991330af4c6
python main.py trades 0xe9fa488d214b3f73fdc81b7587a06991330af4c6 --limit 50 --csv trades.csv
python main.py search-users prepared
```

### Summary

```powershell
python main.py summary <wallet-or-recent-username>
```

### Trades

```powershell
python main.py trades <wallet-or-recent-username> --limit 100 --csv trades.csv
```

### Search recent users

```powershell
python main.py search-users <query>
```

## Testing

```powershell
python -m unittest discover -s tests -v
python -m compileall main.py app.py api polymarket_inspector tests
```

