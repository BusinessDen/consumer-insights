# Consumer Insights

BusinessDen audience analytics dashboard. Part of the Dreck Suite.

Pulls article-level analytics from GA4, Google Search Console, and the BusinessDen RSS feed. Outputs a single-page dashboard showing traffic sources, geographic reach, device breakdown, engagement metrics, subscription funnel, and SEO data.

## Setup

### Secrets

Add these to the repo's GitHub Actions secrets:

- `GA4_SERVICE_ACCOUNT_KEY` — Full JSON contents of the service account key file (same as Reputation tool)
- `GA4_PROPERTY_ID` — `363209481`

### Search Console (pending)

The service account (`reputation-ga4@bizden-restaurant-tracker.iam.gserviceaccount.com`) needs to be added as a user in Google Search Console with at least Restricted access. Until then, Search Console data will be skipped.

## Running locally

```bash
export GA4_SERVICE_ACCOUNT_KEY='{ ... }'
export GA4_PROPERTY_ID='363209481'
pip install -r requirements.txt
python scraper/consumer_insights.py
```

### Flags

| Flag | Default | Purpose |
|---|---|---|
| `--days N` | 90 | Lookback for standard queries |
| `--sub-days N` | 90 | Lookback for subscription queries |
| `--temporal-days N` | 30 | Lookback for temporal patterns |
| `--dry-run` | — | Print queries without executing |
| `--output PATH` | `data/consumer_insights.json` | Output path |

## Schedule

Runs daily at 5:00 AM MT via GitHub Actions. Can also be triggered manually.

## Data Files

- `data/consumer_insights.json` — Full latest dataset (overwritten each run)
- `data/history.json` — Append-only KPI snapshots for long-term trends
