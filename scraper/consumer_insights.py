#!/usr/bin/env python3
"""
BusinessDen Consumer Insights — GA4 Data Scraper

Pulls article-level analytics from BusinessDen's GA4 property,
Search Console data, and RSS feed categories. Outputs JSON for
the Consumer Insights dashboard.

Part of the BusinessDen Dreck Suite.
"""

import argparse
import json
import logging
import os
import sys
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone, timedelta
from collections import defaultdict

import requests
from google.oauth2 import service_account
from google.auth.transport.requests import Request

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GA4_API_URL = "https://analyticsdata.googleapis.com/v1beta/properties/{property_id}:runReport"
GA4_ROW_LIMIT = 10000
MAX_RETRIES = 3
RETRY_BACKOFF_BASE = 2  # seconds, doubles each retry
RSS_FEED_URL = "https://businessden.com/feed/"
GEO_CITIES_PER_ARTICLE = 50

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
log = logging.getLogger("consumer_insights")

# ---------------------------------------------------------------------------
# Authentication
# ---------------------------------------------------------------------------

def authenticate_ga4():
    """Authenticate with GA4 Data API using service account."""
    key_json = os.environ.get("GA4_SERVICE_ACCOUNT_KEY")
    if not key_json:
        log.error("GA4_SERVICE_ACCOUNT_KEY environment variable not set")
        sys.exit(1)

    try:
        key_data = json.loads(key_json)
    except json.JSONDecodeError as e:
        log.error(f"Failed to parse GA4_SERVICE_ACCOUNT_KEY as JSON: {e}")
        sys.exit(1)

    credentials = service_account.Credentials.from_service_account_info(
        key_data,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )
    credentials.refresh(Request())
    log.info("GA4 authentication successful")
    return credentials


def authenticate_search_console():
    """Authenticate with Search Console API using service account.
    
    Uses the same service account as GA4 — the service account must be
    added as a user in Search Console with at least Restricted access.
    
    Returns None if credentials are not available (stubbed for v1).
    """
    key_json = os.environ.get("GA4_SERVICE_ACCOUNT_KEY")
    if not key_json:
        return None

    try:
        key_data = json.loads(key_json)
    except json.JSONDecodeError:
        return None

    try:
        credentials = service_account.Credentials.from_service_account_info(
            key_data,
            scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
        )
        credentials.refresh(Request())
        log.info("Search Console authentication successful")
        return credentials
    except Exception as e:
        log.warning(f"Search Console auth failed (may not have access yet): {e}")
        return None


# ---------------------------------------------------------------------------
# GA4 Query Engine
# ---------------------------------------------------------------------------

def run_ga4_query(credentials, property_id, query_name, body, dry_run=False):
    """Execute a GA4 Data API query with pagination and retry logic.
    
    Returns list of row dicts, or None on failure.
    """
    url = GA4_API_URL.format(property_id=property_id)
    
    if dry_run:
        log.info(f"[DRY RUN] Query '{query_name}':")
        log.info(json.dumps(body, indent=2))
        return []

    all_rows = []
    offset = 0
    page = 1

    while True:
        body_with_offset = {**body, "offset": offset}
        
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                credentials.refresh(Request())
                headers = {
                    "Authorization": f"Bearer {credentials.token}",
                    "Content-Type": "application/json"
                }
                resp = requests.post(url, headers=headers, json=body_with_offset, timeout=60)
                
                if resp.status_code == 429:
                    wait = RETRY_BACKOFF_BASE ** attempt
                    log.warning(f"Rate limited on '{query_name}', retrying in {wait}s (attempt {attempt}/{MAX_RETRIES})")
                    time.sleep(wait)
                    continue
                
                resp.raise_for_status()
                data = resp.json()
                break
                
            except requests.exceptions.RequestException as e:
                if attempt == MAX_RETRIES:
                    log.error(f"Query '{query_name}' failed after {MAX_RETRIES} attempts: {e}")
                    return None
                wait = RETRY_BACKOFF_BASE ** attempt
                log.warning(f"Query '{query_name}' attempt {attempt} failed: {e}. Retrying in {wait}s")
                time.sleep(wait)
        else:
            log.error(f"Query '{query_name}' exhausted all retries")
            return None

        # Parse rows
        dimension_headers = [h["name"] for h in data.get("dimensionHeaders", [])]
        metric_headers = [h["name"] for h in data.get("metricHeaders", [])]
        
        rows = data.get("rows", [])
        for row in rows:
            parsed = {}
            for i, dim in enumerate(row.get("dimensionValues", [])):
                parsed[dimension_headers[i]] = dim["value"]
            for i, met in enumerate(row.get("metricValues", [])):
                # Try to parse as number
                val = met["value"]
                try:
                    parsed[metric_headers[i]] = int(val)
                except ValueError:
                    try:
                        parsed[metric_headers[i]] = float(val)
                    except ValueError:
                        parsed[metric_headers[i]] = val
            all_rows.append(parsed)

        row_count = data.get("rowCount", 0)
        fetched = offset + len(rows)
        
        log.info(f"Query '{query_name}' page {page}: {len(rows)} rows (total {fetched}/{row_count})")

        if fetched >= row_count or len(rows) == 0:
            break
        
        offset = fetched
        page += 1

    log.info(f"Query '{query_name}' complete: {len(all_rows)} total rows")
    return all_rows


def build_date_range(days_ago):
    """Build GA4 date range for N days ago to today."""
    return {"startDate": f"{days_ago}daysAgo", "endDate": "today"}


# ---------------------------------------------------------------------------
# GA4 Query Definitions
# ---------------------------------------------------------------------------

def get_queries(days=90, sub_days=90, temporal_days=30):
    """Return the 8 GA4 query definitions."""
    
    queries = {
        "traffic_sources": {
            "dateRanges": [build_date_range(days)],
            "dimensions": [
                {"name": "pagePath"},
                {"name": "sessionSource"},
                {"name": "sessionMedium"}
            ],
            "metrics": [
                {"name": "screenPageViews"},
                {"name": "totalUsers"},
                {"name": "sessions"}
            ],
            "orderBys": [{"metric": {"metricName": "screenPageViews"}, "desc": True}],
            "limit": GA4_ROW_LIMIT
        },
        "geographic": {
            "dateRanges": [build_date_range(days)],
            "dimensions": [
                {"name": "pagePath"},
                {"name": "city"},
                {"name": "region"}
            ],
            "metrics": [
                {"name": "screenPageViews"},
                {"name": "totalUsers"}
            ],
            "orderBys": [{"metric": {"metricName": "screenPageViews"}, "desc": True}],
            "limit": GA4_ROW_LIMIT
        },
        "device": {
            "dateRanges": [build_date_range(days)],
            "dimensions": [
                {"name": "pagePath"},
                {"name": "deviceCategory"}
            ],
            "metrics": [
                {"name": "screenPageViews"},
                {"name": "totalUsers"},
                {"name": "sessions"}
            ],
            "orderBys": [{"metric": {"metricName": "screenPageViews"}, "desc": True}],
            "limit": GA4_ROW_LIMIT
        },
        "engagement": {
            "dateRanges": [build_date_range(days)],
            "dimensions": [
                {"name": "pagePath"}
            ],
            "metrics": [
                {"name": "screenPageViews"},
                {"name": "totalUsers"},
                {"name": "sessions"},
                {"name": "engagedSessions"},
                {"name": "bounceRate"},
                {"name": "averageSessionDuration"}
            ],
            "orderBys": [{"metric": {"metricName": "screenPageViews"}, "desc": True}],
            "limit": GA4_ROW_LIMIT
        },
        "subscription_funnel": {
            "dateRanges": [build_date_range(sub_days)],
            "dimensions": [
                {"name": "landingPage"},
                {"name": "sessionSource"},
                {"name": "sessionMedium"},
                {"name": "eventName"}
            ],
            "metrics": [
                {"name": "eventCount"}
            ],
            "dimensionFilter": {
                "filter": {
                    "fieldName": "eventName",
                    "stringFilter": {
                        "value": "subscription",
                        "matchType": "EXACT"
                    }
                }
            },
            "limit": GA4_ROW_LIMIT
        },
        "temporal_patterns": {
            "dateRanges": [build_date_range(temporal_days)],
            "dimensions": [
                {"name": "dayOfWeek"},
                {"name": "hour"}
            ],
            "metrics": [
                {"name": "screenPageViews"},
                {"name": "totalUsers"},
                {"name": "sessions"}
            ],
            "limit": GA4_ROW_LIMIT
        },
        "new_vs_returning": {
            "dateRanges": [build_date_range(days)],
            "dimensions": [
                {"name": "pagePath"},
                {"name": "newVsReturning"}
            ],
            "metrics": [
                {"name": "screenPageViews"},
                {"name": "totalUsers"}
            ],
            "orderBys": [{"metric": {"metricName": "screenPageViews"}, "desc": True}],
            "limit": GA4_ROW_LIMIT
        },
        "daily_time_series": {
            "dateRanges": [build_date_range(days)],
            "dimensions": [
                {"name": "date"}
            ],
            "metrics": [
                {"name": "screenPageViews"},
                {"name": "totalUsers"},
                {"name": "sessions"},
                {"name": "newUsers"}
            ],
            "orderBys": [{"dimension": {"dimensionName": "date"}, "desc": False}],
            "limit": GA4_ROW_LIMIT
        }
    }
    
    return queries


# ---------------------------------------------------------------------------
# Post-Processing
# ---------------------------------------------------------------------------

def cap_geographic_data(geo_rows, max_cities=GEO_CITIES_PER_ARTICLE):
    """Cap geographic data to top N cities per article by pageviews."""
    if not geo_rows:
        return geo_rows
    
    by_article = defaultdict(list)
    for row in geo_rows:
        by_article[row["pagePath"]].append(row)
    
    capped = []
    for path, rows in by_article.items():
        sorted_rows = sorted(rows, key=lambda r: r.get("screenPageViews", 0), reverse=True)
        capped.extend(sorted_rows[:max_cities])
    
    log.info(f"Geographic data capped: {len(geo_rows)} → {len(capped)} rows ({max_cities} cities/article)")
    return capped


# ---------------------------------------------------------------------------
# RSS Feed — Category Extraction
# ---------------------------------------------------------------------------

def fetch_rss_categories():
    """Fetch BusinessDen RSS feed and extract URL → categories mapping.
    
    Returns dict: { "/2026/03/15/article-slug/": ["Category1", "Category2"], ... }
    """
    try:
        resp = requests.get(RSS_FEED_URL, timeout=30)
        resp.raise_for_status()
    except requests.exceptions.RequestException as e:
        log.error(f"Failed to fetch RSS feed: {e}")
        return {}

    try:
        root = ET.fromstring(resp.content)
    except ET.ParseError as e:
        log.error(f"Failed to parse RSS feed XML: {e}")
        return {}

    categories = {}
    
    for item in root.iter("item"):
        link_el = item.find("link")
        if link_el is None or not link_el.text:
            continue
        
        # Extract path from full URL
        link = link_el.text.strip()
        # https://businessden.com/2026/03/15/slug/ → /2026/03/15/slug/
        if "businessden.com" in link:
            path = "/" + link.split("businessden.com/", 1)[-1]
        else:
            path = link
        
        # Ensure trailing slash to match GA4 pagePath format
        if not path.endswith("/"):
            path += "/"
        
        cats = []
        for cat_el in item.findall("category"):
            if cat_el.text:
                cats.append(cat_el.text.strip())
        
        if cats:
            categories[path] = cats
    
    log.info(f"RSS feed: extracted categories for {len(categories)} articles")
    return categories


# ---------------------------------------------------------------------------
# Search Console (Stubbed — pending access)
# ---------------------------------------------------------------------------

SEARCH_CONSOLE_API_URL = "https://www.googleapis.com/webmasters/v3/sites/{site_url}/searchAnalytics/query"
SEARCH_CONSOLE_SITE = "https://businessden.com/"


def fetch_search_console_data(credentials, days=90):
    """Fetch search query data from Google Search Console API.
    
    Returns list of row dicts, or None if unavailable.
    """
    if credentials is None:
        log.warning("Search Console: no credentials available, skipping (pending service account access)")
        return None
    
    url = SEARCH_CONSOLE_API_URL.format(
        site_url=requests.utils.quote(SEARCH_CONSOLE_SITE, safe="")
    )
    
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=days)
    
    body = {
        "startDate": start_date.isoformat(),
        "endDate": end_date.isoformat(),
        "dimensions": ["query", "page"],
        "rowLimit": 5000,
        "startRow": 0
    }
    
    try:
        credentials.refresh(Request())
        headers = {
            "Authorization": f"Bearer {credentials.token}",
            "Content-Type": "application/json"
        }
        resp = requests.post(url, headers=headers, json=body, timeout=60)
        resp.raise_for_status()
        data = resp.json()
    except requests.exceptions.RequestException as e:
        log.warning(f"Search Console query failed: {e}")
        return None
    
    rows = []
    for row in data.get("rows", []):
        keys = row.get("keys", [])
        rows.append({
            "query": keys[0] if len(keys) > 0 else "",
            "page": keys[1] if len(keys) > 1 else "",
            "clicks": row.get("clicks", 0),
            "impressions": row.get("impressions", 0),
            "ctr": round(row.get("ctr", 0), 4),
            "position": round(row.get("position", 0), 1)
        })
    
    log.info(f"Search Console: {len(rows)} rows fetched")
    return rows


# ---------------------------------------------------------------------------
# History File
# ---------------------------------------------------------------------------

def update_history(history_path, results, run_number):
    """Append KPI snapshot to history.json."""
    
    # Load existing history
    if os.path.exists(history_path):
        try:
            with open(history_path, "r") as f:
                history = json.load(f)
        except (json.JSONDecodeError, IOError):
            log.warning(f"Could not read {history_path}, starting fresh")
            history = {"snapshots": []}
    else:
        history = {"snapshots": []}
    
    # Compute KPIs from engagement data (has per-article totals)
    engagement = results.get("engagement") or []
    total_pageviews = sum(r.get("screenPageViews", 0) for r in engagement)
    total_users = sum(r.get("totalUsers", 0) for r in engagement)
    total_sessions = sum(r.get("sessions", 0) for r in engagement)
    article_count = len(engagement)
    
    # New users from daily time series
    daily = results.get("daily_time_series") or []
    new_users = sum(r.get("newUsers", 0) for r in daily)
    
    # Subscriber events from funnel
    funnel = results.get("subscription_funnel") or []
    subscriber_events = sum(r.get("eventCount", 0) for r in funnel)
    
    # Top source from traffic data
    traffic = results.get("traffic_sources") or []
    source_totals = defaultdict(int)
    for r in traffic:
        key = f"{r.get('sessionSource', 'unknown')} / {r.get('sessionMedium', 'unknown')}"
        source_totals[key] += r.get("screenPageViews", 0)
    
    top_source = None
    if source_totals and total_pageviews > 0:
        top_key = max(source_totals, key=source_totals.get)
        parts = top_key.split(" / ")
        top_source = {
            "source": parts[0],
            "medium": parts[1] if len(parts) > 1 else "unknown",
            "pct": round(source_totals[top_key] / total_pageviews, 4)
        }
    
    # Top article
    top_article = None
    if engagement:
        top = max(engagement, key=lambda r: r.get("screenPageViews", 0))
        top_article = {
            "path": top.get("pagePath", ""),
            "pageviews": top.get("screenPageViews", 0)
        }
    
    snapshot = {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "run_number": run_number,
        "total_pageviews": total_pageviews,
        "total_users": total_users,
        "total_sessions": total_sessions,
        "new_users": new_users,
        "subscriber_events": subscriber_events,
        "top_source": top_source,
        "top_article": top_article,
        "article_count": article_count
    }
    
    history["snapshots"].append(snapshot)
    
    with open(history_path, "w") as f:
        json.dump(history, f, indent=2)
    
    log.info(f"History updated: {len(history['snapshots'])} total snapshots")


# ---------------------------------------------------------------------------
# Run Number
# ---------------------------------------------------------------------------

def get_next_run_number(history_path):
    """Determine next run number from history file."""
    if os.path.exists(history_path):
        try:
            with open(history_path, "r") as f:
                history = json.load(f)
            snapshots = history.get("snapshots", [])
            if snapshots:
                return max(s.get("run_number", 0) for s in snapshots) + 1
        except (json.JSONDecodeError, IOError):
            pass
    return 1


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="BusinessDen Consumer Insights GA4 Scraper")
    parser.add_argument("--days", type=int, default=90, help="Lookback days for standard queries (default: 90)")
    parser.add_argument("--sub-days", type=int, default=90, help="Lookback days for subscription queries (default: 90)")
    parser.add_argument("--temporal-days", type=int, default=30, help="Lookback days for temporal patterns (default: 30)")
    parser.add_argument("--dry-run", action="store_true", help="Print query payloads without executing")
    parser.add_argument("--output", type=str, default="data/consumer_insights.json", help="Output JSON path")
    args = parser.parse_args()

    property_id = os.environ.get("GA4_PROPERTY_ID", "363209481")
    output_dir = os.path.dirname(args.output) or "data"
    history_path = os.path.join(output_dir, "history.json")
    
    os.makedirs(output_dir, exist_ok=True)

    run_number = get_next_run_number(history_path)
    log.info(f"=== Consumer Insights Scraper — Run #{run_number} ===")
    log.info(f"Property ID: {property_id}")
    log.info(f"Lookback: {args.days}d standard, {args.sub_days}d subscriptions, {args.temporal_days}d temporal")

    # --- Authenticate ---
    credentials = authenticate_ga4()
    sc_credentials = authenticate_search_console()

    # --- Build queries ---
    queries = get_queries(
        days=args.days,
        sub_days=args.sub_days,
        temporal_days=args.temporal_days
    )

    # --- Run GA4 queries ---
    results = {}
    failed = []
    
    for name, body in queries.items():
        result = run_ga4_query(credentials, property_id, name, body, dry_run=args.dry_run)
        if result is None:
            failed.append(name)
        results[name] = result

    # --- Post-process geographic data ---
    if results.get("geographic") is not None:
        results["geographic"] = cap_geographic_data(results["geographic"])

    # --- Fetch Search Console data ---
    log.info("--- Search Console ---")
    results["search_queries"] = fetch_search_console_data(sc_credentials, days=args.days)

    # --- Fetch RSS categories ---
    log.info("--- RSS Categories ---")
    categories = fetch_rss_categories()
    results["categories"] = [
        {"path": path, "categories": cats}
        for path, cats in categories.items()
    ]

    # --- Build output JSON ---
    now = datetime.now(timezone.utc)
    end_date = now.strftime("%Y-%m-%d")
    standard_start = (now - timedelta(days=args.days)).strftime("%Y-%m-%d")
    sub_start = (now - timedelta(days=args.sub_days)).strftime("%Y-%m-%d")
    temporal_start = (now - timedelta(days=args.temporal_days)).strftime("%Y-%m-%d")

    output = {
        "meta": {
            "generated_at": now.isoformat(),
            "run_number": run_number,
            "property_id": property_id,
            "date_ranges": {
                "standard": {"start": standard_start, "end": end_date},
                "subscription": {"start": sub_start, "end": end_date},
                "temporal": {"start": temporal_start, "end": end_date}
            },
            "failed_queries": failed
        },
        "traffic_sources": results.get("traffic_sources"),
        "geographic": results.get("geographic"),
        "device": results.get("device"),
        "engagement": results.get("engagement"),
        "subscription_funnel": results.get("subscription_funnel"),
        "temporal_patterns": results.get("temporal_patterns"),
        "new_vs_returning": results.get("new_vs_returning"),
        "daily_time_series": results.get("daily_time_series"),
        "search_queries": results.get("search_queries"),
        "categories": results.get("categories")
    }

    if not args.dry_run:
        with open(args.output, "w") as f:
            json.dump(output, f, indent=2)
        log.info(f"Output written to {args.output}")

        # --- Update history ---
        update_history(history_path, results, run_number)

    # --- Summary ---
    log.info("=== Summary ===")
    for key in ["traffic_sources", "geographic", "device", "engagement",
                "subscription_funnel", "temporal_patterns", "new_vs_returning",
                "daily_time_series", "search_queries", "categories"]:
        val = results.get(key)
        if val is None:
            log.info(f"  {key}: FAILED / SKIPPED")
        else:
            log.info(f"  {key}: {len(val)} rows")
    
    if failed:
        log.warning(f"Failed queries: {', '.join(failed)}")
        sys.exit(1 if len(failed) == len(queries) else 0)
    
    log.info("Done.")


if __name__ == "__main__":
    main()
