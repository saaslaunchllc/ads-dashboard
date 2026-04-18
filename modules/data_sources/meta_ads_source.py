"""
Meta Ads API data source.

Pulls campaign spend + impressions from the Facebook Marketing API.
Leads are still pulled from Airtable (or another CRM) because Meta
doesn't track post-form conversion quality.

Combine with AirtableSource to get full picture:
    - Spend  → MetaAdsSource
    - Leads  → AirtableSource

Required env vars:
    META_ACCESS_TOKEN      Long-lived system user token
    META_AD_ACCOUNT_ID     e.g. act_123456789
    META_API_VERSION       e.g. v19.0 (default)
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone

import requests

from .base import DataSource, LeadRecord, SpendRecord

logger = logging.getLogger(__name__)


class MetaAdsSource(DataSource):
    """
    Fetches campaign spend data from the Meta (Facebook) Marketing API.

    NOTE: This source only provides spend data. It does NOT return leads —
    lead quality data must come from your CRM (Airtable, GHL, etc.).
    Set LEAD_SOURCE=airtable in your .env and use a CompositeSource.
    """

    API_BASE = "https://graph.facebook.com"

    def __init__(self):
        self.access_token  = os.environ["META_ACCESS_TOKEN"]
        self.ad_account_id = os.environ["META_AD_ACCOUNT_ID"]       # act_XXXXXXXXX
        self.api_version   = os.environ.get("META_API_VERSION", "v19.0")
        self.session       = requests.Session()

    def _get(self, endpoint: str, params: dict, max_retries: int = 3) -> dict:
        """GET request with exponential backoff retry."""
        url = f"{self.API_BASE}/{self.api_version}/{endpoint}"
        params["access_token"] = self.access_token

        for attempt in range(1, max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=30)

                # Meta rate limiting
                if resp.status_code in (429, 503):
                    wait = int(resp.headers.get("Retry-After", 60))
                    logger.warning("Meta API rate limited – waiting %ds", wait)
                    time.sleep(wait)
                    continue

                resp.raise_for_status()
                return resp.json()

            except requests.RequestException as exc:
                if attempt == max_retries:
                    logger.error("Meta API request failed after %d attempts: %s", max_retries, exc)
                    raise
                backoff = 2 ** attempt
                logger.warning("Meta API attempt %d/%d failed: %s – retrying in %ds",
                               attempt, max_retries, exc, backoff)
                time.sleep(backoff)

        return {}

    def _date_range(self, days: int) -> dict:
        """Build a Meta date_preset-compatible since/until date range."""
        now   = datetime.now(timezone.utc)
        since = (now - timedelta(days=days)).strftime("%Y-%m-%d")
        until = now.strftime("%Y-%m-%d")
        return {"since": since, "until": until}

    def fetch_spend(self, days: int = 90) -> list[SpendRecord]:
        """Pull spend per campaign from Meta Ads Insights API."""
        date_range = self._date_range(days)
        params = {
            "level": "campaign",
            "fields": "campaign_name,spend",
            "time_range": f'{{"since":"{date_range["since"]}","until":"{date_range["until"]}"}}',
            "limit": 500,
        }

        results: list[SpendRecord] = []
        try:
            data = self._get(f"{self.ad_account_id}/insights", params)
            for row in data.get("data", []):
                name  = (row.get("campaign_name") or "").strip()
                spend = float(row.get("spend", 0) or 0)
                if name:
                    results.append(SpendRecord(campaign=name, spend=spend))

            # Handle pagination
            while data.get("paging", {}).get("next"):
                data = self._get("", {"after": data["paging"]["cursors"]["after"],
                                      **params})
                for row in data.get("data", []):
                    name  = (row.get("campaign_name") or "").strip()
                    spend = float(row.get("spend", 0) or 0)
                    if name:
                        results.append(SpendRecord(campaign=name, spend=spend))

        except Exception as exc:
            logger.error("MetaAdsSource.fetch_spend failed: %s", exc)

        logger.info("MetaAdsSource: fetched spend for %d campaigns", len(results))
        return results

    def fetch_leads(self, days: int = 90) -> list[LeadRecord]:
        """
        Meta Ads API does not provide lead quality/booking data.
        Return empty list — use AirtableSource (or another CRM) for leads.
        """
        logger.info("MetaAdsSource: leads not available from Meta API – use AirtableSource")
        return []


class CompositeSource(DataSource):
    """
    Combines a lead source (Airtable/CRM) with a spend source (Meta Ads).
    This is the recommended setup for production:

        source = CompositeSource(
            lead_source=AirtableSource(),
            spend_source=MetaAdsSource(),
        )

    The composite merges spend by matching campaign names from Meta
    against UTM campaign values in Airtable.
    """

    def __init__(self, lead_source: DataSource, spend_source: DataSource):
        self.lead_source  = lead_source
        self.spend_source = spend_source

    def fetch_leads(self, days: int = 90) -> list[LeadRecord]:
        return self.lead_source.fetch_leads(days)

    def fetch_spend(self, days: int = 90) -> list[SpendRecord]:
        return self.spend_source.fetch_spend(days)
