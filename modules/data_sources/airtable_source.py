"""
Airtable data source.

Pulls leads from the Airtable Leads table and spend from the
Daily Ad Reporting table. All field IDs are mapped from the
SaasLaunch base (appHSMWWs3kPEhbYs).

Required env vars:
    AIRTABLE_API_KEY       Personal access token
    AIRTABLE_BASE_ID       Base ID (default: appHSMWWs3kPEhbYs)
    AIRTABLE_LEADS_TABLE   Table ID for leads (default: tblOwhlWRgmMjsUXT)
    AIRTABLE_ADS_TABLE     Table ID for Daily Ad Reporting (default: tblBRt5M6VsFPOyuu)
"""

import logging
import os
import time
from datetime import datetime, timedelta, timezone

import requests

from .base import CampaignRaw, DataSource, LeadRecord, SpendRecord

logger = logging.getLogger(__name__)

# ── Field IDs (SaasLaunch Airtable base) ──────────────────────────────────────
F_LEAD_NAME       = "fldXdM92NA7p9nCrB"
F_LEAD_EMAIL      = "fldsiSb8Ex839c9z7"
F_LEAD_CREATED    = "fldoFa3xbByG4hLrE"
F_UTM_CAMPAIGN    = "fldWqLT9NYzo3I01k"
F_UTM_SOURCE      = "fldYLfHCJWuWQ5E90"
F_UTM_MEDIUM      = "fldQPtRxA4V2cPj19"
F_BOOKED          = "fldq0Cz20MfRBlnhI"    # formula → "Call Booked" | "No Call"
F_CLOSED          = "fldmq1KKFazWEfYUP"    # formula → "New Client" | "No Close"
F_CASH            = "fldAXr9uO38qdgleU"    # formula → cash collected per lead

# Daily Ad Reporting fields
F_AD_CAMPAIGN     = "fldQdeivnLeUQcGXT"    # Name (contains campaign name)
F_AD_SPEND        = "fldHbN72Rbbo7ctjg"    # Spend

BOOKED_VALUE  = "Call Booked"
CLOSED_VALUE  = "New Client"
JUNK_SOURCES  = {"zapier", "slack", "organic", "direct", "(not set)"}


class AirtableSource(DataSource):
    """Pulls live data from Airtable via the REST API."""

    BASE_URL = "https://api.airtable.com/v0"

    def __init__(self):
        self.api_key     = os.environ["AIRTABLE_API_KEY"]
        self.base_id     = os.environ.get("AIRTABLE_BASE_ID", "appHSMWWs3kPEhbYs")
        self.leads_table = os.environ.get("AIRTABLE_LEADS_TABLE", "tblOwhlWRgmMjsUXT")
        self.ads_table   = os.environ.get("AIRTABLE_ADS_TABLE", "tblBRt5M6VsFPOyuu")
        self.session     = requests.Session()
        self.session.headers.update({
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json",
        })

    # ── Low-level pagination helper ───────────────────────────────────────────
    def _list_records(
        self,
        table_id: str,
        fields: list[str],
        max_retries: int = 3,
        page_size: int = 100,
    ) -> list[dict]:
        """Paginate through all records in a table with retry logic."""
        url = f"{self.BASE_URL}/{self.base_id}/{table_id}"
        all_records: list[dict] = []
        offset = None

        while True:
            params: dict = {"pageSize": page_size, "fields[]": fields}
            if offset:
                params["offset"] = offset

            for attempt in range(1, max_retries + 1):
                try:
                    resp = self.session.get(url, params=params, timeout=30)
                    if resp.status_code == 429:           # rate limited
                        wait = int(resp.headers.get("Retry-After", 30))
                        logger.warning("Airtable rate limit – waiting %ds", wait)
                        time.sleep(wait)
                        continue
                    resp.raise_for_status()
                    break
                except requests.RequestException as exc:
                    if attempt == max_retries:
                        raise
                    backoff = 2 ** attempt
                    logger.warning("Airtable request failed (attempt %d/%d): %s – retrying in %ds",
                                   attempt, max_retries, exc, backoff)
                    time.sleep(backoff)

            data = resp.json()
            all_records.extend(data.get("records", []))
            offset = data.get("offset")
            if not offset:
                break
            logger.debug("Fetched %d records so far, continuing pagination…", len(all_records))

        logger.info("Airtable: fetched %d records from %s", len(all_records), table_id)
        return all_records

    # ── Date filter helper ────────────────────────────────────────────────────
    @staticmethod
    def _within_window(iso_str: str, days: int) -> bool:
        """Return True if the ISO datetime string is within the last `days` days."""
        if not iso_str:
            return False
        try:
            dt = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            return dt >= cutoff
        except ValueError:
            return False

    # ── Booked / closed field parsing ─────────────────────────────────────────
    @staticmethod
    def _is_booked(cv: dict) -> bool:
        val = cv.get(F_BOOKED)
        return isinstance(val, str) and val == BOOKED_VALUE

    @staticmethod
    def _is_closed(cv: dict) -> bool:
        val = cv.get(F_CLOSED)
        return isinstance(val, str) and val == CLOSED_VALUE

    @staticmethod
    def _get_cash(cv: dict) -> float:
        val = cv.get(F_CASH, 0)
        if isinstance(val, list):
            return sum(float(v) for v in val if v)
        try:
            return float(val or 0)
        except (TypeError, ValueError):
            return 0.0

    # ── Public interface ──────────────────────────────────────────────────────
    def fetch_leads(self, days: int = 90) -> list[LeadRecord]:
        fields = [F_LEAD_NAME, F_LEAD_EMAIL, F_LEAD_CREATED, F_UTM_CAMPAIGN,
                  F_UTM_SOURCE, F_UTM_MEDIUM, F_BOOKED, F_CLOSED, F_CASH]
        raw = self._list_records(self.leads_table, fields)

        results: list[LeadRecord] = []
        for rec in raw:
            cv = rec.get("cellValuesByFieldId") or rec.get("fields", {})

            created = cv.get(F_LEAD_CREATED) or rec.get("createdTime", "")
            if not self._within_window(created, days):
                continue

            booked  = self._is_booked(cv)
            closed  = self._is_closed(cv)
            cash    = self._get_cash(cv)
            source  = (cv.get(F_UTM_SOURCE) or "").lower().strip()
            campaign = (cv.get(F_UTM_CAMPAIGN) or "").strip()

            # Qualified = booked a call (best proxy without a separate score field)
            qualified = booked

            # Junk = no campaign attribution OR known low-quality source
            junk = not campaign or source in JUNK_SOURCES

            results.append(LeadRecord(
                id            = rec["id"],
                name          = cv.get(F_LEAD_NAME) or "",
                email         = cv.get(F_LEAD_EMAIL) or "",
                campaign      = campaign,
                created_at    = created,
                is_booked     = booked,
                is_qualified  = qualified,
                is_junk       = junk,
                is_closed     = closed,
                cash_collected= cash,
                utm_source    = source,
                utm_medium    = (cv.get(F_UTM_MEDIUM) or "").strip(),
            ))

        logger.info("AirtableSource: %d leads within last %d days", len(results), days)
        return results

    def fetch_spend(self, days: int = 90) -> list[SpendRecord]:
        """
        Pulls spend from the Daily Ad Reporting table.
        If your spend data lives in Meta Ads API instead, use MetaAdsSource
        and set SPEND_SOURCE=meta in your .env.
        """
        fields = [F_AD_CAMPAIGN, F_AD_SPEND]
        try:
            raw = self._list_records(self.ads_table, fields)
        except Exception as exc:
            logger.warning("Could not fetch spend from Airtable Ad Reporting: %s", exc)
            return []

        results: list[SpendRecord] = []
        for rec in raw:
            cv = rec.get("cellValuesByFieldId") or rec.get("fields", {})
            name  = (cv.get(F_AD_CAMPAIGN) or "").strip()
            spend = cv.get(F_AD_SPEND) or 0
            if name:
                try:
                    results.append(SpendRecord(campaign=name, spend=float(spend)))
                except (TypeError, ValueError):
                    pass

        logger.info("AirtableSource: fetched spend for %d ad rows", len(results))
        return results
