"""
Mock data source for local testing.

Returns realistic-looking synthetic data so you can test the full
pipeline — metrics, formatting, Slack output — without any API keys.

Usage:
    LEAD_SOURCE=mock python main.py --run-now
"""

import logging
from datetime import datetime, timedelta, timezone
from .base import DataSource, LeadRecord, SpendRecord

logger = logging.getLogger(__name__)

_MOCK_CAMPAIGNS = [
    {
        "name": "Alexia | Winning Ad Iteration W",
        "spend": 2425.93,
        "leads": [
            ("Jeff Hart",      "jeff@example.com",   True,  True,  False, 4995.0),
            ("Amy Chen",       "amy@example.com",    True,  True,  True,  4995.0),
            ("Mark Rivers",    "mark@example.com",   True,  False, False, 0.0),
            ("Laura Simms",    "laura@example.com",  False, False, False, 0.0),
            ("Dan Kowalski",   "dan@example.com",    False, False, True,  0.0),
            ("Priya Nair",     "priya@example.com",  True,  True,  False, 4995.0),
            ("Yusuf Osman",    "yusuf@example.com",  False, False, True,  0.0),
            ("Carla Diaz",     "carla@example.com",  True,  True,  False, 0.0),
            ("Tom Hanley",     "tom@example.com",    False, False, False, 0.0),
        ],
    },
    {
        "name": "Campaign 1 Hubspot/enterprise/1-3m in arr angle",
        "spend": 3225.65,
        "leads": [
            ("Sara T",         "sara@example.com",   True,  True,  False, 4995.0),
            ("Leon M",         "leon@example.com",   True,  False, False, 0.0),
            ("Grace W",        "grace@example.com",  True,  True,  True,  4995.0),
            ("Victor P",       "victor@example.com", False, False, True,  0.0),
            ("Nina B",         "nina@example.com",   False, False, True,  0.0),
            ("James R",        "james@example.com",  True,  True,  False, 0.0),
            ("Olivia S",       "olivia@example.com", True,  False, False, 0.0),
            ("Ravi K",         "ravi@example.com",   False, False, True,  0.0),
            ("Hannah J",       "hannah@example.com", True,  True,  True,  4995.0),
            ("Max T",          "max@example.com",    False, False, False, 0.0),
        ],
    },
    {
        "name": "SCALING 2 C | Ai ugc/static ads 11/22/25",
        "spend": 1442.58,
        "leads": [
            ("Ben Fox",        "ben@example.com",    True,  True,  False, 4995.0),
            ("Steph Cole",     "steph@example.com",  False, False, True,  0.0),
            ("Carlos V",       "carlos@example.com", True,  False, False, 0.0),
            ("Diana W",        "diana@example.com",  False, False, True,  0.0),
            ("Eric H",         "eric@example.com",   True,  True,  False, 4995.0),
        ],
    },
    {
        "name": "ABO | A+ | IG | semi qualifying Form | Goal - Targeting -",
        "spend": 1371.14,
        "leads": [
            ("Troy K",         "troy@example.com",   True,  True,  False, 0.0),
            ("Husam K",        "husam@example.com",  True,  False, False, 0.0),
            ("Chris M",        "chris@example.com",  True,  True,  True,  6000.0),
            ("Jeff D",         "jeffd@example.com",  True,  False, False, 0.0),
            ("Travis S",       "travis@example.com", True,  False, False, 0.0),
            ("Rohan S",        "rohan@example.com",  True,  False, False, 0.0),
            ("Anton F",        "antonf@example.com", False, False, False, 0.0),
            ("Mark S",         "marks@example.com",  False, False, True,  0.0),
            ("Harold E",       "harold@example.com", False, False, True,  0.0),
            ("Bill R",         "billr@example.com",  False, False, False, 0.0),
        ],
    },
]


class MockSource(DataSource):
    """Returns synthetic data for testing. No API keys required."""

    def fetch_leads(self, days: int = 90) -> list[LeadRecord]:
        now = datetime.now(timezone.utc)
        results: list[LeadRecord] = []
        idx = 0
        for camp_data in _MOCK_CAMPAIGNS:
            for i, (name, email, booked, qualified, junk, cash) in enumerate(camp_data["leads"]):
                created = now - timedelta(days=(idx % days))
                results.append(LeadRecord(
                    id            = f"mock_{idx:04d}",
                    name          = name,
                    email         = email,
                    campaign      = camp_data["name"],
                    created_at    = created.isoformat(),
                    is_booked     = booked,
                    is_qualified  = qualified,
                    is_junk       = junk,
                    is_closed     = cash > 0,
                    cash_collected= cash,
                    utm_source    = "facebook",
                    utm_medium    = "cpc",
                ))
                idx += 1
        logger.info("MockSource: generated %d leads", len(results))
        return results

    def fetch_spend(self, days: int = 90) -> list[SpendRecord]:
        results = [
            SpendRecord(campaign=c["name"], spend=c["spend"])
            for c in _MOCK_CAMPAIGNS
        ]
        logger.info("MockSource: generated spend for %d campaigns", len(results))
        return results
