"""
Base abstract data source.
All data sources must implement this interface so the pipeline
can swap between Airtable, Meta Ads, Google Ads, or mock data
without changing any other module.
"""
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Optional


@dataclass
class LeadRecord:
    """Normalised representation of a single lead across all sources."""
    id: str
    name: str
    email: str
    campaign: str                     # UTM campaign / ad set name
    created_at: str                   # ISO 8601 string
    is_booked: bool = False           # Booked a call
    is_qualified: bool = False        # Passed qualification criteria
    is_junk: bool = False             # Flagged as junk / disqualified
    is_closed: bool = False           # Became a paying client
    cash_collected: float = 0.0       # Revenue attributed to this lead
    utm_source: str = ""
    utm_medium: str = ""


@dataclass
class SpendRecord:
    """Ad spend data for a campaign over the reporting window."""
    campaign: str
    spend: float = 0.0                # Total spend in USD


@dataclass
class CampaignRaw:
    """Raw combined data for one campaign before metrics are computed."""
    name: str
    spend: float = 0.0
    leads: list = field(default_factory=list)   # List[LeadRecord]


class DataSource(ABC):
    """
    Abstract base class for all data sources.
    Implement `fetch_leads` and `fetch_spend` to plug in a new source.
    """

    @abstractmethod
    def fetch_leads(self, days: int = 90) -> list[LeadRecord]:
        """
        Return all leads created within the last `days` days.
        Must return a list of LeadRecord objects.
        """
        ...

    @abstractmethod
    def fetch_spend(self, days: int = 90) -> list[SpendRecord]:
        """
        Return ad spend per campaign for the last `days` days.
        Must return a list of SpendRecord objects.
        If spend data is not available from this source, return [].
        """
        ...

    def fetch_all(self, days: int = 90) -> list[CampaignRaw]:
        """
        Merges leads and spend into CampaignRaw objects keyed by campaign name.
        Override this only if you need custom merge logic.
        """
        leads = self.fetch_leads(days)
        spend_records = self.fetch_spend(days)

        spend_by_campaign: dict[str, float] = {}
        for s in spend_records:
            spend_by_campaign[s.campaign] = spend_by_campaign.get(s.campaign, 0.0) + s.spend

        campaigns: dict[str, CampaignRaw] = {}
        for lead in leads:
            camp = lead.campaign or "(Unattributed)"
            if camp not in campaigns:
                campaigns[camp] = CampaignRaw(
                    name=camp,
                    spend=spend_by_campaign.get(camp, 0.0),
                    leads=[]
                )
            campaigns[camp].leads.append(lead)

        # Include campaigns that have spend but no leads
        for camp, spend in spend_by_campaign.items():
            if camp not in campaigns:
                campaigns[camp] = CampaignRaw(name=camp, spend=spend, leads=[])

        return list(campaigns.values())
