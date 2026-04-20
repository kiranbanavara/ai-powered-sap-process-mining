"""Abstract base connector."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from ..event_log import EventLog


class BaseConnector(ABC):
    """Source-agnostic interface. A connector knows how to pull raw data from SAP and
    return a canonical EventLog. Downstream code only talks to the EventLog.

    Each process has its own method because the scope parameters differ
    (sales_orgs for O2C, purchasing_orgs / company_codes for P2P, etc.). The CLI
    picks the right method by process slug.
    """

    name: str = "base"

    @abstractmethod
    def extract_o2c(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        sales_orgs: list[str] | None = None,
    ) -> EventLog:
        """Extract an Order-to-Cash event log for the given window and scope."""
        raise NotImplementedError

    @abstractmethod
    def extract_p2p(
        self,
        start: datetime | None = None,
        end: datetime | None = None,
        purchasing_orgs: list[str] | None = None,
        company_codes: list[str] | None = None,
    ) -> EventLog:
        """Extract a Procure-to-Pay event log for the given window and scope."""
        raise NotImplementedError

    def extract(
        self,
        process_slug: str,
        start: datetime | None = None,
        end: datetime | None = None,
        **scope,
    ) -> EventLog:
        """Dispatch by process slug. The CLI/UI call this; connectors implement the
        per-process methods."""
        if process_slug == "o2c":
            return self.extract_o2c(start=start, end=end, sales_orgs=scope.get("sales_orgs"))
        if process_slug == "p2p":
            return self.extract_p2p(
                start=start,
                end=end,
                purchasing_orgs=scope.get("purchasing_orgs"),
                company_codes=scope.get("company_codes"),
            )
        raise ValueError(f"Unknown process '{process_slug}'. Use 'o2c' or 'p2p'.")
