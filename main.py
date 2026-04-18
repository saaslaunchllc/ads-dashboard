import os
import requests
from datetime import datetime, timezone
from collections import Counter
import pytz

# ── Config from environment variables ─────────────────────────────────────────
AIRTABLE_TOKEN   = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE_ID = os.environ.get("AIRTABLE_BASE_ID", "appHSMWWs3kPEhbYs")
SLACK_TOKEN      = os.environ["SLACK_TOKEN"]
SLACK_CHANNEL_ID = os.environ.get("SLACK_CHANNEL_ID", "C0ATPPQ99T2")
TZ               = pytz.timezone("America/New_York")

# ── Table / field constants ────────────────────────────────────────────────────
LEADS_TABLE    = "tblOwhlWRgmMjsUXT"
CALLS_TABLE    = "tbl1MC2Y0lPC6TVM2"
EOC_TABLE      = "tblyJQjIG030lRGaB"

LEAD_FIELDS = [
    "fldXdM92NA7p9nCrB",  # Name
    "fldsiSb8Ex839c9z7",  # Email
    "fldWqLT9NYzo3I01k",  # UTM Campaign
    "fldYLfHCJWuWQ5E90",  # UTM Source
    "fldq0Cz20MfRBlnhI",  # Booked?
    "fldmq1KKFazWEfYUP",  # Closed?
    "fldAXr9uO38qdgleU",  # Cash Collected
    "fldoFa3xbByG4hLrE",  # Created Date
]
CALL_FIELDS = [
    "fldvqkY6pmRVKHq4s",  # Full Name
    "fldtm2dOO2DAbIvqM",  # Call Status
    "fldU5rZBoO1ofQ9s1",  # Scheduled Date
]
EOC_FIELDS = [
    "fld0BTtBm0EBrK6Bf",  # Name (formula)
    "fldlXS3X8SwnoV7SQ",  # Sales Call Outcome
    "fldBoYnkZXarRlibO",  # Call Date
    "fldQjeOUYZtKZzg4k",  # Form Type
    "fldIwbEIRm9es2G7R",  # Call Type
    "fld9sohlGYtVKFKYP",  # Lead Quality
]

# ── Airtable helpers ───────────────────────────────────────────────────────────
def airtable_get(table_id, params):
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_id}"
    records, offset = [], None
    while True:
        p = dict(params)
        if offset:
            p["offset"] = offset
        r = requests.get(url, headers=headers, params=p)
        r.raise_for_status()
        data = r.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return records


def today_formula():
    """Returns an Airtable filterByFormula string for today in New York time."""
    now_ny = datetime.now(TZ)
    date_str = now_ny.strftime("%Y-%m-%d")
    return f"IS_SAME({{Created Date}}, '{date_str}', 'day')"


def today_call_formula():
    now_ny = datetime.now(TZ)
    date_str = now_ny.strftime("%Y-%m-%d")
    return f"IS_SAME({{Scheduled Date}}, '{date_str}', 'day')"


def today_eoc_formula():
    now_ny = datetime.now(TZ)
    date_str = now_ny.strftime("%Y-%m-%d")
    return f"{{Call Date}} = '{date_str}'"


# ── Data fetching ──────────────────────────────────────────────────────────────
def fetch_leads_today():
    now_ny = datetime.now(TZ)
    date_str = now_ny.strftime("%Y-%m-%d")
    formula = f"IS_SAME({{Created Date}}, '{date_str}', 'day')"
    params = {
        "filterByFormula": formula,
        "fields[]": LEAD_FIELDS,
        "pageSize": 100,
    }
    return airtable_get(LEADS_TABLE, params)


def fetch_calls_today():
    now_ny = datetime.now(TZ)
    date_str = now_ny.strftime("%Y-%m-%d")
    formula = f"IS_SAME({{Scheduled Date}}, '{date_str}', 'day')"
    params = {
        "filterByFormula": formula,
        "fields[]": CALL_FIELDS,
        "pageSize": 100,
    }
    return airtable_get(CALLS_TABLE, params)


def fetch_eoc_today():
    now_ny = datetime.now(TZ)
    date_str = now_ny.strftime("%Y-%m-%d")
    formula = f"{{Call Date}} = '{date_str}'"
    params = {
        "filterByFormula": formula,
        "fields[]": EOC_FIELDS,
        "pageSize": 100,
    }
    return airtable_get(EOC_TABLE, params)


# ── Deduplication ──────────────────────────────────────────────────────────────
def dedup_leads(records):
    """Remove duplicate opt-ins by email (keep first occurrence)."""
    seen_emails, seen_names, unique = set(), set(), []
    for r in records:
        f = r.get("fields", {})
        email = (f.get("fldsiSb8Ex839c9z7") or "").strip().lower()
        name  = (f.get("fldXdM92NA7p9nCrB") or "").strip().lower()
        key   = email if email else name
        if key and key not in seen_emails:
            seen_emails.add(key)
            unique.append(r)
    return unique


# ── Metric computation ─────────────────────────────────────────────────────────
def compute_leads_metrics(records):
    total   = len(records)
    booked  = sum(1 for r in records if r["fields"].get("fldq0Cz20MfRBlnhI") == "Call Booked")
    closed  = sum(1 for r in records if r["fields"].get("fldmq1KKFazWEfYUP") == "Closed")
    revenue = sum(r["fields"].get("fldAXr9uO38qdgleU") or 0 for r in records)
    booking_rate = (booked / total * 100) if total > 0 else 0

    # Per-campaign breakdown (deduplicate bookings by email within campaign)
    camp_data = {}
    booked_emails = set()
    for r in records:
        f       = r["fields"]
        campaign = f.get("fldWqLT9NYzo3I01k") or "Unattributed"
        email    = (f.get("fldsiSb8Ex839c9z7") or "").strip().lower()
        is_booked = f.get("fldq0Cz20MfRBlnhI") == "Call Booked"

        if campaign not in camp_data:
            camp_data[campaign] = {"leads": 0, "booked": 0}
        camp_data[campaign]["leads"] += 1

        # Only count booking once per unique email
        if is_booked and email not in booked_emails:
            camp_data[campaign]["booked"] += 1
            booked_emails.add(email)

    return {
        "total": total,
        "booked": booked,
        "closed": closed,
        "revenue": revenue,
        "booking_rate": booking_rate,
        "campaigns": camp_data,
    }


def compute_calls_metrics(call_records, eoc_records):
    # Cancelled calls from Calls table
    cancelled = []
    for r in call_records:
        f      = r["fields"]
        status = (f.get("fldtm2dOO2DAbIvqM") or {})
        if isinstance(status, dict):
            status = status.get("name", "")
        if status == "Cancelled":
            name     = f.get("fldvqkY6pmRVKHq4s", "Unknown")
            sched_dt = f.get("fldU5rZBoO1ofQ9s1", "")
            # Parse time for display
            try:
                dt  = datetime.fromisoformat(sched_dt.replace("Z", "+00:00"))
                t   = dt.astimezone(TZ).strftime("%-I:%M %p")
            except Exception:
                t = ""
            cancelled.append({"name": name, "time": t})

    # EOC forms breakdown
    cc_forms     = [r for r in eoc_records if (r["fields"].get("fldQjeOUYZtKZzg4k") or {}).get("name") == "Sales Call Outcome Form"]
    triage_forms = [r for r in eoc_records if (r["fields"].get("fldQjeOUYZtKZzg4k") or {}).get("name") == "Triage Outcome Form"]

    # CC outcomes
    def outcome_name(r):
        o = r["fields"].get("fldlXS3X8SwnoV7SQ")
        return (o or {}).get("name", "") if isinstance(o, dict) else (o or "")

    sent_to_cc  = [r for r in cc_forms if outcome_name(r) == "Sent to CC"]
    no_show_ss  = [r for r in cc_forms if outcome_name(r) == "No Show SS"]
    no_show_cc  = [r for r in cc_forms if outcome_name(r) == "No Show CC"]
    deposit     = [r for r in cc_forms if outcome_name(r) == "Deposit"]
    lost        = [r for r in cc_forms if outcome_name(r) == "Lost"]
    closed_cc   = [r for r in cc_forms if outcome_name(r) in ("Deposit", "Won")]

    showed    = len(sent_to_cc) + len(deposit) + len(lost) + len(closed_cc)
    no_shows  = len(no_show_ss) + len(no_show_cc)
    total_sched = showed + no_shows
    show_rate = (showed / total_sched * 100) if total_sched > 0 else 0

    # Lead quality from triage forms
    def lead_quality(r):
        q = r["fields"].get("fld9sohlGYtVKFKYP")
        if isinstance(q, dict):
            name = q.get("name", "")
            # Strip emoji prefix
            return name.split(" ", 1)[-1] if name else "Unknown"
        return str(q) if q else "Unknown"

    quality_counts = Counter(lead_quality(r) for r in triage_forms)

    return {
        "cancelled": cancelled,
        "cc_total": total_sched,
        "showed": showed,
        "no_show_ss": len(no_show_ss),
        "no_show_cc": len(no_show_cc),
        "show_rate": show_rate,
        "sent_to_cc_names": [r["fields"].get("fldkLuVFjv8FzB0sf", [{}])[0].get("name", "?") if r["fields"].get("fldkLuVFjv8FzB0sf") else r["fields"].get("fld0BTtBm0EBrK6Bf", "?") for r in sent_to_cc],
        "no_show_ss_names": [r["fields"].get("fldkLuVFjv8FzB0sf", [{}])[0].get("name", "?") if r["fields"].get("fldkLuVFjv8FzB0sf") else r["fields"].get("fld0BTtBm0EBrK6Bf", "?") for r in no_show_ss],
        "triage_total": len(triage_forms),
        "triage_quality": quality_counts,
        "cash_collected": sum(0 for _ in closed_cc),  # extend if cash field available
    }


# ── Message formatting ─────────────────────────────────────────────────────────
def format_message(leads_m, calls_m):
    now_ny   = datetime.now(TZ)
    date_str = now_ny.strftime("%A, %B %-d, %Y").upper()

    lines = [
        f"*DAILY AD PERFORMANCE — {date_str}*",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "*TODAY'S LEADS*",
        f"New Leads: *{leads_m['total']}*   |   Booked: *{leads_m['booked']}*   |   "
        f"Booking Rate: *{leads_m['booking_rate']:.1f}%*   |   "
        f"Closed: *{leads_m['closed']}*   |   Revenue: *${leads_m['revenue']:,.0f}*",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"*TRIAGE CALLS — {calls_m['triage_total']} TOTAL*",
    ]

    for quality, count in sorted(calls_m["triage_quality"].items(), key=lambda x: -x[1]):
        lines.append(f"  {quality}: *{count}*")

    lines += [
        "",
        f"*CLOSER CALLS — {calls_m['cc_total']} SCHEDULED*",
        f"Showed: *{calls_m['showed']}*   |   No Show: *{calls_m['no_show_ss'] + calls_m['no_show_cc']}*   |   Show Rate: *{calls_m['show_rate']:.0f}%*",
    ]

    if calls_m["sent_to_cc_names"]:
        lines.append(f"Sent to CC: {', '.join(calls_m['sent_to_cc_names'])}")
    if calls_m["no_show_ss_names"]:
        lines.append(f"No Show SS: {', '.join(calls_m['no_show_ss_names'])}")

    lines += [
        "",
        f"*CANCELLED — {len(calls_m['cancelled'])}*",
    ]
    for c in calls_m["cancelled"]:
        lines.append(f"  {c['name']} — {c['time']}")

    lines += [
        f"Closed on Call: *0*   |   Cash Collected: *$0*",
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "*ACTIVE CAMPAIGNS TODAY*",
        "",
    ]

    for camp, data in sorted(leads_m["campaigns"].items(), key=lambda x: -x[1]["leads"]):
        br = (data["booked"] / data["leads"] * 100) if data["leads"] > 0 else 0
        lines.append(f"*{camp}*")
        lines.append(f"Leads: *{data['leads']}*   |   Booked: *{data['booked']}*   |   Booking Rate: *{br:.0f}%*")
        lines.append("")

    lines += [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"_Source: Airtable · EOC Forms · {now_ny.strftime('%b %-d, %Y')}_",
    ]

    return "\n".join(lines)


# ── Slack ──────────────────────────────────────────────────────────────────────
def send_slack(message):
    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {SLACK_TOKEN}",
            "Content-Type": "application/json",
        },
        json={"channel": SLACK_CHANNEL_ID, "text": message},
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Slack error: {data.get('error')}")
    print(f"Sent to Slack: {data['ts']}")


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("Fetching Airtable data...")
    raw_leads  = fetch_leads_today()
    leads      = dedup_leads(raw_leads)
    calls      = fetch_calls_today()
    eoc        = fetch_eoc_today()

    print(f"  Leads today (deduped): {len(leads)} (raw: {len(raw_leads)})")
    print(f"  Calls today: {len(calls)}")
    print(f"  EOC forms today: {len(eoc)}")

    leads_m = compute_leads_metrics(leads)
    calls_m = compute_calls_metrics(calls, eoc)

    message = format_message(leads_m, calls_m)
    print("\n── MESSAGE PREVIEW ──")
    print(message)
    print("────────────────────\n")

    send_slack(message)
    print("Done.")


if __name__ == "__main__":
    main()
