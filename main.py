import os
import requests
from datetime import datetime, timedelta, time as dtime
from collections import Counter
import pytz

# ── Config ─────────────────────────────────────────────────────────────────────
AIRTABLE_TOKEN    = os.environ["AIRTABLE_TOKEN"]
AIRTABLE_BASE_ID  = os.environ.get("AIRTABLE_BASE_ID", "appHSMWWs3kPEhbYs")
SLACK_TOKEN       = os.environ["SLACK_TOKEN"]
SLACK_CHANNEL_ID  = os.environ.get("SLACK_CHANNEL_ID", "C0ATPPQ99T2")
META_TOKEN        = os.environ["META_TOKEN"]
META_AD_ACCOUNT   = os.environ.get("META_AD_ACCOUNT", "act_2489671261377664")
TZ                = pytz.timezone("America/New_York")

# ── Table / field IDs ──────────────────────────────────────────────────────────
LEADS_TABLE = "tblOwhlWRgmMjsUXT"
CALLS_TABLE = "tbl1MC2Y0lPC6TVM2"
EOC_TABLE   = "tblyJQjIG030lRGaB"

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
    "fldkLuVFjv8FzB0sf",  # Calls link
    "fld9sohlGYtVKFKYP",  # Lead Quality
]


# ── Airtable ───────────────────────────────────────────────────────────────────
def airtable_get(table_id, params):
    headers = {"Authorization": f"Bearer {AIRTABLE_TOKEN}"}
    url = f"https://api.airtable.com/v0/{AIRTABLE_BASE_ID}/{table_id}"
    records, offset = [], None
    while True:
        p = list(params) if isinstance(params, list) else list(params.items())
        if offset:
            p.append(("offset", offset))
        r = requests.get(url, headers=headers, params=p, timeout=30)
        r.raise_for_status()
        data = r.json()
        records.extend(data.get("records", []))
        offset = data.get("offset")
        if not offset:
            break
    return records


def today_utc_range():
    """Returns (start_utc, end_utc) strings covering today in New York time."""
    now_ny    = datetime.now(TZ)
    today     = now_ny.date()
    tomorrow  = today + timedelta(days=1)
    start_utc = TZ.localize(datetime.combine(today,    dtime.min)).astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    end_utc   = TZ.localize(datetime.combine(tomorrow, dtime.min)).astimezone(pytz.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")
    return start_utc, end_utc


def fetch_leads_today():
    params = [
        ("filterByFormula", "IS_SAME(CREATED_TIME(), TODAY(), 'day')"),
        ("pageSize", 100),
    ]
    for f in LEAD_FIELDS:
        params.append(("fields[]", f))
    return airtable_get(LEADS_TABLE, params)


def fetch_calls_today():
    params = [
        ("filterByFormula", "IS_SAME({Scheduled Date}, TODAY(), 'day')"),
        ("pageSize", 100),
    ]
    for f in CALL_FIELDS:
        params.append(("fields[]", f))
    return airtable_get(CALLS_TABLE, params)


def fetch_eoc_today():
    now_ny   = datetime.now(TZ)
    date_str = now_ny.strftime("%Y-%m-%d")
    params = [
        ("filterByFormula", f"{{Call Date}} = '{date_str}'"),
        ("pageSize", 100),
    ]
    for f in EOC_FIELDS:
        params.append(("fields[]", f))
    return airtable_get(EOC_TABLE, params)


# ── Meta Ads ───────────────────────────────────────────────────────────────────
def fetch_meta_spend_today():
    """Returns {campaign_name: spend_float} for today."""
    url = f"https://graph.facebook.com/v19.0/{META_AD_ACCOUNT}/insights"
    params = {
        "access_token": META_TOKEN,
        "fields":        "campaign_name,spend,impressions,clicks,reach",
        "date_preset":   "today",
        "level":         "campaign",
        "limit":         500,
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    spend_by_campaign = {}
    for row in data.get("data", []):
        name  = row.get("campaign_name", "Unknown")
        spend = float(row.get("spend", 0))
        spend_by_campaign[name] = spend_by_campaign.get(name, 0) + spend

    return spend_by_campaign


# ── Deduplication ──────────────────────────────────────────────────────────────
def dedup_leads(records):
    seen, unique = set(), []
    for r in records:
        f     = r.get("fields", {})
        email = (f.get("fldsiSb8Ex839c9z7") or "").strip().lower()
        name  = (f.get("fldXdM92NA7p9nCrB") or "").strip().lower()
        key   = email if email else name
        if key and key not in seen:
            seen.add(key)
            unique.append(r)
    return unique


# ── Metrics ────────────────────────────────────────────────────────────────────
def compute_leads_metrics(records):
    total        = len(records)
    booked       = sum(1 for r in records if r["fields"].get("fldq0Cz20MfRBlnhI") == "Call Booked")
    closed       = sum(1 for r in records if r["fields"].get("fldmq1KKFazWEfYUP") == "Closed")
    revenue      = sum(r["fields"].get("fldAXr9uO38qdgleU") or 0 for r in records)
    booking_rate = (booked / total * 100) if total > 0 else 0

    camp_data, booked_emails = {}, set()
    for r in records:
        f        = r["fields"]
        campaign = f.get("fldWqLT9NYzo3I01k") or "Unattributed"
        email    = (f.get("fldsiSb8Ex839c9z7") or "").strip().lower()
        is_booked = f.get("fldq0Cz20MfRBlnhI") == "Call Booked"

        if campaign not in camp_data:
            camp_data[campaign] = {"leads": 0, "booked": 0}
        camp_data[campaign]["leads"] += 1

        if is_booked and email not in booked_emails:
            camp_data[campaign]["booked"] += 1
            booked_emails.add(email)

    return {
        "total": total, "booked": booked, "closed": closed,
        "revenue": revenue, "booking_rate": booking_rate,
        "campaigns": camp_data,
    }


def compute_calls_metrics(call_records, eoc_records):
    # Cancelled calls
    cancelled = []
    for r in call_records:
        f      = r["fields"]
        status = f.get("fldtm2dOO2DAbIvqM") or {}
        if isinstance(status, dict):
            status = status.get("name", "")
        if status == "Cancelled":
            name = f.get("fldvqkY6pmRVKHq4s", "Unknown")
            sched = f.get("fldU5rZBoO1ofQ9s1", "")
            try:
                dt = datetime.fromisoformat(sched.replace("Z", "+00:00"))
                t  = dt.astimezone(TZ).strftime("%-I:%M %p")
            except Exception:
                t = ""
            cancelled.append({"name": name, "time": t})

    cc_forms     = [r for r in eoc_records if (r["fields"].get("fldQjeOUYZtKZzg4k") or {}).get("name") == "Sales Call Outcome Form"]
    triage_forms = [r for r in eoc_records if (r["fields"].get("fldQjeOUYZtKZzg4k") or {}).get("name") == "Triage Outcome Form"]

    def outcome(r):
        o = r["fields"].get("fldlXS3X8SwnoV7SQ")
        return (o or {}).get("name", "") if isinstance(o, dict) else (o or "")

    def person_name(r):
        links = r["fields"].get("fldkLuVFjv8FzB0sf")
        if links and isinstance(links, list):
            return links[0].get("name", "?")
        return r["fields"].get("fld0BTtBm0EBrK6Bf", "?")

    sent_to_cc = [r for r in cc_forms if outcome(r) == "Sent to CC"]
    no_show_ss = [r for r in cc_forms if outcome(r) == "No Show SS"]
    no_show_cc = [r for r in cc_forms if outcome(r) == "No Show CC"]
    deposit    = [r for r in cc_forms if outcome(r) == "Deposit"]
    lost       = [r for r in cc_forms if outcome(r) == "Lost"]

    showed      = len(sent_to_cc) + len(deposit) + len(lost)
    no_shows    = len(no_show_ss) + len(no_show_cc)
    total_sched = showed + no_shows
    show_rate   = (showed / total_sched * 100) if total_sched > 0 else 0

    def quality_label(r):
        q = r["fields"].get("fld9sohlGYtVKFKYP")
        if isinstance(q, dict):
            name = q.get("name", "")
            parts = name.split(" ", 1)
            return parts[-1] if len(parts) > 1 else name
        return str(q) if q else "Unknown"

    quality_counts = Counter(quality_label(r) for r in triage_forms)

    return {
        "cancelled":       cancelled,
        "cc_total":        total_sched,
        "showed":          showed,
        "no_show_ss":      len(no_show_ss),
        "no_show_cc":      len(no_show_cc),
        "show_rate":       show_rate,
        "sent_to_cc_names": [person_name(r) for r in sent_to_cc],
        "no_show_ss_names": [person_name(r) for r in no_show_ss],
        "triage_total":    len(triage_forms),
        "triage_quality":  quality_counts,
    }


# ── Message ────────────────────────────────────────────────────────────────────
def format_message(leads_m, calls_m, meta_spend):
    now_ny   = datetime.now(TZ)
    date_str = now_ny.strftime("%A, %B %-d, %Y").upper()

    # Total Meta spend today
    total_meta_spend = sum(meta_spend.values())

    lines = [
        f"*DAILY AD PERFORMANCE — {date_str}*",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        "*TODAY'S LEADS*",
        (f"New Leads: *{leads_m['total']}*   |   Booked: *{leads_m['booked']}*   |   "
         f"Booking Rate: *{leads_m['booking_rate']:.1f}%*   |   "
         f"Closed: *{leads_m['closed']}*   |   Revenue: *${leads_m['revenue']:,.0f}*"),
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
        (f"Showed: *{calls_m['showed']}*   |   "
         f"No Show: *{calls_m['no_show_ss'] + calls_m['no_show_cc']}*   |   "
         f"Show Rate: *{calls_m['show_rate']:.0f}%*"),
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
        "",
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        "",
        f"*META ADS — TODAY'S SPEND*",
        f"Total Spend: *${total_meta_spend:,.2f}*",
        "",
    ]

    # Per active FB campaign spend + Airtable lead match
    airtable_camps = leads_m["campaigns"]
    all_camps = set(list(meta_spend.keys()) + list(airtable_camps.keys()))

    for camp in sorted(all_camps, key=lambda c: -meta_spend.get(c, 0)):
        if camp == "Unattributed":
            continue
        spend   = meta_spend.get(camp, 0)
        at_data = airtable_camps.get(camp, {"leads": 0, "booked": 0})
        leads   = at_data["leads"]
        booked  = at_data["booked"]
        br      = (booked / leads * 100) if leads > 0 else 0
        cpbc    = (spend / booked) if booked > 0 else 0
        spend_str = f"${spend:,.2f}" if spend > 0 else "$0"
        cpbc_str  = f"${cpbc:,.0f}" if cpbc > 0 else "—"

        lines.append(f"*{camp}*")
        lines.append(
            f"Spend: *{spend_str}*   |   Leads: *{leads}*   |   "
            f"Booked: *{booked}*   |   Booking Rate: *{br:.0f}%*   |   CPBC: *{cpbc_str}*"
        )
        lines.append("")

    # Unattributed
    if "Unattributed" in airtable_camps:
        d  = airtable_camps["Unattributed"]
        br = (d["booked"] / d["leads"] * 100) if d["leads"] > 0 else 0
        lines += [
            "*Unattributed*",
            f"Leads: *{d['leads']}*   |   Booked: *{d['booked']}*   |   Booking Rate: *{br:.0f}%*",
            "",
        ]

    lines += [
        "━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━",
        f"_Source: Airtable · Meta Ads · EOC Forms · {now_ny.strftime('%b %-d, %Y')}_",
    ]

    return "\n".join(lines)


# ── Slack ──────────────────────────────────────────────────────────────────────
def send_slack(message):
    r = requests.post(
        "https://slack.com/api/chat.postMessage",
        headers={
            "Authorization": f"Bearer {SLACK_TOKEN}",
            "Content-Type":  "application/json",
        },
        json={"channel": SLACK_CHANNEL_ID, "text": message},
        timeout=30,
    )
    r.raise_for_status()
    data = r.json()
    if not data.get("ok"):
        raise RuntimeError(f"Slack error: {data.get('error')}")
    print(f"Sent: {data['ts']}")


# ── Main ───────────────────────────────────────────────────────────────────────
def main():
    print("Fetching data...")
    raw_leads  = fetch_leads_today()
    leads      = dedup_leads(raw_leads)
    calls      = fetch_calls_today()
    eoc        = fetch_eoc_today()
    meta_spend = fetch_meta_spend_today()

    print(f"  Leads (deduped): {len(leads)} / raw: {len(raw_leads)}")
    print(f"  Calls: {len(calls)}  |  EOC forms: {len(eoc)}")
    print(f"  Meta campaigns with spend today: {len(meta_spend)}")
    for k, v in meta_spend.items():
        print(f"    {k}: ${v:.2f}")

    leads_m = compute_leads_metrics(leads)
    calls_m = compute_calls_metrics(calls, eoc)
    message = format_message(leads_m, calls_m, meta_spend)

    print("\n── MESSAGE PREVIEW ──")
    print(message)
    print("────────────────────\n")

    send_slack(message)
    print("Done.")


if __name__ == "__main__":
    main()
