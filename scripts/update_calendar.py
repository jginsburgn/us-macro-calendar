import re
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup
import pytz

# --- Sources ---

BLS_URL = "https://www.bls.gov/schedule/news_release/bls.ics"
BEA_URL = "https://www.bea.gov/news/schedule/ics/online-calendar-subscription.ics"
FOMC_URL = "https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm"

NOW = datetime.now(timezone.utc)

# Major macro keywords to keep from BLS/BEA
MAJOR_KEYWORDS = [
    "Consumer Price Index",         # CPI
    "Employment Situation",         # Nonfarm Payrolls
    "Producer Price Index",         # PPI
    "Gross Domestic Product",       # GDP
    "Personal Income and Outlays",  # PCE-ish
]

MONTH_MAP = {
    "January": 1,
    "February": 2,
    "March": 3,
    "April": 4,
    "May": 5,
    "June": 6,
    "July": 7,
    "August": 8,
    "September": 9,
    "October": 10,
    "November": 11,
    "December": 12,
}

EASTERN = pytz.timezone("US/Eastern")


# ---------- Helpers for ICS parsing (BLS/BEA) ----------

def fetch_lines(url: str):
    # User-Agent to avoid 403s from BLS/BEA
    headers = {
        "User-Agent": (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0 Safari/537.36"
        ),
        "Accept": "text/calendar,text/plain,*/*",
    }
    resp = requests.get(url, headers=headers, timeout=30)
    resp.raise_for_status()
    return resp.text.splitlines()


def parse_dtstart(line: str):
    """
    Parses a DTSTART line into a datetime in UTC (best effort).
    Handles:
      DTSTART;VALUE=DATE-TIME:20250107T133000Z
      DTSTART;VALUE=DATE:20260128
      DTSTART;TZID=US-Eastern:20260109T083000
      DTSTART:20250425T180000Z
    """
    if ":" not in line:
        return None
    value = line.split(":", 1)[1].strip()
    try:
        if "T" in value:
            # Has time
            if value.endswith("Z"):
                dt = datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(
                    tzinfo=timezone.utc
                )
            else:
                # Treat as naive UTC here (we only use it for >= NOW filtering);
                # timezone-specific normalization happens later where needed.
                dt = datetime.strptime(value, "%Y%m%dT%H%M%S").replace(
                    tzinfo=timezone.utc
                )
        else:
            # Date only
            dt = datetime.strptime(value, "%Y%m%d").replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def annotate_source(event_lines, source_tag):
    """
    Add a source marker (BLS / BEA) into DESCRIPTION or as COMMENT.
    """
    out = []
    inserted = False
    for line in event_lines:
        out.append(line)
        if not inserted and line.startswith("DESCRIPTION:"):
            out.append(f"  (Source: {source_tag})")
            inserted = True
    if not inserted:
        out.insert(1, f"COMMENT:Source={source_tag}")
    return out


def filter_events(lines, source_tag):
    """
    Pull VEVENT blocks from an ICS file, keep only those with
    MAJOR_KEYWORDS in any line, and only for dates >= NOW.
    """
    events = []
    current = []
    in_event = False
    include = False
    event_dt = None

    for line in lines:
        if line.startswith("BEGIN:VEVENT"):
            in_event = True
            current = [line]
            include = False
            event_dt = None
        elif line.startswith("END:VEVENT"):
            current.append(line)
            if include and (event_dt is None or event_dt >= NOW):
                events.append(annotate_source(current, source_tag))
            in_event = False
            current = []
            event_dt = None
        elif in_event:
            if line.startswith("DTSTART"):
                dt = parse_dtstart(line)
                if dt is not None:
                    event_dt = dt
            if any(k in line for k in MAJOR_KEYWORDS):
                include = True
            current.append(line)
        else:
            # ignore non-event lines
            continue

    return events


def normalize_bls_events_to_utc(events):
    """
    For events sourced from BLS, convert any
    DTSTART;TZID=US-Eastern:YYYYMMDDTHHMMSS
    to plain UTC like:
    DTSTART:YYYYMMDDTHHMMSSZ

    We don't touch all-day VALUE=DATE events.
    """
    normalized = []
    for ev in events:
        new_ev = []
        for line in ev:
            if line.startswith("DTSTART;TZID=US-Eastern:"):
                val = line.split(":", 1)[1].strip()
                # BLS uses times like 20260109T083000
                try:
                    local_naive = datetime.strptime(val, "%Y%m%dT%H%M%S")
                    local_dt = EASTERN.localize(local_naive)
                    utc_dt = local_dt.astimezone(timezone.utc)
                    new_line = "DTSTART:" + utc_dt.strftime("%Y%m%dT%H%M%SZ")
                    new_ev.append(new_line)
                except Exception:
                    # Fall back to original line if parsing fails
                    new_ev.append(line)
            elif line.startswith("DTEND;TZID=US-Eastern:"):
                val = line.split(":", 1)[1].strip()
                try:
                    local_naive = datetime.strptime(val, "%Y%m%dT%H%M%S")
                    local_dt = EASTERN.localize(local_naive)
                    utc_dt = local_dt.astimezone(timezone.utc)
                    new_line = "DTEND:" + utc_dt.strftime("%Y%m%dT%H%M%SZ")
                    new_ev.append(new_line)
                except Exception:
                    new_ev.append(line)
            else:
                new_ev.append(line)
        normalized.append(new_ev)
    return normalized


# ---------- Scraping Fed FOMC page ----------

def scrape_fomc_events():
    """
    Scrape FOMC meeting dates from the Fed's official calendar page.
    We:
      - fetch the page
      - flatten to text
      - find year blocks like '2025 FOMC Meetings', '2026 FOMC Meetings'
      - within each block, find patterns: 'Month  DD-DD' (optional '*')
      - use the SECOND day as the policy decision date
      - make all-day events for those dates, only if >= NOW.
      - deduplicate meeting dates in case the pattern appears twice.
    """
    resp = requests.get(FOMC_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text("\n")

    events = []
    now_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # Years to scan for FOMC meetings; tweak as needed.
    years_to_scan = [2025, 2026, 2027]

    # Precompile month+day-range pattern, e.g. "January 27-28*"
    month_pattern = (
        r"(January|February|March|April|May|June|July|August|September|October|November|December)"
        r"[^\d]{0,20}"
        r"(\d{1,2})-(\d{1,2})\*?"  # start-end, optional '*'
    )
    month_re = re.compile(month_pattern)

    seen_dates = set()  # YYYYMMDD strings to avoid duplicates

    for year in years_to_scan:
        header = f"{year} FOMC Meetings"
        idx = text.find(header)
        if idx == -1:
            continue

        # Bound this section by the next year's header or end of text
        end_idx = len(text)
        for other_year in years_to_scan:
            if other_year <= year:
                continue
            other_header = f"{other_year} FOMC Meetings"
            tmp = text.find(other_header)
            if tmp != -1 and tmp > idx:
                end_idx = min(end_idx, tmp)

        section = text[idx:end_idx]

        for m in month_re.finditer(section):
            month_name, _day1_str, day2_str = m.groups()
            month = MONTH_MAP[month_name]
            day2 = int(day2_str)

            try:
                dt = datetime(year, month, day2, tzinfo=timezone.utc)
            except ValueError:
                continue

            if dt < NOW:
                continue

            date_key = dt.strftime("%Y%m%d")
            if date_key in seen_dates:
                # Avoid duplicates (e.g. if pattern appears twice in the text)
                continue
            seen_dates.add(date_key)

            uid = f"FOMC-{date_key}@us-macro"
            ev = [
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"DTSTAMP:{now_str}",
                f"DTSTART;VALUE=DATE:{date_key}",
                "SUMMARY:FOMC Meeting – Rate Decision",
                "DESCRIPTION:Federal Open Market Committee meeting "
                "(second day, policy statement expected). "
                f"(Source: Federal Reserve FOMC calendar, {year})",
                "END:VEVENT",
            ]
            events.append(ev)

    return events


# ---------- Main entrypoint ----------

def main():
    # BLS & BEA events
    bls_lines = fetch_lines(BLS_URL)
    bea_lines = fetch_lines(BEA_URL)

    bls_events = filter_events(bls_lines, "BLS")
    bea_events = filter_events(bea_lines, "BEA")

    # Normalize BLS events so DTSTART is UTC, no TZID=US-Eastern
    bls_events = normalize_bls_events_to_utc(bls_events)

    # Fed FOMC meetings scraped live
    fomc_events = scrape_fomc_events()

    header = [
        "BEGIN:VCALENDAR",
        "VERSION:2.0",
        "PRODID:-//YourGitHubUser//US Macro Calendar//EN",
        "CALSCALE:GREGORIAN",
        "METHOD:PUBLISH",
        "X-WR-CALNAME:US Macro Major Events",
    ]
    footer = ["END:VCALENDAR"]

    with open("us_macro.ics", "w", encoding="utf-8") as f:
        for line in header:
            f.write(line + "\n")
        for ev in bls_events + bea_events + fomc_events:
            for l in ev:
                f.write(l + "\n")
        for line in footer:
            f.write(line + "\n")


if __name__ == "__main__":
    main()
