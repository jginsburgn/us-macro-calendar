import re
import requests
from datetime import datetime, timezone
from bs4 import BeautifulSoup

# --- Sources ---

BLS_URL = "https://www.bls.gov/schedule/news_release/bls.ics"
# BEA ICS link – if this ever changes, update here.
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


# ---------- Helpers for ICS parsing (BLS/BEA) ----------

def fetch_lines(url: str):
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
    Parses a DTSTART line into a datetime in UTC.
    Handles:
      DTSTART;VALUE=DATE-TIME:20250107T133000Z
      DTSTART;VALUE=DATE:20260128
      DTSTART:20250425T180000Z
    """
    if ":" not in line:
        return None
    value = line.split(":", 1)[1].strip()
    try:
        if "T" in value:
            if value.endswith("Z"):
                dt = datetime.strptime(value, "%Y%m%dT%H%M%SZ").replace(
                    tzinfo=timezone.utc
                )
            else:
                dt = datetime.strptime(value, "%Y%m%dT%H%M%S").replace(
                    tzinfo=timezone.utc
                )
        else:
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
    """
    resp = requests.get(FOMC_URL, timeout=30)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.text, "html.parser")
    text = soup.get_text("\n")

    # For safety, normalize multiple spaces/newlines
    # but keep enough structure for regex to work.
    # We'll search per-year blocks.
    events = []
    now_str = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    # We'll cover current and near-future years; tweak as you like.
    years_to_scan = [2025, 2026, 2027]

    # Precompile month+day-range pattern
    month_pattern = (
        r"(January|February|March|April|May|June|July|August|September|October|November|December)"
        r"[^\d]{0,20}"          # some whitespace or punctuation between month and numbers
        r"(\d{1,2})-(\d{1,2})\*?"  # start-end, optional '*'
    )
    month_re = re.compile(month_pattern)

    for year in years_to_scan:
        header = f"{year} FOMC Meetings"
        idx = text.find(header)
        if idx == -1:
            continue

        # Find the start of the NEXT year header to bound the section
        # e.g., '2026 FOMC Meetings', etc.
        # If not found, go to end of text.
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
            month_name, day1_str, day2_str = m.groups()
            month = MONTH_MAP[month_name]
            day2 = int(day2_str)

            try:
                dt = datetime(year, month, day2, tzinfo=timezone.utc)
            except ValueError:
                # If for some reason the date is malformed, skip
                continue

            if dt < NOW:
                continue

            uid = f"FOMC-{dt.strftime('%Y%m%d')}@us-macro"
            ev = [
                "BEGIN:VEVENT",
                f"UID:{uid}",
                f"DTSTAMP:{now_str}",
                f"DTSTART;VALUE=DATE:{dt.strftime('%Y%m%d')}",
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
