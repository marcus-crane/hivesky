import csv
import os
import re
from pathlib import Path
import sys
import time
from io import BytesIO
from urllib.parse import urljoin

from atproto import Client, models
from bs4 import BeautifulSoup
from PIL import Image
import pendulum
import requests

BSKY_BLOB_LIMIT = 1_000_000
USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

STATE_FILE = Path(__file__).parent / "consultations.csv"

FIELDS = [
    'link',
    'title',
    'agencies',
    'start',
    'end',
    'open',
    'notified_open',
    'notified_1week',
    'notified_1day'
]

previous_state = {}

if STATE_FILE.exists():
    with STATE_FILE.open(newline="") as file:
        reader = csv.DictReader(file)
        for row in reader:
            previous_state[row["link"]] = row

def as_bool(v):
    return str(v).strip().lower() == "true"

def browserless_fetch(target_url, timeout=60):
    # Elections uses Imperva WAF and some party websites probably dislike
    # seeing requests from Github's IP addresses too (NationBuilder probably)
    token = os.environ.get("BROWSERLESS_API_TOKEN")
    if not token:
        print("Please set BROWSERLESS_API_TOKEN env var")
        sys.exit(1)
    url = os.environ.get("BROWSERLESS_URL")
    if not url:
        print("Please set BROWSERLESS_URL env var")
        sys.exit(1)

    # Inline the token into the URL so requests does not percent-encode it via
    # the params= dict (which mangles tokens containing characters like + or =).
    if "?" in url:
        sep = "&"
    else:
        sep = "?"
    full_url = f"{url}{sep}token={token}"
    r = requests.post(
        full_url,
        params={"stealth": True},
        json={"url": target_url},
        timeout=timeout,
    )
    r.raise_for_status()
    # Browserless wraps the rendered page in an HTML envelope — strip it back
    # out so BeautifulSoup parses the original document.
    soup = BeautifulSoup(r.text, "html.parser")
    pre = soup.find("pre")
    if pre:
        return pre.get_text()
    return r.text

def fetch_og_meta(url):
    try:
        html = browserless_fetch(url)
    except requests.RequestException as e:
        print(f"Failed to fetch OG metadata for {url}: {e}")
        return None, None, None

    soup = BeautifulSoup(html, "html.parser")

    def meta(prop):
        tag = soup.find("meta", attrs={"property": prop})
        if tag and tag.get("content"):
            return tag.get("content").strip()
        return None

    image = meta("og:image")
    if image:
        image = urljoin(url, image)
    return image, meta("og:title"), meta("og:description") or ""

def fetch_image_bytes(url):
    try:
        r = requests.get(url, headers={"User-Agent": USER_AGENT}, timeout=30)
        r.raise_for_status()
        return r.content
    except requests.RequestException as e:
        print(f"Failed to fetch image {url}: {e}")
        return None

def fit_thumb(img_bytes):
    # Bluesky rejects embed thumbs over 1MB. Re-encode as progressively smaller
    # JPEGs until it fits, or give up (caller drops the thumb).
    if len(img_bytes) <= BSKY_BLOB_LIMIT:
        return img_bytes
    try:
        img = Image.open(BytesIO(img_bytes))
    except Exception as e:
        print(f"Could not open image for resizing: {e}")
        return None
    if img.mode != "RGB":
        img = img.convert("RGB")
    for max_dim, quality in [(1600, 85), (1200, 80), (1000, 75), (800, 70), (600, 65)]:
        copy = img.copy()
        copy.thumbnail((max_dim, max_dim))
        buf = BytesIO()
        copy.save(buf, format="JPEG", quality=quality, optimize=True)
        data = buf.getvalue()
        if len(data) <= BSKY_BLOB_LIMIT:
            return data
    print("Could not shrink image below Bluesky blob limit")
    return None

def build_link_embed(client, href, title, description, image_url):
    thumb = None
    if image_url:
        img_data = fetch_image_bytes(image_url)
        if img_data:
            img_data = fit_thumb(img_data)
        if img_data:
            thumb = client.upload_blob(img_data).blob
    return models.AppBskyEmbedExternal.Main(
        external=models.AppBskyEmbedExternal.External(
            uri=href,
            title=title,
            description=description,
            thumb=thumb,
        )
    )

def parse_date_ranges(date):
    # 11 Dec 2025 to 27 Feb 2026 => Date format when crossing year boundry pulled from Wayback Machine
    start_str, end_str = date.split(" to ")
    parsed_end = pendulum.from_format(end_str, "D MMM YYYY", tz="Pacific/Auckland")
    try:
        parsed_start = pendulum.from_format(start_str, "D MMM YYYY", tz="Pacific/Auckland")
    except:
        # For most of the year, we only have the end year available
        parsed_start = pendulum.from_format(f"{start_str} {parsed_end.year}", "D MMM YYYY", tz="Pacific/Auckland")
    return parsed_start, parsed_end

# Each scraper returns a list of consultation dicts with these keys:
#   link, title, agencies, start (pendulum or None), end (pendulum), open (bool)
# The shared loop below handles state, notifications and posting for all of them.

def scrape_govtnz():
    # Central government aggregator — links out to individual agency pages and
    # carries each consultation's own agency, status and start-to-end date range.
    html = browserless_fetch("https://www.govt.nz/browse/engaging-with-government/consultations-have-your-say/consultations-listing/")
    soup = BeautifulSoup(html, features="lxml")
    out = []
    for item in soup.find_all("div", class_="ga-content-container"):
        start, end = parse_date_ranges(item.find("span", class_="cli-date").text)
        out.append({
            "link": item.find("a").attrs.get("href"),
            "title": item.find("h3", class_="cli-title").text,
            "agencies": item.find("span", class_="cli-agencies").text,
            "start": start,
            "end": end,
            "open": item.find("span", class_="cli-status").text.lower().strip() == "open",
        })
    return out

def parse_mbie_due(text):
    # "Submissions due: 31 July 2026, 5pm" — MBIE only publishes the close date.
    match = re.search(r"(\d{1,2}\s+[A-Za-z]+\s+\d{4})", text)
    if not match:
        return None
    return pendulum.from_format(match.group(1), "D MMMM YYYY", tz="Pacific/Auckland")

def scrape_mbie():
    # MBIE runs its own listing behind Imperva (browserless stealth gets through).
    # Every item is an MBIE consultation, links are relative, and there is no
    # start date — only a "Submissions due" close date.
    base = "https://www.mbie.govt.nz"
    html = browserless_fetch(f"{base}/have-your-say?type[open]=open")
    soup = BeautifulSoup(html, features="lxml")
    out = []
    for item in soup.find_all("div", class_="listing-item"):
        link_tag = item.find("a", class_="listing-link")
        date_tag = item.find("span", class_="submission-date")
        if not link_tag or not date_tag:
            continue
        end = parse_mbie_due(date_tag.get_text())
        if end is None:
            continue
        # MBIE detail pages carry no OG metadata, so keep the listing blurb to
        # use as the link-card description.
        blurb = item.find("p", class_="lh-copy")
        out.append({
            "link": urljoin(base, link_tag.attrs.get("href")),
            "title": link_tag.get_text(strip=True),
            "agencies": "Ministry of Business, Innovation and Employment",
            "start": None,
            "end": end,
            "open": "item-open" in (item.get("class") or []),
            "description": blurb.get_text(strip=True) if blurb else "",
        })
    return out

def parse_epa_due(text):
    # Detail pages state "You have until 21 July 2026 to make a submission."
    match = re.search(r"until\s+(\d{1,2}\s+[A-Za-z]+\s+\d{4})\s+to make a submission", text, re.I)
    if not match:
        return None
    return pendulum.from_format(match.group(1), "D MMMM YYYY", tz="Pacific/Auckland")

def scrape_epa():
    # EPA lists open consultations without close dates; the deadline lives on
    # each detail page. Reuse a previously stored end date when we have one so we
    # don't refetch every detail page on every run.
    base = "https://www.epa.govt.nz"
    html = browserless_fetch(f"{base}/public-consultations/open-consultations/")
    soup = BeautifulSoup(html, features="lxml")
    out = []
    for item in soup.find_all("li", class_="result"):
        link_tag = item.find("a", class_="result__link")
        if not link_tag:
            continue
        href = urljoin(base, link_tag.attrs.get("href"))
        prev_end = previous_state.get(href, {}).get("end")
        if prev_end:
            end = pendulum.parse(prev_end)
        else:
            end = parse_epa_due(browserless_fetch(href))
        if end is None:
            print(f"Could not find a close date for EPA consultation {href}, skipping")
            continue
        summary = item.find("p", class_="result__summary")
        out.append({
            "link": href,
            "title": link_tag.get("title") or link_tag.get_text(strip=True),
            "agencies": "Environmental Protection Authority",
            "start": None,
            "end": end,
            "open": True,
            "description": summary.get_text(strip=True) if summary else "",
        })
    return out

def parse_cs_date(text):
    # Citizen Space detail dates read "Closes 5 Jul 2026" / "Opened 6 Jun 2026".
    match = re.search(r"(\d{1,2}\s+[A-Za-z]+\s+\d{4})", text)
    if not match:
        return None
    return pendulum.from_format(match.group(1), "D MMM YYYY", tz="Pacific/Auckland")

def scrape_health():
    # Ministry of Health runs Citizen Space. The finder lists open consultations
    # but its inline date label flips between "Opened"/"Closes" depending on sort,
    # so read the close (and open) dates off the detail page's date sidebar.
    # Reuse stored dates to avoid refetching every detail page each run.
    base = "https://consult.health.govt.nz"
    html = browserless_fetch(f"{base}/consultation_finder/?st=open")
    soup = BeautifulSoup(html, features="lxml")
    out = []
    for card in soup.find_all("li", attrs={"data-consultation-state": "open"}):
        title_tag = card.select_one("h2 a")
        if not title_tag:
            continue
        href = urljoin(base, title_tag.attrs.get("href"))
        prev = previous_state.get(href, {})
        if prev.get("end"):
            start = pendulum.parse(prev["start"]) if prev.get("start") else None
            end = pendulum.parse(prev["end"])
        else:
            detail = BeautifulSoup(browserless_fetch(href), features="lxml")
            close_tag = detail.find(class_="cs-consultation-sidebar-primary-date")
            open_tag = detail.find(class_="cs-consultation-sidebar-secondary-date")
            end = parse_cs_date(close_tag.get_text()) if close_tag else None
            start = parse_cs_date(open_tag.get_text()) if open_tag else None
        if end is None:
            print(f"Could not find a close date for health consultation {href}, skipping")
            continue
        body = card.find("div", class_="col-md-9")
        summary = body.find("span") if body else None
        out.append({
            "link": href,
            "title": title_tag.get_text(strip=True),
            "agencies": "Ministry of Health",
            "start": start,
            "end": end,
            "open": True,
            "description": summary.get_text(strip=True) if summary else "",
        })
    return out

def parse_nz_date(text):
    # Accepts "9 June 2026" or "26 Jun 2026" (full or abbreviated month name).
    match = re.search(r"(\d{1,2}\s+[A-Za-z]+\s+\d{4})", text)
    if not match:
        return None
    for fmt in ("D MMMM YYYY", "D MMM YYYY"):
        try:
            return pendulum.from_format(match.group(1), fmt, tz="Pacific/Auckland")
        except Exception:
            continue
    return None

def scrape_pharmac():
    # Pharmac's listing gives the open date with a year but the close date
    # without one ("Closes 26 Jun"), so infer the close year from the open date,
    # rolling over when the close month precedes the open month. Detail pages
    # supply og:title and og:image for the link card.
    base = "https://www.pharmac.govt.nz"
    html = browserless_fetch(f"{base}/news-and-resources/consultations-and-decisions?type=Consultation&page=1&status=open")
    soup = BeautifulSoup(html, features="lxml")
    out = []
    results = soup.find(class_="list-results")
    if not results:
        return out
    for item in results.find_all("li"):
        link_tag = item.find("a", class_="link-chevron")
        close_tag = item.find("time", class_="tag-type")
        if not link_tag or not close_tag:
            continue
        # Drop the listing's tracking query params so the link is canonical.
        href = urljoin(base, link_tag.attrs.get("href").split("?")[0])
        opened_tag = item.find("time", class_="list-time")
        start = parse_nz_date(opened_tag.get("datetime") or opened_tag.get_text()) if opened_tag else None
        close_match = re.search(r"(\d{1,2})\s+([A-Za-z]+)", (close_tag.get("datetime") or "") + " " + close_tag.get_text())
        if not close_match:
            print(f"Could not find a close date for Pharmac consultation {href}, skipping")
            continue
        base_year = (start or pendulum.now("Pacific/Auckland")).year
        end = parse_nz_date(f"{close_match.group(1)} {close_match.group(2)} {base_year}")
        if end and start and end < start:
            end = end.add(years=1)
        if end is None:
            print(f"Could not parse close date for Pharmac consultation {href}, skipping")
            continue
        out.append({
            "link": href,
            "title": link_tag.get_text(strip=True),
            "agencies": "Pharmac",
            "start": start,
            "end": end,
            "open": True,
        })
    return out

def scrape_doc():
    # DOC lists open consultations as cards; the close date, when present, is
    # embedded in the summary prose ("Submissions close 14 July 2026."). Some
    # consultations have no date at all, so end stays None for those.
    base = "https://www.doc.govt.nz"
    html = browserless_fetch(f"{base}/get-involved/have-your-say/open-for-your-comment/")
    soup = BeautifulSoup(html, features="lxml")
    out = []
    for card in soup.find_all(class_="card"):
        link_tag = card.find("a", class_="card_link")
        if not link_tag:
            continue
        href = link_tag.attrs.get("href", "")
        # Skip the "All consultations" index card that shares the same markup.
        if href.rstrip("/").endswith("all-consultations"):
            continue
        body = card.find("p")
        description = body.get_text(strip=True) if body else ""
        # Only treat a date as the close date when it sits in the same clause as
        # "close" — avoids grabbing unrelated dates from the blurb.
        close_match = re.search(r"clos\w*\b[^.]*?(\d{1,2}\s+[A-Za-z]+\s+\d{4})", description, re.I)
        out.append({
            "link": urljoin(base, href),
            "title": link_tag.get_text(strip=True),
            "agencies": "Department of Conservation",
            "start": None,
            "end": parse_nz_date(close_match.group(1)) if close_match else None,
            "open": True,
            "description": description,
        })
    return out

SCRAPERS = [scrape_govtnz, scrape_mbie, scrape_epa, scrape_health, scrape_pharmac, scrape_doc]

post_to_bluesky = os.environ.get("POST_TO_BLUESKY", False)
client = None
if post_to_bluesky:
    client = Client()
    client.login(
        os.environ.get("BLUESKY_USERNAME", False),
        os.environ.get("BLUESKY_PASSWORD", False),
    )

consultations = []
seen_links = set()
for scraper in SCRAPERS:
    try:
        scraped = scraper()
    except Exception as e:
        # Don't let one site's outage drop everything else's notifications.
        print(f"Scraper {scraper.__name__} failed: {e}")
        continue
    for consultation in scraped:
        # Dedupe by link in case a consultation shows up via more than one source.
        if consultation["link"] in seen_links:
            continue
        seen_links.add(consultation["link"])
        consultations.append(consultation)

titles = []

for consultation in consultations:
    href = consultation["link"]
    title = consultation["title"]
    agencies = consultation["agencies"]
    start = consultation["start"]
    end = consultation["end"]
    consult_open = consultation["open"]

    prev_item = previous_state.get(href, {})
    notified_open = as_bool(prev_item.get("notified_open"))
    notified_1week = as_bool(prev_item.get("notified_1week"))
    notified_1day = as_bool(prev_item.get("notified_1day"))

    if consult_open:
        # Some sources (DOC) don't always publish a close date — those can only
        # get the initial "now open" notice, never the closing-soon reminders.
        days_left = (end - pendulum.now("Pacific/Auckland")).in_days() if end else None
        text = None
        if days_left is not None and 0 < days_left <= 1 and not notified_1day:
            text = f"The following consultation from {agencies} closes for submissions tomorrow\n\n{title}"
        elif days_left is not None and 0 < days_left <= 7 and not notified_1week:
            text = f"The following consultation from {agencies} closes for submissions in 1 week\n\n{title}"
        elif not notified_open:
            text = f"The following consultation from {agencies} is now open for submissions\n\n{title}"

        posted = True
        if text:
            image_url, og_title, og_description = fetch_og_meta(href)
            # Fall back to the listing blurb when the page has no OG description.
            card_description = og_description or consultation.get("description", "")
            # Tidy whitespace and keep the card description to a sane length.
            card_description = re.sub(r"\s+", " ", card_description).strip()
            if len(card_description) > 300:
                card_description = card_description[:297].rstrip() + "…"
            if post_to_bluesky:
                embed = build_link_embed(
                    client, href, og_title or title, card_description, image_url
                )
                try:
                    client.send_post(text, embed=embed)
                    print(f"Successfully posted {href}")
                    time.sleep(5)
                except Exception as e:
                    print(f"Failed to post {href}: {e}")
                    posted = False
            else:
                print(text)
                print(f"Card: {og_title or title} — {href}")
                print(f"Desc: {card_description}" if card_description else "(no description)")
                print(f"Thumb: {image_url}" if image_url else "(no thumb)")

        if posted:
            notified_open = True
            if days_left is not None and days_left <= 7:
                notified_1week = True
            if days_left is not None and days_left <= 1:
                notified_1day = True

    titles.append({
        'link': href,
        'title': title,
        'agencies': agencies,
        # Some sources don't publish a start (MBIE) or close (some DOC) date;
        # leave whatever is missing blank.
        'start': start.to_iso8601_string() if start else "",
        'end': end.to_iso8601_string() if end else "",
        'open': consult_open,
        'notified_open': notified_open,
        'notified_1week': notified_1week,
        'notified_1day': notified_1day
    })

# Sort by start date, falling back to the close date, then to now when a
# consultation has neither.
titles.sort(key=lambda k: pendulum.parse(k["start"] or k["end"]) if (k["start"] or k["end"]) else pendulum.now("Pacific/Auckland"))

with STATE_FILE.open("w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(titles)
