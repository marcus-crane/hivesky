import csv
import os
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

r = browserless_fetch("https://www.govt.nz/browse/engaging-with-government/consultations-have-your-say/consultations-listing/")
soup = BeautifulSoup(r, features="lxml")

post_to_bluesky = os.environ.get("POST_TO_BLUESKY", False)
client = None
if post_to_bluesky:
    client = Client()
    client.login(
        os.environ.get("BLUESKY_USERNAME", False),
        os.environ.get("BLUESKY_PASSWORD", False),
    )

titles = []

for item in soup.find_all("div", class_="ga-content-container"):
    title = item.find("h3", class_="cli-title").text
    agencies = item.find("span", class_="cli-agencies").text
    status_str = item.find("span", class_="cli-status").text
    consult_open = False
    if status_str.lower().strip() == "open":
        consult_open = True
    href = item.find("a").attrs.get('href')

    start, end = parse_date_ranges(item.find("span", class_="cli-date").text)

    prev_item = previous_state.get(href, {})
    notified_open = as_bool(prev_item.get("notified_open"))
    notified_1week = as_bool(prev_item.get("notified_1week"))
    notified_1day = as_bool(prev_item.get("notified_1day"))

    if consult_open:
        days_left = (end - pendulum.now("Pacific/Auckland")).in_days()
        text = None
        if 0 < days_left <= 1 and not notified_1day:
            text = f"The following consultation from {agencies} closes for submissions tomorrow\n\n{title}"
        elif 0 < days_left <= 7 and not notified_1week:
            text = f"The following consultation from {agencies} closes for submissions in 1 week\n\n{title}"
        elif not notified_open:
            text = f"The following consultation from {agencies} is now open for submissions\n\n{title}"

        posted = True
        if text:
            image_url, og_title, og_description = fetch_og_meta(href)
            if post_to_bluesky:
                embed = build_link_embed(
                    client, href, og_title or title, og_description, image_url
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
                print(f"Thumb: {image_url}" if image_url else "(no thumb)")

        if posted:
            notified_open = True
            if days_left <= 7:
                notified_1week = True
            if days_left <= 1:
                notified_1day = True

    titles.append({
        'link': href,
        'title': title,
        'agencies': agencies,
        'start': start.to_iso8601_string(),
        'end': end.to_iso8601_string(),
        'open': consult_open,
        'notified_open': notified_open,
        'notified_1week': notified_1week,
        'notified_1day': notified_1day
    })

titles.sort(key=lambda k: pendulum.parse(k["start"]))

with STATE_FILE.open("w", newline="") as file:
    writer = csv.DictWriter(file, fieldnames=FIELDS)
    writer.writeheader()
    writer.writerows(titles)
