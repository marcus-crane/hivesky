import csv
import os
from pathlib import Path
import sys

from bs4 import BeautifulSoup
import pendulum
import requests

STATE_FILE = Path("consultations.csv")

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
        if not notified_open:
            print("now open")
            notified_open = True
        if 0 < days_left <= 7 and not notified_1week and not notified_1day:
            print(f"{days_left} days left until {title} by {agencies}")
            notified_1week = True
        if 0 < days_left <= 1 and not notified_1day:
            print(f"{days_left} days left until {title} by {agencies}")
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
