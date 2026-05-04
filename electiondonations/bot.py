import csv
import os
import re
import sys
import time
from io import BytesIO
from urllib.parse import urljoin

import requests
from atproto import Client, models
from bs4 import BeautifulSoup
from PIL import Image

BSKY_BLOB_LIMIT = 1_000_000

DONATIONS_URL = "https://elections.nz/democracy-in-nz/political-parties-in-new-zealand/donations-exceeding-20000"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"

PARTY_SITES = {
    "ACT New Zealand": "https://www.act.org.nz",
    "New Zealand First Party": "https://www.nzfirst.nz",
    "The Green Party of Aotearoa New Zealand": "https://www.greens.org.nz",
    "Te Pāti Māori": "https://www.maoriparty.org.nz",
    "New Zealand Labour Party": "https://www.labour.org.nz",
    "The New Zealand National Party": "https://www.national.org.nz",
    "Opportunity Party": "https://www.opportunity.org.nz/",
    "The Opportunities Party": "https://www.opportunity.org.nz/",
}

# These party websites don't have og:image banners sadly
PARTY_BANNERS = {
    "ACT New Zealand": "https://framerusercontent.com/images/WiUfeXvLaYKyPDOWzkFn5Wm6B8.png",
}


class Donation:
    def __init__(
        self,
        party,
        return_date,
        donor_name,
        amount,
        donation_date,
        pdf_url,
    ):
        self.party = party
        self.return_date = return_date
        self.donor_name = donor_name
        self.amount = amount
        self.donation_date = donation_date
        self.pdf_url = pdf_url

    def __str__(self):
        return f"{self.party} <- {self.amount} from {self.donor_name}"

    def __repr__(self):
        return self.__str__()


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


def fetch_page():
    return browserless_fetch(DONATIONS_URL)


def cell_lines(td):
    # Convert <br> to newlines so get_text honours them as line breaks.
    for br in td.find_all("br"):
        br.replace_with("\n")
    text = td.get_text("\n").replace("\xa0", " ")
    return [line.strip() for line in text.split("\n") if line.strip()]


def parse_amount(text):
    # text looks like "$50,000, 29 April 2026" or "$100,000.00, 28 April 2026".
    m = re.match(r"^(\$[\d,]+(?:\.\d+)?)\s*,?\s*(.*)$", text)
    if not m:
        return None, None
    amount = m.group(1).rstrip(",")
    if amount.endswith(".00"):
        amount = amount[:-3]
    return amount, m.group(2).strip()


def parse_donations(html):
    soup = BeautifulSoup(html, "html.parser")
    # The page lists one table per electoral cycle (e.g. "since 1 January 2026",
    # "since 1 January 2023"). We only post from the most recent cycle, which is
    # always the first heading.
    heading = None
    for tag in soup.find_all(["h2", "h3"]):
        if "Party donations exceeding" in tag.get_text():
            heading = tag
            break
    if heading is None:
        print("Could not find donations heading on page")
        sys.exit(1)
    table = heading.find_next("table")
    if table is None:
        print("Could not find donations table after heading")
        sys.exit(1)

    donations = []
    tbody = table.find("tbody")
    if tbody is None:
        tbody = table
    for row in tbody.find_all("tr"):
        cells = row.find_all("td")
        if len(cells) < 3:
            continue

        party_lines = cell_lines(cells[0])
        donor_lines = cell_lines(cells[1])
        amount_lines = cell_lines(cells[2])
        if not party_lines or not donor_lines or not amount_lines:
            continue
        # Header rows sometimes live inside the tbody.
        if "name and address" in donor_lines[0].lower():
            continue

        link = cells[2].find("a", href=True)
        if link is None:
            continue
        pdf_url = urljoin(DONATIONS_URL, link["href"])

        amount, donation_date = parse_amount(amount_lines[0])
        if amount is None:
            continue

        party = party_lines[0]
        if len(party_lines) > 1:
            return_date = party_lines[1]
        else:
            return_date = ""
        donor_name = donor_lines[0]

        donations.append(
            Donation(
                party,
                return_date,
                donor_name,
                amount,
                donation_date,
                pdf_url,
            )
        )

    return donations


def load_history():
    path = os.path.join(SCRIPT_DIR, "history.csv")
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return []
    with open(path) as f:
        return list(csv.DictReader(f))


def save_history(history, donation):
    history.append({"url": donation.pdf_url})
    path = os.path.join(SCRIPT_DIR, "history.csv")
    with open(path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["url"])
        writer.writeheader()
        for row in history:
            writer.writerow(row)


def published_urls(history):
    return {row["url"] for row in history}


def party_site(party):
    return PARTY_SITES.get(party.strip())


def fetch_og_card(url):
    try:
        html = browserless_fetch(url)
    except requests.RequestException as e:
        print(f"Failed to fetch OG card for {url}: {e}")
        return None

    soup = BeautifulSoup(html, "html.parser")

    def meta(prop, attr="property"):
        tag = soup.find("meta", attrs={attr: prop})
        if not tag:
            return ""
        content = tag.get("content")
        if not content:
            return ""
        return content.strip()

    title = meta("og:title")
    if not title:
        title_tag = soup.find("title")
        if title_tag:
            title = title_tag.get_text().strip()

    description = meta("og:description")
    if not description:
        description = meta("description", attr="name")

    image = meta("og:image")
    if image:
        image = urljoin(url, image)

    return {"title": title, "description": description, "image": image}


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


def build_post_text(donation):
    text = f"{donation.party} received a {donation.amount} donation from {donation.donor_name}"
    if donation.donation_date:
        text += f" on {donation.donation_date}"
    text += "."
    return text


if __name__ == "__main__":
    html = fetch_page()
    donations = parse_donations(html)
    history = load_history()
    seen = published_urls(history)

    # Reverse so oldest donations get posted first when bootstrapping or
    # catching up after an outage.
    donations = list(reversed(donations))

    post_to_bluesky = os.environ.get("POST_TO_BLUESKY", False)
    client = None
    if post_to_bluesky:
        client = Client()
        client.login(
            os.environ.get("BLUESKY_USERNAME", False),
            os.environ.get("BLUESKY_PASSWORD", False),
        )

    # Cache OG cards across the run so we hit each party homepage at most once.
    og_cache = {}

    for donation in donations:
        if donation.pdf_url in seen:
            print(f"Skipped {donation.pdf_url} as already syndicated")
            continue

        text = build_post_text(donation)
        site = party_site(donation.party)
        og = None
        if site:
            if site not in og_cache:
                og_cache[site] = fetch_og_card(site)
            og = og_cache[site]

        embed = None
        embed_title = None
        embed_description = None
        embed_image_url = None
        if og:
            embed_title = og["title"]
            embed_description = og["description"]

            embed_image_url = og["image"]
            if not embed_image_url:
                banner = PARTY_BANNERS.get(donation.party.strip())
                if banner:
                    embed_image_url = banner

            if post_to_bluesky and embed_image_url:
                thumb = None
                img_data = fetch_image_bytes(embed_image_url)
                if img_data:
                    img_data = fit_thumb(img_data)
                if img_data:
                    thumb = client.upload_blob(img_data).blob
                embed = models.AppBskyEmbedExternal.Main(
                    external=models.AppBskyEmbedExternal.External(
                        title=embed_title,
                        description=embed_description,
                        uri=site,
                        thumb=thumb,
                    )
                )

        if post_to_bluesky:
            try:
                if embed:
                    client.send_post(text, embed=embed)
                else:
                    client.send_post(text)
                save_history(history, donation)
                print(f"Successfully posted {donation.pdf_url}")
                # Avoid spamming followers and any Bluesky rate limits.
                time.sleep(5)
            except Exception as e:
                print(f"Failed to post {donation.pdf_url}: {e}")
                continue
        else:
            print(text)
            print("----")
            if og:
                print(embed_title)
                print(embed_description)
                print(site)
                if embed_image_url:
                    print(f"Thumb: {embed_image_url}")
                else:
                    print("Thumb: (none)")
            else:
                print("(no embed)")
            print("----")
            save_history(history, donation)
