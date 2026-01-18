import argparse
import csv
import time
from typing import List, Dict, Iterable
from urllib.parse import urlparse, urlunparse, urljoin
import requests
from bs4 import BeautifulSoup


#CATEGORY_URLS: Dict[str, str] = {
#    "electronics": "https://www.jumia.com.ng/electronics/",
#    "home_office": "https://www.jumia.com.ng/home-office/",
#    "health_beauty_personal_care": "https://www.jumia.com.ng/health-beauty/",
#    "phones_tablets": "https://www.jumia.com.ng/phones-tablets/",
#    "fashion": "https://www.jumia.com.ng/category-fashion-by-jumia/",
#}


CATEGORY_URLS: Dict[str, str] = {
    "electronics": "https://www.konga.com/category/electronics-5261",
    "home_office": "https://www.konga.com/category/home-kitchen-602",
    "health_beauty_personal_care": "https://www.konga.com/category/beauty-health-personal-care-4",
    "phones_tablets": "https://www.konga.com/category/phones-tablets-5294",
    "fashion": "https://www.konga.com/category/konga-fashion-1259",
}

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                  "AppleWebKit/537.36 (KHTML, like Gecko) "
                  "Chrome/122.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
}


def fetch_page(url: str, params: Dict = None) -> str:
    response = requests.get(url, headers=HEADERS, params=params, timeout=20)
    response.raise_for_status()
    return response.text


def normalize_url(url: str) -> str:
    parsed = urlparse(url)
    normalized = parsed._replace(query="", fragment="")
    return urlunparse(normalized)


def parse_reviews(html: str) -> List[Dict]:
    soup = BeautifulSoup(html, "html.parser")
    reviews = []

    review_blocks = soup.select("article.-review, div.-review, article.review, div.review")
    if not review_blocks:
        review_blocks = soup.select("section.card.aim article")

    for block in review_blocks:
        title_el = block.select_one("h3")
        body_el = None
        body_tags = block.select("p")
        if body_tags:
            body_el = body_tags[0]

        rating_el = block.select_one(".stars")
        author = ""
        date = ""
        meta_div = block.select_one("div.-df.-j-bet.-i-ctr.-gy5")
        if meta_div:
            spans = meta_div.select("span")
            if len(spans) > 0:
                date = spans[0].get_text(strip=True)
            if len(spans) > 1:
                author_text = spans[1].get_text(strip=True)
                if author_text.lower().startswith("by "):
                    author = author_text[3:].strip()
                else:
                    author = author_text

        title = title_el.get_text(strip=True) if title_el else ""
        body = body_el.get_text(strip=True) if body_el else ""

        rating = ""
        if rating_el:
            txt = rating_el.get_text(strip=True)
            parts = txt.split()
            if parts:
                rating = parts[0]

        if not body and not title:
            continue

        reviews.append(
            {
                "title": title,
                "rating": rating,
                "body": body,
                "author": author,
                "date": date,
            }
        )

    return reviews


def scrape_jumia_reviews(product_url: str, pages: int = 1, delay_seconds: float = 1.0) -> List[Dict]:
    base_url = normalize_url(product_url)
    all_reviews: List[Dict] = []

    for page in range(1, pages + 1):
        params = {"page": page}
        html = fetch_page(base_url, params=params)
        page_reviews = parse_reviews(html)
        if not page_reviews:
            break
        all_reviews.extend(page_reviews)
        if delay_seconds and page < pages:
            time.sleep(delay_seconds)

    return all_reviews


def parse_category_product_urls(html: str, category_url: str) -> List[str]:
    soup = BeautifulSoup(html, "html.parser")
    urls: List[str] = []

    product_links = soup.select("a.core")
    if not product_links:
        product_links = soup.select("article a")

    base = category_url
    for a in product_links:
        href = a.get("href")
        if not href:
            continue
        full = urljoin(base, href)
        urls.append(normalize_url(full))

    return list(dict.fromkeys(urls))


def scrape_category(category_key: str, pages: int, review_pages: int, delay_seconds: float) -> List[Dict]:
    category_url = CATEGORY_URLS[category_key]
    all_rows: List[Dict] = []

    for page in range(1, pages + 1):
        params = {"page": page}
        html = fetch_page(category_url, params=params)
        product_urls = parse_category_product_urls(html, category_url)
        if not product_urls:
            break

        for product_url in product_urls:
            product_reviews = scrape_jumia_reviews(product_url, pages=review_pages, delay_seconds=delay_seconds)
            for r in product_reviews:
                row = dict(r)
                row["category"] = category_key
                row["product_url"] = product_url
                all_rows.append(row)

        if delay_seconds:
            time.sleep(delay_seconds)

    return all_rows


def scrape_multiple_categories(categories: Iterable[str], category_pages: int, review_pages: int, delay_seconds: float) -> List[Dict]:
    all_rows: List[Dict] = []
    for key in categories:
        if key not in CATEGORY_URLS:
            continue
        rows = scrape_category(key, pages=category_pages, review_pages=review_pages, delay_seconds=delay_seconds)
        all_rows.extend(rows)
    return all_rows


def save_reviews_to_csv(reviews: List[Dict], filename: str) -> None:
    if not reviews:
        return
    fieldnames = ["category", "product_url", "title", "rating", "body", "author", "date"]
    with open(filename, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(reviews)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--categories", nargs="*", default=list(CATEGORY_URLS.keys()))
    parser.add_argument("--category-pages", type=int, default=1)
    parser.add_argument("--review-pages", type=int, default=1)
    parser.add_argument("--output", default="jumia_reviews.csv")
    parser.add_argument("--delay", type=float, default=1.0)
    args = parser.parse_args()

    reviews = scrape_multiple_categories(
        categories=args.categories,
        category_pages=args.category_pages,
        review_pages=args.review_pages,
        delay_seconds=args.delay,
    )
    print(f"Fetched {len(reviews)} reviews")
    save_reviews_to_csv(reviews, args.output)
    print(f"Saved reviews to {args.output}")



if __name__ == "__main__":
    main()
