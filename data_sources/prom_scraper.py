import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import json
import streamlit as st


class PromScraper:

    BASE_URL = "https://prom.ua"

    SEARCH_TERMS = {
        "food": "продукти харчування",
        "home": "товари для дому",
        "cosmetics": "косметика",
        "electronics": "ноутбук",
        "kids": "дитячі товари",
        "pets": "корм для тварин",
        "energy": "зарядна станція",
        "health": "вітаміни",
    }

    HEADERS = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/125.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml",
        "Accept-Language": "uk-UA,uk;q=0.9",
    }

    def get_top_products(self, category_codes, max_per_category=25):
        all_products = []

        for code in category_codes:
            term = self.SEARCH_TERMS.get(code, code)
            url = "{}/ua/search?search_term={}".format(self.BASE_URL, term)

            try:
                st.text("Prom.ua: poshuk [{}]...".format(term))
                resp = requests.get(url, headers=self.HEADERS, timeout=15)
                resp.raise_for_status()
                soup = BeautifulSoup(resp.text, "lxml")

                products = self._parse_jsonld(soup, code)
                if products:
                    all_products.extend(products[:max_per_category])
                    st.text("Prom.ua [{}]: {} tovariv".format(code, len(products)))
                    time.sleep(2)
                    continue

                products = self._parse_html(soup, code, max_per_category)
                if products:
                    all_products.extend(products)
                    st.text("Prom.ua [{}]: {} tovariv".format(code, len(products)))
                else:
                    st.info("Prom.ua [{}]: dani nedostupni (JS rendering)".format(code))

                time.sleep(2)

            except Exception as e:
                st.warning("Prom.ua [{}]: {}".format(code, e))

        return pd.DataFrame(all_products) if all_products else pd.DataFrame()

    def _parse_jsonld(self, soup, code):
        products = []
        for script in soup.find_all("script", type="application/ld+json"):
            try:
                raw = script.string
                if not raw:
                    continue

                data = json.loads(raw)

                # data can be dict, list, or something else
                items_to_process = []

                if isinstance(data, dict):
                    items_to_process = data.get("itemListElement", [])
                elif isinstance(data, list):
                    for d in data:
                        if isinstance(d, dict):
                            items_to_process.extend(d.get("itemListElement", []))

                for el in items_to_process:
                    # el might be a dict or something else
                    if not isinstance(el, dict):
                        continue

                    # item_data might be nested under "item" key or be el itself
                    item_data = el.get("item", el)

                    # item_data must be a dict
                    if not isinstance(item_data, dict):
                        continue

                    name = item_data.get("name", "")
                    if not name:
                        continue

                    price = 0
                    offers = item_data.get("offers", {})
                    if isinstance(offers, dict):
                        price_raw = offers.get("price", 0)
                        try:
                            price = float(price_raw)
                        except (ValueError, TypeError):
                            price = 0
                    elif isinstance(offers, list) and len(offers) > 0:
                        first_offer = offers[0]
                        if isinstance(first_offer, dict):
                            try:
                                price = float(first_offer.get("price", 0))
                            except (ValueError, TypeError):
                                price = 0

                    products.append({
                        "name": str(name),
                        "price": price,
                        "rating": 0,
                        "reviews_count": 0,
                        "category": code,
                        "source": "prom.ua",
                        "url": str(item_data.get("url", "")),
                    })

            except (json.JSONDecodeError, TypeError, ValueError):
                continue

        return products

    def _parse_html(self, soup, code, limit):
        products = []
        selectors = [
            "[data-qaid='product_block']",
            "[data-qaid='product-item']",
            "div[class*='product_u']",
            "li[class*='catalog']",
            "div[data-product-id]",
        ]

        cards = []
        for sel in selectors:
            cards = soup.select(sel)
            if cards:
                break

        for card in cards[:limit]:
            try:
                name = None
                name_selectors = [
                    "[data-qaid='product_name']",
                    "[data-qaid='product_link']",
                    "a[title]",
                    "span[class*='name']",
                ]
                for ns in name_selectors:
                    el = card.select_one(ns)
                    if el:
                        name = el.get_text(strip=True) or el.get("title", "")
                        if name:
                            break

                if not name:
                    continue

                price = 0
                price_selectors = [
                    "[data-qaid='product_price']",
                    "span[class*='price']",
                ]
                for ps in price_selectors:
                    el = card.select_one(ps)
                    if el:
                        txt = el.get_text(strip=True)
                        digits = "".join(c for c in txt if c.isdigit() or c == ".")
                        if digits:
                            try:
                                price = float(digits)
                            except ValueError:
                                price = 0
                            break

                products.append({
                    "name": name,
                    "price": price,
                    "rating": 0,
                    "reviews_count": 0,
                    "category": code,
                    "source": "prom.ua",
                    "url": "",
                })
            except Exception:
                continue

        return products
