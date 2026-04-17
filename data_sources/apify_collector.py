# -*- coding: utf-8 -*-
import pandas as pd
import streamlit as st
import time
import streamlit as st
import random

class ApifyKeyRotator:
    """Rotates through multiple Apify API keys."""

    def __init__(self):
        self.keys = list(st.secrets.get("APIFY_KEYS", []))
        self.failed_keys = set()

    def get_working_key(self):
        """Returns a working API key or None."""
        available = [k for k in self.keys if k not in self.failed_keys]
        if not available:
            # Reset and try all again
            self.failed_keys.clear()
            available = self.keys
        if not available:
            return None
        return random.choice(available)

    def mark_failed(self, key):
        """Mark a key as exhausted/failed."""
        self.failed_keys.add(key)
        remaining = len(self.keys) - len(self.failed_keys)
        st.warning(f"\u26a0\ufe0f \u041a\u043b\u044e\u0447 \u0432\u0438\u0447\u0435\u0440\u043f\u0430\u043d\u043e. \u0417\u0430\u043b\u0438\u0448\u0438\u043b\u043e\u0441\u044c: {remaining}")


def get_apify_client():
    """Get ApifyClient with automatic key rotation."""
    from apify_client import ApifyClient

    rotator = ApifyKeyRotator()

    for attempt in range(len(rotator.keys)):
        key = rotator.get_working_key()
        if not key:
            st.error("\u0412\u0441\u0456 API-\u043a\u043b\u044e\u0447\u0456 \u0432\u0438\u0447\u0435\u0440\u043f\u0430\u043d\u043e!")
            return None

        client = ApifyClient(key)
        try:
            # Test the key
            user = client.user().get()
            if user:
                plan = user.get("plan", {})
                usage = plan.get("usageTotal", {}).get("value", 0)
                limit = plan.get("limit", {}).get("value", 5)
                st.info(f"\U0001f511 \u0410\u043a\u0430\u0443\u043d\u0442: {user.get('username', '?')} | "
                        f"\u0412\u0438\u043a\u043e\u0440\u0438\u0441\u0442\u0430\u043d\u043e: ${usage:.2f} / ${limit:.2f}")
                if usage >= limit * 0.95:
                    rotator.mark_failed(key)
                    continue
                return client
        except Exception:
            rotator.mark_failed(key)
            continue

    return None



class ApifyCollector:
    def __init__(self, api_token=None):
        if api_token:
            from apify_client import ApifyClient
            self.client = ApifyClient(api_token)
        else:
            self.client = get_apify_client()

    ROZETKA_CATEGORY_URLS = {
        "food": "https://rozetka.com.ua/supermarket/c4626923/",
        "home": "https://rozetka.com.ua/tovary-dlya-doma/c2394287/",
        "cosmetics": "https://rozetka.com.ua/kosmetika-i-parfyumeriya/c4629305/",
        "electronics": "https://rozetka.com.ua/telefony-tv-i-elektronika/c4627949/",
        "kids": "https://rozetka.com.ua/detskie-tovary/c88468/",
        "pets": "https://rozetka.com.ua/zootovar/c35974/",
        "energy": "https://rozetka.com.ua/search/?text=\u0437\u0430\u0440\u044f\u0434\u043d\u0430+\u0441\u0442\u0430\u043d\u0446\u0456\u044f",
        "health": "https://rozetka.com.ua/zdorove-i-sport/c4627858/",
    }

    def __init__(self, api_token):
        self.api_token = api_token
        self.client = None
        try:
            from apify_client import ApifyClient
            self.client = ApifyClient(api_token)
            self.available = True
        except ImportError:
            st.error("apify-client \u043d\u0435 \u0432\u0441\u0442\u0430\u043d\u043e\u0432\u043b\u0435\u043d\u043e. \u0412\u0438\u043a\u043e\u043d\u0430\u0439\u0442\u0435: pip install apify-client")
            self.available = False
        except Exception as e:
            st.error("Apify \u043f\u043e\u043c\u0438\u043b\u043a\u0430: {}".format(e))
            self.available = False

    # ===========================================
    # ROZETKA
    # ===========================================
def get_rozetka_products(self, category_codes, max_per_category=50):
    """Fetch products from Rozetka via Apify actor."""
    all_products = []

    for code in category_codes:
        url = self.ROZETKA_CATEGORY_URLS.get(code)
        if not url:
            continue

        st.info(f"\U0001f6d2 Rozetka [{code}]: \u0437\u0431\u0438\u0440\u0430\u0454\u043c\u043e \u0434\u043e {max_per_category} \u0442\u043e\u0432\u0430\u0440\u0456\u0432...")

        try:
            run_input = {
                "startUrls": [{"url": url}],
                "maxItems": max_per_category,
                "proxy": {"useApifyProxy": True},
            }

            run = self.client.actor("nazar/rozetka-category-scraper").call(
                run_input=run_input,
                timeout_secs=300,
            )

            dataset_items = self.client.dataset(
                run["defaultDatasetId"]
            ).list_items().items

            st.success(
                f"\u2705 Rozetka [{code}]: "
                f"\u043e\u0442\u0440\u0438\u043c\u0430\u043d\u043e {len(dataset_items)} \u0442\u043e\u0432\u0430\u0440\u0456\u0432"
            )

            for item in dataset_items:
                parsed = self._safe_parse_rozetka(item, code)
                if parsed:
                    all_products.append(parsed)

        except Exception as e:
            st.warning(f"\u26a0\ufe0f Rozetka [{code}]: {e}")

    return pd.DataFrame(all_products) if all_products else pd.DataFrame()

def _safe_parse_rozetka(self, item, category_code):
    """Parse one Rozetka product from Apify dataset."""
    try:
        # --- name ---
        name = item.get("name") or item.get("title") or "N/A"

        # --- price (nested object or flat) ---
        price_raw = item.get("price", 0)
        if isinstance(price_raw, dict):
            price = float(price_raw.get("current", 0) or 0)
            old_price = float(price_raw.get("old", 0) or 0)
        else:
            price = float(price_raw or 0)
            old_price = float(item.get("old_price", 0) or 0)

        # --- reviews (nested object or flat) ---
        reviews_raw = item.get("reviews", {})
        if isinstance(reviews_raw, dict):
            rating = float(reviews_raw.get("rating", 0) or 0)
            reviews_count = int(reviews_raw.get("count", 0) or 0)
        else:
            rating = float(item.get("rating", 0) or 0)
            reviews_count = int(item.get("reviews_count", 0) or item.get("reviewsCount", 0) or 0)

        # --- brand (nested object or flat) ---
        brand_raw = item.get("brand", "")
        if isinstance(brand_raw, dict):
            brand = brand_raw.get("name", "")
        else:
            brand = str(brand_raw or "")

        # --- seller (nested object or flat) ---
        seller_raw = item.get("seller", "")
        if isinstance(seller_raw, dict):
            seller = seller_raw.get("name", "")
        else:
            seller = str(seller_raw or "")

        # --- url & image ---
        url = item.get("url", "")
        image = item.get("image", "")

        # --- category from Rozetka data ---
        cat_raw = item.get("category", {})
        rozetka_category = ""
        if isinstance(cat_raw, dict):
            rozetka_category = cat_raw.get("name", "")

        return {
            "name": name,
            "price": price,
            "old_price": old_price,
            "rating": rating,
            "reviews_count": reviews_count,
            "category": category_code,
            "rozetka_category": rozetka_category,
            "source": "rozetka (apify)",
            "url": url,
            "image": image,
            "brand": brand,
            "seller": seller,
        }
    except Exception as e:
        return {
            "name": str(item.get("name", "parse error")),
            "price": 0, "old_price": 0,
            "rating": 0, "reviews_count": 0,
            "category": category_code,
            "rozetka_category": "",
            "source": "rozetka (apify)",
            "url": "", "image": "",
            "brand": "", "seller": "",
        }

    def _safe_float(self, val):
        if val is None:
            return 0.0
        if isinstance(val, (int, float)):
            return float(val)
        if isinstance(val, str):
            clean = val.replace(" ", "").replace(",", ".")
            try:
                return float(clean)
            except ValueError:
                return 0.0
        if isinstance(val, dict):
            for k in ["value", "amount", "price", "current"]:
                if k in val:
                    return self._safe_float(val[k])
            return 0.0
        if isinstance(val, list):
            if val:
                return self._safe_float(val[0])
            return 0.0
        return 0.0

    def _safe_int(self, val):
        if val is None:
            return 0
        if isinstance(val, (int, float)):
            return int(val)
        if isinstance(val, str):
            clean = "".join(c for c in val if c.isdigit())
            try:
                return int(clean) if clean else 0
            except ValueError:
                return 0
        if isinstance(val, dict):
            for k in ["value", "count", "amount"]:
                if k in val:
                    return self._safe_int(val[k])
            return 0
        return 0

    # ===========================================
    # GOOGLE TRENDS - skip Apify, use local
    # ===========================================
    def get_google_trends(self, category_codes, geo="UA", timeframe="today 1-m"):
        # Google Trends through Apify is too slow and expensive
        # Return empty to fall back to local method
        st.info(
            "\u2139\ufe0f Google Trends \u0447\u0435\u0440\u0435\u0437 Apify "
            "\u0432\u0438\u043c\u043a\u043d\u0435\u043d\u043e (\u043f\u043e\u0432\u0456\u043b\u044c\u043d\u043e "
            "\u0442\u0430 \u0434\u043e\u0440\u043e\u0433\u043e). "
            "\u0412\u0438\u043a\u043e\u0440\u0438\u0441\u0442\u043e\u0432\u0443\u0454\u0442\u044c\u0441\u044f "
            "\u043b\u043e\u043a\u0430\u043b\u044c\u043d\u0438\u0439 \u043c\u0435\u0442\u043e\u0434."
        )
        return pd.DataFrame()

    # ===========================================
    # PROM.UA
    # ===========================================
    def get_prom_products(self, category_codes, max_per_category=50):
        # Prom.ua through Apify generic scraper
        if not self.available:
            return pd.DataFrame()

        all_products = []
        search_terms = {
            "food": "\u043f\u0440\u043e\u0434\u0443\u043a\u0442\u0438 \u0445\u0430\u0440\u0447\u0443\u0432\u0430\u043d\u043d\u044f",
            "home": "\u0442\u043e\u0432\u0430\u0440\u0438 \u0434\u043b\u044f \u0434\u043e\u043c\u0443",
            "cosmetics": "\u043a\u043e\u0441\u043c\u0435\u0442\u0438\u043a\u0430",
            "electronics": "\u0435\u043b\u0435\u043a\u0442\u0440\u043e\u043d\u0456\u043a\u0430",
            "kids": "\u0434\u0438\u0442\u044f\u0447\u0456 \u0442\u043e\u0432\u0430\u0440\u0438",
            "pets": "\u0437\u043e\u043e\u0442\u043e\u0432\u0430\u0440\u0438",
            "energy": "\u0437\u0430\u0440\u044f\u0434\u043d\u0430 \u0441\u0442\u0430\u043d\u0446\u0456\u044f",
            "health": "\u0432\u0456\u0442\u0430\u043c\u0456\u043d\u0438",
        }

        for code in category_codes:
            term = search_terms.get(code, code)
            url = "https://prom.ua/ua/search?search_term={}".format(term)

            try:
                st.text("\U0001f6d2 Apify: Prom.ua [{}]...".format(code))

                run = self.client.actor("apify/cheerio-scraper").call(
                    run_input={
                        "startUrls": [{"url": url}],
                        "maxRequestsPerCrawl": 1,
                        "pageFunction": """
                            async function pageFunction(context) {
                                const { $, request } = context;
                                const results = [];
                                $('[data-qaid="product_block"], [data-product-id]').each((i, el) => {
                                    const name = $(el).find('[data-qaid="product_name"], a[title]').first().text().trim() || $(el).find('a[title]').first().attr('title') || '';
                                    const priceText = $(el).find('[data-qaid="product_price"], span[class*="price"]').first().text().trim();
                                    const price = parseFloat(priceText.replace(/[^0-9.]/g, '')) || 0;
                                    if (name) results.push({ name, price });
                                });
                                return results;
                            }
                        """,
                        "proxy": {"useApifyProxy": True},
                    },
                    timeout_secs=60,
                )

                items = list(
                    self.client.dataset(run["defaultDatasetId"]).iterate_items()
                )

                for item in items:
                    if isinstance(item, list):
                        for p in item[:max_per_category]:
                            if isinstance(p, dict) and p.get("name"):
                                all_products.append({
                                    "name": p["name"],
                                    "price": self._safe_float(p.get("price", 0)),
                                    "old_price": 0,
                                    "rating": 0,
                                    "reviews_count": 0,
                                    "category": code,
                                    "source": "prom.ua (apify live)",
                                    "url": "",
                                    "brand": "",
                                    "seller": "",
                                })
                    elif isinstance(item, dict) and item.get("name"):
                        all_products.append({
                            "name": item["name"],
                            "price": self._safe_float(item.get("price", 0)),
                            "old_price": 0,
                            "rating": 0,
                            "reviews_count": 0,
                            "category": code,
                            "source": "prom.ua (apify live)",
                            "url": "",
                            "brand": "",
                            "seller": "",
                        })

                st.text("\u2705 Prom.ua [{}]: {} \u0442\u043e\u0432\u0430\u0440\u0456\u0432".format(
                    code, len(all_products)
                ))

            except Exception as e:
                st.warning("\u26a0\ufe0f Apify Prom.ua [{}]: {}".format(code, e))

        return pd.DataFrame(all_products) if all_products else pd.DataFrame()
