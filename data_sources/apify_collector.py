# -*- coding: utf-8 -*-
import pandas as pd
import streamlit as st
import time


class ApifyCollector:
    ROZETKA_CATEGORY_URLS = {
        "food": "https://rozetka.com.ua/supermarket/c4626923/",
        "home": "https://rozetka.com.ua/tovary-dlya-doma/c2394287/",
        "cosmetics": "https://rozetka.com.ua/kosmetika-i-parfyumeriya/c4629305/",
        "electronics": "https://rozetka.com.ua/telefony-tv-i-elektronika/c4627949/",
        "kids": "https://rozetka.com.ua/detskie-tovary/c88468/",
        "pets": "https://rozetka.com.ua/zootovar/c35974/",
        "energy": "https://rozetka.com.ua/search/?text=зарядна+станція",
        "health": "https://rozetka.com.ua/zdorove-i-sport/c4627858/",
    }

    def __init__(self, api_token=None):
        self.available = False
        self.client = None
        try:
            from apify_client import ApifyClient
            if api_token:
                self.client = ApifyClient(api_token)
                self.available = True
        except ImportError:
            st.warning("apify-client не встановлено")
        except Exception as e:
            st.warning(f"Apify: {e}")

    def get_rozetka_products(self, category_codes, max_per_category=50):
        if not self.client:
            return pd.DataFrame()

        all_products = []

        for code in category_codes:
            url = self.ROZETKA_CATEGORY_URLS.get(code)
            if not url:
                continue

            st.info(f"🛒 Rozetka [{code}]: збираємо до {max_per_category} товарів...")

            try:
                run_input = {
                    "startUrls": [{"url": url}],
                    "proxy": {"useApifyProxy": True},
                }

                run = self.client.actor("nazar/rozetka-category-scraper").call(
                    run_input=run_input,
                    timeout_secs=90,
                    memory_mbytes=256,
                )

                items = self.client.dataset(
                    run["defaultDatasetId"]
                ).list_items(limit=max_per_category).items

                count = 0
                for item in items:
                    parsed = self._safe_parse_rozetka(item, code)
                    if parsed:
                        all_products.append(parsed)
                        count += 1
                    if count >= max_per_category:
                        break

                st.success(f"✅ Rozetka [{code}]: отримано {count} товарів")

            except Exception as e:
                error_msg = str(e)
                if "timeout" in error_msg.lower():
                    st.warning(f"⏱ Rozetka [{code}]: таймаут, беремо що встигли зібрати")
                    try:
                        runs = self.client.actor("nazar/rozetka-category-scraper").runs().list(limit=1).items
                        if runs:
                            last_run = runs[0]
                            items = self.client.dataset(
                                last_run["defaultDatasetId"]
                            ).list_items(limit=max_per_category).items
                            for item in items[:max_per_category]:
                                parsed = self._safe_parse_rozetka(item, code)
                                if parsed:
                                    all_products.append(parsed)
                            st.info(f"📦 Rozetka [{code}]: взято {len(items)} з попереднього запуску")
                    except Exception:
                        pass
                else:
                    st.warning(f"⚠️ Rozetka [{code}]: {e}")

        return pd.DataFrame(all_products) if all_products else pd.DataFrame()

    def get_last_dataset(self, max_items=100):
        """Отримати дані з останнього запуску без нового збору."""
        if not self.client:
            return pd.DataFrame()

        try:
            runs = self.client.actor("nazar/rozetka-category-scraper").runs().list(limit=1).items
            if not runs:
                st.warning("Немає попередніх запусків Rozetka")
                return pd.DataFrame()

            last_run = runs[0]
            st.info(f"📦 Завантажуємо з останнього запуску ({last_run.get('startedAt', '?')})...")

            items = self.client.dataset(
                last_run["defaultDatasetId"]
            ).list_items(limit=max_items).items

            products = []
            for item in items:
                parsed = self._safe_parse_rozetka(item, "mixed")
                if parsed:
                    products.append(parsed)

            st.success(f"✅ Завантажено {len(products)} товарів з кешу Apify")
            return pd.DataFrame(products) if products else pd.DataFrame()

        except Exception as e:
            st.warning(f"⚠️ Помилка завантаження: {e}")
            return pd.DataFrame()

    def _safe_parse_rozetka(self, item, category_code):
        try:
            name = item.get("name") or item.get("title") or "N/A"

            price_raw = item.get("price", 0)
            if isinstance(price_raw, dict):
                price = self._safe_float(price_raw.get("current", 0))
                old_price = self._safe_float(price_raw.get("old", 0))
            else:
                price = self._safe_float(price_raw)
                old_price = self._safe_float(item.get("old_price", 0))

            reviews_raw = item.get("reviews", {})
            if isinstance(reviews_raw, dict):
                rating = self._safe_float(reviews_raw.get("rating", 0))
                reviews_count = self._safe_int(reviews_raw.get("count", 0))
            else:
                rating = self._safe_float(item.get("rating", 0))
                reviews_count = self._safe_int(item.get("reviews_count", 0))

            brand_raw = item.get("brand", "")
            if isinstance(brand_raw, dict):
                brand = brand_raw.get("name", "")
            else:
                brand = str(brand_raw or "")

            seller_raw = item.get("seller", "")
            if isinstance(seller_raw, dict):
                seller = seller_raw.get("name", "")
            else:
                seller = str(seller_raw or "")

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
                "url": item.get("url", ""),
                "image": item.get("image", ""),
                "brand": brand,
                "seller": seller,
            }
        except Exception:
            return None

    def _safe_float(self, val):
        try:
            if val is None:
                return 0.0
            if isinstance(val, (int, float)):
                return float(val)
            cleaned = str(val).replace(" ", "").replace(",", ".")
            return float(cleaned) if cleaned else 0.0
        except (ValueError, TypeError):
            return 0.0

    def _safe_int(self, val):
        try:
            if val is None:
                return 0
            if isinstance(val, int):
                return val
            return int(float(str(val).replace(" ", "")))
        except (ValueError, TypeError):
            return 0

    def get_google_trends(self, category_codes, geo="UA", timeframe="today 1-m"):
        return pd.DataFrame()

    def get_prom_products(self, category_codes, max_per_category=50):
        return pd.DataFrame()
