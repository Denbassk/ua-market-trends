# -*- coding: utf-8 -*-
import pandas as pd
import streamlit as st


class ApifyCollector:
    """Collector for Rozetka products via Apify."""

    ROZETKA_CATEGORY_URLS = {
        "food":        "https://rozetka.com.ua/ua/alkoholnie-napitki-i-produkty/c4626923/",
        "alcohol":     "https://rozetka.com.ua/ua/alkogolnie-napitki-konfety/c4594201/",
        "grocery":     "https://rozetka.com.ua/ua/produkty/c4624997/",
        "chemistry":   "https://rozetka.com.ua/ua/bytovaya-himiya/c4429255/",
        "home":        "https://rozetka.com.ua/ua/tovary-dlya-doma/c2394287/",
        "cosmetics":   "https://rozetka.com.ua/ua/kosmetika-i-parfyumeriya/c4629305/",
        "electronics": "https://rozetka.com.ua/ua/telefony-tv-i-elektronika/c4627949/",
        "kids":        "https://rozetka.com.ua/ua/detskie-tovary/c88468/",
        "pets":        "https://rozetka.com.ua/ua/zootovar/c35974/",
        "health":      "https://rozetka.com.ua/ua/zdorove-i-sport/c4627858/",
        "energy":      "https://rozetka.com.ua/search/?text=зарядна+станція",
    }

    # Человекочитаемые названия для UI
    CATEGORY_LABELS = {
        "food":        "🍽 Їжа та алкоголь (все)",
        "alcohol":     "🥃 Алкогольні напої",
        "grocery":     "🛒 Продукти харчування",
        "chemistry":   "🧴 Побутова хімія",
        "home":        "🏠 Товари для дому",
        "cosmetics":   "💄 Косметика",
        "electronics": "📱 Електроніка",
        "kids":        "👶 Дитячі товари",
        "pets":        "🐾 Зоотовари",
        "health":      "💊 Здоров'я та спорт",
        "energy":      "🔋 Зарядні станції",
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

    # =========================================================
    # ROZETKA — новий збір
    # =========================================================
    def get_rozetka_products(self, category_codes, max_per_category=50):
        """Launch Apify actor for each category, limit results."""
        if not self.client:
            return pd.DataFrame()

        all_products = []

        for code in category_codes:
            url = self.ROZETKA_CATEGORY_URLS.get(code)
            if not url:
                st.warning(f"⚠️ Невідома категорія: {code}")
                continue

            label = self.CATEGORY_LABELS.get(code, code)
            st.info(f"🛒 Rozetka [{label}]: збираємо до {max_per_category} товарів...")

            try:
                run_input = {
                    "startUrls": [{"url": url}],
                    "proxy": {"useApifyProxy": True},
                }

                run = self.client.actor("nazar/rozetka-category-scraper").call(
                    run_input=run_input,
                    timeout_secs=max(60, int(max_per_category / 50 * 30)),
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

                st.success(f"✅ Rozetka [{label}]: отримано {count} товарів")

            except Exception as e:
                error_msg = str(e).lower()
                if "timeout" in error_msg:
                    st.warning(f"⏱ Rozetka [{label}]: таймаут — беремо що встигли")
                    try:
                        runs = self.client.actor(
                            "nazar/rozetka-category-scraper"
                        ).runs().list(limit=1).items
                        if runs:
                            items = self.client.dataset(
                                runs[0]["defaultDatasetId"]
                            ).list_items(limit=max_per_category).items
                            for item in items[:max_per_category]:
                                parsed = self._safe_parse_rozetka(item, code)
                                if parsed:
                                    all_products.append(parsed)
                    except Exception:
                        pass
                else:
                    st.warning(f"⚠️ Rozetka [{label}]: {e}")

        return pd.DataFrame(all_products) if all_products else pd.DataFrame()

    # =========================================================
    # ROZETKA — завантажити з кешу (без витрат)
    # =========================================================
    def get_last_dataset(self, max_items=2500):
        """Load products from last Apify run without new charges."""
        if not self.client:
            return pd.DataFrame()

        try:
            runs = self.client.actor(
                "nazar/rozetka-category-scraper"
            ).runs().list(limit=1).items
            if not runs:
                st.warning("Немає попередніх запусків Rozetka")
                return pd.DataFrame()

            last_run = runs[0]
            started = last_run.get("startedAt", "?")
            st.info(f"📦 Завантажуємо з останнього запуску ({started})...")

            items = self.client.dataset(
                last_run["defaultDatasetId"]
            ).list_items(limit=max_items).items

            products = []
            for item in items:
                # Визначаємо категорію автоматично з даних Rozetka
                detected_code = self._detect_category(item)
                parsed = self._safe_parse_rozetka(item, detected_code)
                if parsed:
                    products.append(parsed)

            st.success(f"✅ Завантажено {len(products)} товарів з кешу Apify")
            return pd.DataFrame(products) if products else pd.DataFrame()

        except Exception as e:
            st.warning(f"⚠️ Помилка завантаження: {e}")
            return pd.DataFrame()

    # =========================================================
    # Визначення категорії з полів Rozetka
    # =========================================================
    def _detect_category(self, item):
        """Determine internal category code from Rozetka data fields."""
        cat_raw = item.get("category", {})
        if not isinstance(cat_raw, dict):
            return "other"

        root = (cat_raw.get("root") or "").lower()
        name = (cat_raw.get("name") or "").lower()
        combined = root + " " + name

        # Маппинг ключових слів → внутрішній код
        mapping = [
            (["алкоголь", "віскі", "горілка", "вино", "коньяк",
              "пиво", "лікер", "текіла", "ром ", "джин",
              "абсент", "бренді", "шампанськ", "ігрист"], "alcohol"),
            (["продукт", "харчув", "їжа", "бакал", "молоч",
              "м'яс", "ковбас", "хліб", "крупа", "олія",
              "конди", "солодощ", "снек", "чай", "кав",
              "напої безалкогольні", "консерв", "соус",
              "спеції", "приправ", "макарон", "цукор"], "grocery"),
            (["хіміч", "хімія", "миюч", "пральн", "прибиранн",
              "побутова хімія"], "chemistry"),
            (["дім", "дома", "домаш", "меблі", "текстиль",
              "посуд", "інтер'єр", "ліжк", "матрац", "рушник",
              "футон", "топер", "подушк"], "home"),
            (["косметик", "парфум", "макіяж", "догляд за шкір",
              "шампунь", "гель для душ"], "cosmetics"),
            (["електрон", "телефон", "ноутбук", "планшет",
              "телевізор", "аудіо", "фото", "відео",
              "комп'ют", "гаджет"], "electronics"),
            (["дитяч", "діт", "дитин", "іграшк", "коляск",
              "малюк"], "kids"),
            (["зоо", "тварин", "корм для", "собак", "кішок",
              "акваріум", "гризун"], "pets"),
            (["здоров", "спорт", "фітнес", "медич", "аптек",
              "вітамін"], "health"),
        ]

        for keywords, code in mapping:
            for kw in keywords:
                if kw in combined:
                    return code

        # Якщо root містить "Алкогольні напої та продукти"
        if "алкогольні напої та продукт" in combined:
            return "food"

        return "other"

    # =========================================================
    # Парсинг одного товару
    # =========================================================
    def _safe_parse_rozetka(self, item, category_code):
        """Parse a single Rozetka product item into a dict."""
        try:
            name = item.get("name") or item.get("title") or "N/A"

            # Price
            price_raw = item.get("price", 0)
            if isinstance(price_raw, dict):
                price = self._safe_float(price_raw.get("current", 0))
                old_price = self._safe_float(price_raw.get("old", 0))
            else:
                price = self._safe_float(price_raw)
                old_price = self._safe_float(item.get("old_price", 0))

            # Reviews
            reviews_raw = item.get("reviews", {})
            if isinstance(reviews_raw, dict):
                rating = self._safe_float(reviews_raw.get("rating", 0))
                reviews_count = self._safe_int(reviews_raw.get("count", 0))
            else:
                rating = self._safe_float(item.get("rating", 0))
                reviews_count = self._safe_int(item.get("reviews_count", 0))

            # Brand
            brand_raw = item.get("brand", "")
            brand = brand_raw.get("name", "") if isinstance(brand_raw, dict) else str(brand_raw or "")

            # Seller
            seller_raw = item.get("seller", "")
            seller = seller_raw.get("name", "") if isinstance(seller_raw, dict) else str(seller_raw or "")

            # Rozetka category label
            cat_raw = item.get("category", {})
            rozetka_category = ""
            if isinstance(cat_raw, dict):
                parts = []
                if cat_raw.get("root"):
                    parts.append(cat_raw["root"])
                if cat_raw.get("name"):
                    parts.append(cat_raw["name"])
                rozetka_category = " → ".join(parts)

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

    # =========================================================
    # Helpers
    # =========================================================
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

    # =========================================================
    # Stubs for Google Trends / Prom
    # =========================================================
    def get_google_trends(self, category_codes, geo="UA", timeframe="today 1-m"):
        return pd.DataFrame()

    def get_prom_products(self, category_codes, max_per_category=50):
        return pd.DataFrame()
