# -*- coding: utf-8 -*-
import pandas as pd
import numpy as np


class TrendAnalyzer:
    """Аналіз трендів товарів та пошукових запитів."""

    # =============================================================
    # TREND SCORE — головна метрика трендовості товару
    # =============================================================
    def calculate_trend_scores(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Додає колонку trend_score до DataFrame товарів.

        Формула:
          trend_score = popularity × quality × discount_boost

        Де:
          popularity  = norm(reviews_count)        — скільки людей купили
          quality     = rating / 5                  — наскільки задоволені
          discount_boost = old_price / price         — якщо є знижка, магазин просуває товар

        Результат нормалізується від 0 до 100.
        """
        if df.empty:
            return df

        result = df.copy()

        # Безпечне приведення типів
        result["price"] = pd.to_numeric(result.get("price", 0), errors="coerce").fillna(0)
        result["old_price"] = pd.to_numeric(result.get("old_price", 0), errors="coerce").fillna(0)
        result["rating"] = pd.to_numeric(result.get("rating", 0), errors="coerce").fillna(0)
        result["reviews_count"] = pd.to_numeric(result.get("reviews_count", 0), errors="coerce").fillna(0)

        # --- Popularity (log scale, бо розподіл дуже нерівномірний) ---
        rc = result["reviews_count"].clip(lower=0)
        rc_log = np.log1p(rc)  # log(1 + x)
        rc_max = rc_log.max()
        result["_popularity"] = (rc_log / rc_max * 100) if rc_max > 0 else 0

        # --- Quality ---
        result["_quality"] = (result["rating"] / 5.0 * 100).clip(0, 100)

        # --- Discount boost (1.0 = нема знижки, до 2.0 = велика знижка) ---
        result["_discount"] = np.where(
            (result["old_price"] > result["price"]) & (result["price"] > 0),
            (result["old_price"] / result["price"]).clip(upper=2.0),
            1.0
        )

        # --- Фінальний score ---
        raw_score = (
            result["_popularity"] * 0.50 +
            result["_quality"] * 0.30 +
            (result["_discount"] - 1.0) * 100 * 0.20  # 0-20 балів за знижку
        )

        # Нормалізація 0-100
        score_min = raw_score.min()
        score_max = raw_score.max()
        if score_max > score_min:
            result["trend_score"] = ((raw_score - score_min) / (score_max - score_min) * 100).round(1)
        else:
            result["trend_score"] = 50.0

        # Текстовий лейбл
        result["trend_label"] = pd.cut(
            result["trend_score"],
            bins=[-1, 25, 50, 75, 100],
            labels=["🔵 Низький", "🟡 Середній", "🟠 Високий", "🔴 Гарячий"]
        )

        # Прибираємо тимчасові колонки
        result.drop(columns=["_popularity", "_quality", "_discount"], inplace=True, errors="ignore")

        return result.sort_values("trend_score", ascending=False)

    # =============================================================
    # ТОП ТРЕНДОВИХ ТОВАРІВ
    # =============================================================
    def get_top_trending(self, df: pd.DataFrame, n: int = 20) -> pd.DataFrame:
        """Повертає топ-N товарів за trend_score."""
        scored = self.calculate_trend_scores(df)
        return scored.head(n)

    # =============================================================
    # ТРЕНДОВІ БРЕНДИ
    # =============================================================
    def get_trending_brands(self, df: pd.DataFrame, top_n: int = 15) -> pd.DataFrame:
        """Які бренди зараз найпопулярніші."""
        scored = self.calculate_trend_scores(df)

        if "brand" not in scored.columns:
            return pd.DataFrame()

        brands = scored[scored["brand"].notna() & (scored["brand"] != "")].copy()
        if brands.empty:
            return pd.DataFrame()

        result = brands.groupby("brand").agg(
            avg_score=("trend_score", "mean"),
            total_reviews=("reviews_count", "sum"),
            avg_rating=("rating", "mean"),
            products_count=("name", "count"),
            avg_price=("price", "mean"),
        ).round(1)

        result = result.sort_values("avg_score", ascending=False).head(top_n)
        result = result.reset_index()
        return result

    # =============================================================
    # ТРЕНДОВІ КАТЕГОРІЇ
    # =============================================================
    def get_trending_categories(self, df: pd.DataFrame) -> pd.DataFrame:
        """Які категорії Rozetka зараз найпопулярніші."""
        scored = self.calculate_trend_scores(df)

        cat_col = "rozetka_category" if "rozetka_category" in scored.columns else "category"
        cats = scored[scored[cat_col].notna() & (scored[cat_col] != "")].copy()
        if cats.empty:
            return pd.DataFrame()

        result = cats.groupby(cat_col).agg(
            avg_score=("trend_score", "mean"),
            total_reviews=("reviews_count", "sum"),
            avg_rating=("rating", "mean"),
            products_count=("name", "count"),
            avg_price=("price", "mean"),
            max_discount_pct=("old_price", lambda x: 0),  # placeholder
        ).round(1)

        # Реальний розрахунок знижки
        for cat_name in result.index:
            cat_df = cats[cats[cat_col] == cat_name]
            has_discount = cat_df[(cat_df["old_price"] > cat_df["price"]) & (cat_df["price"] > 0)]
            if not has_discount.empty:
                discount_pct = ((has_discount["old_price"] - has_discount["price"]) / has_discount["old_price"] * 100)
                result.loc[cat_name, "max_discount_pct"] = round(discount_pct.mean(), 1)

        result = result.sort_values("avg_score", ascending=False)
        result = result.reset_index()
        result.rename(columns={cat_col: "category_name"}, inplace=True)
        return result

    # =============================================================
    # ЦІНОВІ АНОМАЛІЇ (великі знижки = магазин просуває)
    # =============================================================
    def get_price_drops(self, df: pd.DataFrame, min_discount_pct: float = 20) -> pd.DataFrame:
        """Товари з великими знижками — сигнал що магазин їх просуває."""
        if df.empty:
            return pd.DataFrame()

        result = df.copy()
        result["price"] = pd.to_numeric(result.get("price", 0), errors="coerce").fillna(0)
        result["old_price"] = pd.to_numeric(result.get("old_price", 0), errors="coerce").fillna(0)

        has_discount = result[(result["old_price"] > result["price"]) & (result["price"] > 0)].copy()
        if has_discount.empty:
            return pd.DataFrame()

        has_discount["discount_pct"] = (
            (has_discount["old_price"] - has_discount["price"]) / has_discount["old_price"] * 100
        ).round(1)

        big_drops = has_discount[has_discount["discount_pct"] >= min_discount_pct]
        return big_drops.sort_values("discount_pct", ascending=False)

    # =============================================================
    # GOOGLE TRENDS — аналіз пошукових запитів
    # =============================================================
    def count_rising(self, gt_data: pd.DataFrame) -> int:
        if gt_data.empty or len(gt_data) < 4:
            return 0
        count = 0
        quarter = max(len(gt_data) // 4, 1)
        for col in gt_data.columns:
            first_q = gt_data[col].iloc[:quarter].mean()
            last_q = gt_data[col].iloc[-quarter:].mean()
            if first_q > 0 and last_q > first_q * 1.1:
                count += 1
            elif first_q == 0 and last_q > 5:
                count += 1
        return count

    def get_rising_trends(self, gt_data: pd.DataFrame) -> pd.DataFrame:
        if gt_data.empty or len(gt_data) < 4:
            return pd.DataFrame()

        results = []
        quarter = max(len(gt_data) // 4, 1)
        for col in gt_data.columns:
            first_q = gt_data[col].iloc[:quarter].mean()
            last_q = gt_data[col].iloc[-quarter:].mean()
            if first_q > 0:
                growth = ((last_q - first_q) / first_q) * 100
            elif last_q > 0:
                growth = 100.0
            else:
                growth = 0.0

            trend_direction = "📈 Зростає" if growth > 10 else (
                "📉 Спадає" if growth < -10 else "➡️ Стабільно"
            )
            volatility = gt_data[col].std()
            results.append({
                "keyword": col,
                "first_quarter_avg": round(first_q, 1),
                "last_quarter_avg": round(last_q, 1),
                "growth_pct": round(growth, 1),
                "trend": trend_direction,
                "volatility": round(volatility, 1),
                "peak_value": int(gt_data[col].max()),
                "peak_date": gt_data[col].idxmax().strftime("%d.%m.%Y")
                    if hasattr(gt_data[col].idxmax(), "strftime") else str(gt_data[col].idxmax()),
            })

        df = pd.DataFrame(results)
        return df.sort_values("growth_pct", ascending=False)

    # =============================================================
    # РЕКОМЕНДАЦІЇ
    # =============================================================
    def generate_recommendations(self, gt_data: pd.DataFrame,
                                  marketplace_data: pd.DataFrame) -> list:
        recommendations = []

        # --- Google Trends рекомендації ---
        if not gt_data.empty:
            rising = self.get_rising_trends(gt_data)
            top_rising = rising[rising["growth_pct"] > 15]
            if not top_rising.empty:
                kws = top_rising["keyword"].tolist()[:5]
                recommendations.append({
                    "title": "🚀 Тренди, що швидко зростають",
                    "description": (
                        "Ці пошукові запити показують стабільне зростання інтересу "
                        "в Україні — сигнал до формування асортименту."
                    ),
                    "keywords": kws,
                    "action": "Додайте товари за цими запитами в каталог та налаштуйте SEO",
                    "priority": "high"
                })

            stable = rising[
                (rising["growth_pct"].between(-10, 10)) &
                (rising["last_quarter_avg"] > 30)
            ]
            if not stable.empty:
                kws = stable["keyword"].tolist()[:5]
                recommendations.append({
                    "title": "📊 Стабільний попит — базовий асортимент",
                    "description": "Ці категорії мають стабільний попит без різких коливань.",
                    "keywords": kws,
                    "action": "Забезпечте стабільну наявність цих товарів",
                    "priority": "high"
                })

            declining = rising[rising["growth_pct"] < -15]
            if not declining.empty:
                kws = declining["keyword"].tolist()[:5]
                recommendations.append({
                    "title": "⚠️ Спадаючий інтерес",
                    "description": "Ці запити втрачають популярність.",
                    "keywords": kws,
                    "action": "Зменшіть запаси та переключіть рекламні бюджети",
                    "priority": "medium"
                })

        # --- Marketplace рекомендації ---
        if not marketplace_data.empty:
            scored = self.calculate_trend_scores(marketplace_data)

            # Гарячі товари
            hot = scored[scored["trend_score"] >= 75]
            if not hot.empty:
                top_names = hot["name"].tolist()[:5]
                recommendations.append({
                    "title": f"🔥 {len(hot)} гарячих товарів (trend score ≥ 75)",
                    "description": (
                        "Ці товари мають високу комбінацію відгуків, рейтингу "
                        "та знижок — вони зараз найбільш затребувані."
                    ),
                    "keywords": top_names,
                    "action": "Додайте аналоги цих товарів у свій каталог",
                    "priority": "high"
                })

            # Великі знижки
            drops = self.get_price_drops(marketplace_data, min_discount_pct=25)
            if not drops.empty:
                recommendations.append({
                    "title": f"💰 {len(drops)} товарів з великими знижками (≥25%)",
                    "description": (
                        "Магазини активно просувають ці товари через знижки — "
                        "це сигнал конкуренції або сезонного попиту."
                    ),
                    "keywords": drops["name"].tolist()[:5],
                    "action": "Відстежуйте ціни конкурентів на ці позиції",
                    "priority": "medium"
                })

            # Трендові бренди
            brands = self.get_trending_brands(marketplace_data, top_n=5)
            if not brands.empty:
                brand_names = brands["brand"].tolist()
                recommendations.append({
                    "title": "🏷 Трендові бренди",
                    "description": "Ці бренди мають найвищий середній trend score.",
                    "keywords": brand_names,
                    "action": "Розширте асортимент цих брендів",
                    "priority": "medium"
                })

        # --- Загальні поради ---
        recommendations.append({
            "title": "🇺🇦 Фокус на ціну та цінність",
            "description": (
                "Українські покупці чутливі до ціни. "
                "Позиціонуйте товари через 'цінність': комплекти, бандли, економія."
            ),
            "action": "Створюйте бандли та 'розумні набори'",
            "priority": "high"
        })

        return recommendations
