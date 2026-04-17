import pandas as pd
import numpy as np


class TrendAnalyzer:
    """Анализ трендов и генерация рекомендаций."""

    def count_rising(self, gt_data: pd.DataFrame) -> int:
        """Подсчёт растущих трендов."""
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
        """Выявление растущих трендов с % роста."""
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

            # Волатильность
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
        df = df.sort_values("growth_pct", ascending=False)
        return df

    def generate_recommendations(self, gt_data: pd.DataFrame,
                                  marketplace_data: pd.DataFrame) -> list:
        """Генерация рекомендаций для магазина."""
        recommendations = []

        # ─── Анализ Google Trends ───
        if not gt_data.empty:
            rising = self.get_rising_trends(gt_data)

            # Топ растущие
            top_rising = rising[rising["growth_pct"] > 15]
            if not top_rising.empty:
                kws = top_rising["keyword"].tolist()[:5]
                recommendations.append({
                    "title": "🚀 Тренди, що швидко зростають",
                    "description": (
                        f"Ці запити показують стабільне зростання інтересу "
                        f"в Україні. Це сигнал до формування асортименту "
                        f"або маркетингових кампаній навколо цих тем."
                    ),
                    "keywords": kws,
                    "action": "Додайте товари за цими запитами в каталог та налаштуйте SEO",
                    "priority": "high"
                })

            # Стабильный спрос
            stable = rising[
                (rising["growth_pct"].between(-10, 10)) &
                (rising["last_quarter_avg"] > 30)
            ]
            if not stable.empty:
                kws = stable["keyword"].tolist()[:5]
                recommendations.append({
                    "title": "📊 Стабільний попит — базовий асортимент",
                    "description": (
                        "Ці категорії мають стабільний попит без різких коливань. "
                        "Вони підходять для формування базового асортименту "
                        "з передбачуваним оборотом."
                    ),
                    "keywords": kws,
                    "action": "Забезпечте стабільну наявність цих товарів",
                    "priority": "high"
                })

            # Падающие
            declining = rising[rising["growth_pct"] < -15]
            if not declining.empty:
                kws = declining["keyword"].tolist()[:5]
                recommendations.append({
                    "title": "⚠️ Спадаючий інтерес — обережно",
                    "description": (
                        "Ці запити втрачають популярність. Не варто "
                        "інвестувати в великі закупівлі товарів цих категорій."
                    ),
                    "keywords": kws,
                    "action": "Зменшіть запаси та переключіть рекламні бюджети",
                    "priority": "medium"
                })

        # ─── Анализ маркетплейсов ───
        if not marketplace_data.empty and "rating" in marketplace_data.columns:
            top_rated = marketplace_data[marketplace_data["rating"] >= 4.5]
            if not top_rated.empty:
                recommendations.append({
                    "title": "⭐ Високорейтингові товари — орієнтир якості",
                    "description": (
                        f"Знайдено {len(top_rated)} товарів з рейтингом 4.5+. "
                        f"Аналізуйте їх характеристики — це те, що цінують покупці."
                    ),
                    "action": "Знайдіть аналоги або постачальників схожих товарів",
                    "priority": "high"
                })

            # Ценовые ниши
            if "price" in marketplace_data.columns:
                median_price = marketplace_data["price"].median()
                recommendations.append({
                    "title": f"💰 Цінова ніша: медіанна ціна ₴{median_price:,.0f}",
                    "description": (
                        f"Медіанна ціна в обраних категоріях — ₴{median_price:,.0f}. "
                        f"Товари в діапазоні ₴{median_price*0.7:,.0f}–₴{median_price*1.3:,.0f} "
                        f"мають найширшу аудиторію."
                    ),
                    "action": "Формуйте ціновий асортимент навколо цього діапазону",
                    "priority": "medium"
                })

        # ─── Общие рекомендации для украинского рынка ───
        recommendations.extend([
            {
                "title": "🇺🇦 Фокус на ціну та цінність",
                "description": (
                    "Згідно з дослідженням PwC, українські покупці надзвичайно "
                    "чутливі до ціни. Позиціонуйте товари через 'цінність', "
                    "а не лише ціну: комплекти, бандли, довгострокова економія."
                ),
                "action": "Створюйте бандли та 'розумні набори' для економії",
                "priority": "high"
            },
            {
                "title": "📱 Мобільна оптимізація",
                "description": (
                    "60%+ покупок в Україні здійснюються з мобільних пристроїв. "
                    "Ваш магазин повинен ідеально працювати на смартфонах."
                ),
                "action": "Перевірте мобільну версію магазину та швидкість завантаження",
                "priority": "high"
            },
        ])

        return recommendations
