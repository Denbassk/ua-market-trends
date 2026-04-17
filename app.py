# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
import sqlite3
import json
import yaml
from yaml.loader import SafeLoader

from data_sources.google_trends import GoogleTrendsCollector
from data_sources.rozetka_scraper import RozetkaScraper
from data_sources.prom_scraper import PromScraper
from utils.database import Database
from utils.analytics import TrendAnalyzer

try:
    from data_sources.apify_collector import ApifyCollector
    APIFY_AVAILABLE = True
except ImportError:
    APIFY_AVAILABLE = False

# =============================================
# CONSTANTS
# =============================================
DB_PATH = "data/trends.db"

CATEGORIES = {
    "🍽 Їжа та алкоголь (все)": "food",
    "🥃 Алкогольні напої": "alcohol",
    "🛒 Продукти харчування": "grocery",
    "🧴 Побутова хімія": "chemistry",
    "🏠 Товари для дому": "home",
    "💄 Косметика": "cosmetics",
    "📱 Електроніка": "electronics",
    "👶 Дитячі товари": "kids",
    "🐾 Зоотовари": "pets",
    "💊 Здоров'я та спорт": "health",
    "🔋 Зарядні станції": "energy",
}

# Маппінг код → українська назва (використовується всюди)
CAT_LABELS = {
    "food": "🍽 Їжа та алкоголь",
    "alcohol": "🥃 Алкоголь",
    "grocery": "🛒 Продукти",
    "chemistry": "🧴 Побут. хімія",
    "home": "🏠 Дім",
    "cosmetics": "💄 Косметика",
    "electronics": "📱 Електроніка",
    "kids": "👶 Дитячі",
    "pets": "🐾 Зоотовари",
    "health": "💊 Здоров'я",
    "energy": "🔋 Зарядні",
    "other": "📦 Інше",
    "mixed": "📦 Змішане",
}

PERIOD_MAP = {
    "Останні 7 днів": "now 7-d",
    "Останні 30 днів": "today 1-m",
    "Останні 90 днів": "today 3-m",
    "Останні 6 місяців": "today 6-m",
    "Останній рік": "today 12-m",
}


def cat_label(code):
    """Переводить код категорії в українську назву."""
    return CAT_LABELS.get(code, code)


# =============================================
# AUTH
# =============================================
def check_password():
    if st.session_state.get("authenticated"):
        return True
    st.markdown("## 🔐 Вхід в систему")
    st.caption("UA Market Trends Analyzer")
    username = st.text_input("Логін", key="login_user")
    password = st.text_input("Пароль", type="password", key="login_pass")
    if st.button("Ввійти", key="login_btn", type="primary", use_container_width=True):
        config_path = "config_auth.yaml"
        if os.path.exists(config_path):
            with open(config_path, encoding="utf-8") as f:
                cfg = yaml.load(f, Loader=SafeLoader)
            users = cfg.get("credentials", {}).get("usernames", {})
            user_data = users.get(username)
            if user_data:
                import bcrypt
                if bcrypt.checkpw(password.encode(), user_data["password"].encode()):
                    st.session_state["authenticated"] = True
                    st.session_state["name"] = user_data.get("name", username)
                    st.rerun()
                else:
                    st.error("Невірний пароль")
            else:
                st.error("Користувача не знайдено")
    return False
# =============================================
# LOCAL DATA CACHE
# =============================================
def get_cached_products():
    """Load products from local SQLite cache."""
    if not os.path.exists(DB_PATH):
        return pd.DataFrame()
    try:
        conn = sqlite3.connect(DB_PATH)
        df = pd.read_sql_query(
            "SELECT * FROM product_snapshots ORDER BY collected_at DESC", conn
        )
        conn.close()
        return df
    except Exception:
        return pd.DataFrame()


def get_cache_stats():
    """Get cache statistics."""
    if not os.path.exists(DB_PATH):
        return {"products": 0, "trends": 0, "last_update": "\u043d\u0435\u043c\u0430\u0454"}
    try:
        conn = sqlite3.connect(DB_PATH)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM product_snapshots")
        products = cur.fetchone()[0]
        cur.execute("SELECT COUNT(*) FROM trend_snapshots")
        trends = cur.fetchone()[0]
        cur.execute("SELECT MAX(collected_at) FROM product_snapshots")
        last = cur.fetchone()[0] or "\u043d\u0435\u043c\u0430\u0454"
        conn.close()
        if last != "\u043d\u0435\u043c\u0430\u0454":
            try:
                dt = datetime.fromisoformat(last)
                last = dt.strftime("%d.%m.%Y %H:%M")
            except Exception:
                pass
        return {"products": products, "trends": trends, "last_update": last}
    except Exception:
        return {"products": 0, "trends": 0, "last_update": "\u043d\u0435\u043c\u0430\u0454"}


def save_products_to_cache(df, source):
    """Save products to local cache with ALL fields."""
    if df.empty:
        return
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    # Переконаємось що таблиця має всі колонки
    try:
        conn.execute("ALTER TABLE product_snapshots ADD COLUMN old_price REAL DEFAULT 0")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE product_snapshots ADD COLUMN image TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE product_snapshots ADD COLUMN brand TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE product_snapshots ADD COLUMN seller TEXT DEFAULT ''")
    except Exception:
        pass
    try:
        conn.execute("ALTER TABLE product_snapshots ADD COLUMN rozetka_category TEXT DEFAULT ''")
    except Exception:
        pass

    now = datetime.now().isoformat()
    for _, row in df.iterrows():
        try:
            conn.execute(
                "INSERT INTO product_snapshots "
                "(source, name, price, old_price, rating, reviews_count, "
                "category, rozetka_category, url, image, brand, seller, collected_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    source,
                    str(row.get("name", "")),
                    float(row.get("price", 0) or 0),
                    float(row.get("old_price", 0) or 0),
                    float(row.get("rating", 0) or 0),
                    int(float(row.get("reviews_count", 0) or 0)),
                    str(row.get("category", "")),
                    str(row.get("rozetka_category", "")),
                    str(row.get("url", "")),
                    str(row.get("image", "")),
                    str(row.get("brand", "")),
                    str(row.get("seller", "")),
                    now,
                )
            )
        except Exception:
            continue
    conn.commit()
    conn.close()


# =============================================
# MAIN
# =============================================
def main():
    st.set_page_config(
        page_title="UA Market Trends",
        page_icon="🇺🇦",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    # --- Глобальні стилі ---
    st.markdown("""
    <style>
        .main-header {
            font-size: 2rem; font-weight: 700; text-align: center;
            padding: 0.5rem 0; margin-bottom: 0.5rem;
            background: linear-gradient(90deg, #0057b7 0%, #ffd700 100%);
            -webkit-background-clip: text; -webkit-text-fill-color: transparent;
        }
        div[data-testid="stSidebar"] {
            background: linear-gradient(180deg, #0d1117 0%, #161b22 100%);
        }
        div[data-testid="stSidebar"] .stRadio > label {
            font-size: 1.05rem;
        }
    </style>
    """, unsafe_allow_html=True)

    if not check_password():
        st.stop()

    # --- SIDEBAR ---
    with st.sidebar:
        st.markdown(
            '<p class="main-header">🇺🇦 UA Market Trends</p>',
            unsafe_allow_html=True
        )
        st.caption(f"👤 {st.session_state.get('name', '')}")

        if st.button("🚪 Вийти", use_container_width=True):
            st.session_state["authenticated"] = False
            st.rerun()

        st.divider()

        # Cache stats
        stats = get_cache_stats()
        if stats["products"] > 0:
            st.success(
                f"📦 **{stats['products']}** товарів\n\n"
                f"🕐 {stats['last_update']}"
            )
        else:
            st.warning("📦 База порожня")

        st.divider()

        page = st.radio(
            "📋 Розділ:",
            [
                "🚀 Зібрати дані",
                "📊 Аналітика товарів",
                "📈 Тренди Google",
                "💡 Рекомендації",
                "📥 Експорт даних",
            ],
            label_visibility="collapsed",
        )

    # --- HEADER ---
    st.markdown(
        '<p class="main-header">🇺🇦 UA Market Trends Analyzer</p>',
        unsafe_allow_html=True
    )

    # --- ROUTING ---
    if page.startswith("🚀"):
        page_collect()
    elif page.startswith("📊"):
        page_analytics()
    elif page.startswith("📈"):
        page_trends()
    elif page.startswith("💡"):
        page_recommendations()
    elif page.startswith("📥"):
        page_export()


# =============================================
# PAGE: Collect Data
# =============================================
def page_collect():
    st.header("\U0001f680 \u0417\u0431\u0456\u0440 \u0434\u0430\u043d\u0438\u0445")

    # Cache info
    stats = get_cache_stats()
    if stats["products"] > 0:
        st.success(
            "\U0001f4be \u0423 \u0431\u0430\u0437\u0456 \u0432\u0436\u0435 \u0454 **{} \u0442\u043e\u0432\u0430\u0440\u0456\u0432** "
            "(\u043e\u043d\u043e\u0432\u043b\u0435\u043d\u043e {}). "
            "\u041c\u043e\u0436\u0435\u0442\u0435 \u043f\u0435\u0440\u0435\u0439\u0442\u0438 \u0434\u043e "
            "'\u0410\u043d\u0430\u043b\u0456\u0442\u0438\u043a\u0430' \u0431\u0435\u0437 \u043d\u043e\u0432\u043e\u0433\u043e \u0437\u0431\u043e\u0440\u0443.".format(
                stats["products"], stats["last_update"]
            )
        )

    st.info(
        "\U0001f4a1 **\u042f\u043a \u0447\u0430\u0441\u0442\u043e \u0437\u0431\u0438\u0440\u0430\u0442\u0438:**\n\n"
        "- **Rozetka/Prom.ua:** 1-2 \u0440\u0430\u0437\u0438 \u043d\u0430 \u043c\u0456\u0441\u044f\u0446\u044c "
        "(\u0446\u0456\u043d\u0438 \u0442\u0430 \u0430\u0441\u043e\u0440\u0442\u0438\u043c\u0435\u043d\u0442 "
        "\u0437\u043c\u0456\u043d\u044e\u044e\u0442\u044c\u0441\u044f \u043f\u043e\u0432\u0456\u043b\u044c\u043d\u043e)\n\n"
        "- **Google Trends:** 1 \u0440\u0430\u0437 \u043d\u0430 \u0442\u0438\u0436\u0434\u0435\u043d\u044c "
        "(\u0434\u043b\u044f \u0432\u0456\u0434\u0441\u0442\u0435\u0436\u0435\u043d\u043d\u044f \u0441\u0435\u0437\u043e\u043d\u043d\u043e\u0441\u0442\u0456)\n\n"
        "- **100-200 \u0442\u043e\u0432\u0430\u0440\u0456\u0432 \u043d\u0430 \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0456\u044e** "
        "\u2014 \u0434\u043e\u0441\u0442\u0430\u0442\u043d\u044c\u043e \u0434\u043b\u044f \u0430\u043d\u0430\u043b\u0456\u0437\u0443"
    )

    st.divider()

    # --- Settings ---
    col1, col2 = st.columns(2)

    with col1:
        selected = st.multiselect(
            "\U0001f4c2 \u041a\u0430\u0442\u0435\u0433\u043e\u0440\u0456\u0457:",
            options=list(CATEGORIES.keys()),
            default=[list(CATEGORIES.keys())[0], list(CATEGORIES.keys())[1], list(CATEGORIES.keys())[2], list(CATEGORIES.keys())[3]]
        )

        time_period = st.selectbox(
            "\U0001f4c5 \u041f\u0435\u0440\u0456\u043e\u0434 (Google Trends):",
            list(PERIOD_MAP.keys()), index=1
        )

    with col2:
        max_products = st.select_slider(
            "\U0001f4e6 \u041c\u0430\u043a\u0441. \u0442\u043e\u0432\u0430\u0440\u0456\u0432 \u043d\u0430 \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0456\u044e:",
            options=[20, 50, 100, 200, 500],
            value=50,
            help="\u0411\u0456\u043b\u044c\u0448\u0435 \u0442\u043e\u0432\u0430\u0440\u0456\u0432 = "
                 "\u0431\u0456\u043b\u044c\u0448\u0435 \u0447\u0430\u0441\u0443 \u0442\u0430 \u043b\u0456\u043c\u0456\u0442\u0456\u0432 Apify"
        )

        max_keywords = st.select_slider(
            "\U0001f50d Google Trends \u0437\u0430\u043f\u0438\u0442\u0456\u0432 \u043d\u0430 \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0456\u044e:",
            options=[1, 2, 3, 5],
            value=2,
        )

    # --- Estimated cost ---
    n_cats = len(selected)
    est_products = n_cats * max_products
    est_cost_apify = n_cats * 0.5  # ~$0.50 per category on Rozetka scraper
    st.markdown(
        "\U0001f4b0 **\u041e\u0446\u0456\u043d\u043a\u0430:** ~{} \u0442\u043e\u0432\u0430\u0440\u0456\u0432, "
        "~${:.2f} Apify \u043a\u0440\u0435\u0434\u0438\u0442\u0456\u0432, "
        "~{} \u0445\u0432 \u0447\u0430\u0441\u0443".format(
            est_products, est_cost_apify, max(1, n_cats * 1)
        )
    )

    st.divider()

    # --- Sources ---
    st.subheader("\U0001f50d \u0414\u0436\u0435\u0440\u0435\u043b\u0430")

    c1, c2, c3 = st.columns(3)
    with c1:
        use_gt = st.checkbox("Google Trends", value=True)
    with c2:
        use_roz = st.checkbox("Rozetka", value=True)
    with c3:
        use_prom = st.checkbox("Prom.ua", value=False)

    # Apify
    saved_token = ""
    try:
        saved_token = st.secrets.get("APIFY_API_TOKEN", "")
    except Exception:
        saved_token = ""

    use_apify = st.checkbox(
        "\U0001f511 Apify",
        value=bool(saved_token),
        help="\u0420\u0435\u0430\u043b\u044c\u043d\u0456 \u0434\u0430\u043d\u0456 \u0437 "
             "\u043c\u0430\u0440\u043a\u0435\u0442\u043f\u043b\u0435\u0439\u0441\u0456\u0432. "
             "\u0411\u0435\u0437 Apify \u2014 \u0434\u0430\u043d\u0456 \u0437 \u043b\u043e\u043a\u0430\u043b\u044c\u043d\u043e\u0433\u043e \u043a\u0435\u0448\u0443."
    )
    apify_token = saved_token
    if use_apify and not saved_token:
        apify_token = st.text_input("Apify API Token:", type="password")
    elif use_apify and saved_token:
        st.success("\u2705 API \u043a\u043b\u044e\u0447 \u0437\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0435\u043d\u043e")

    st.divider()

    # --- Load from Apify cache ---
    if APIFY_AVAILABLE and use_apify and (apify_token or saved_token):
        token = apify_token or saved_token
        if st.button("\U0001f4e6 \u0417\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0438\u0442\u0438 \u0437 \u043e\u0441\u0442\u0430\u043d\u043d\u044c\u043e\u0433\u043e \u0437\u0430\u043f\u0443\u0441\u043a\u0443 Apify (\u0431\u0435\u0437 \u0432\u0438\u0442\u0440\u0430\u0442)", use_container_width=True):
            apify = ApifyCollector(token)
            cached = apify.get_last_dataset(max_items=2500)
            if not cached.empty:
                db = Database()
                db.clear_products()
                save_products_to_cache(cached, "rozetka")
                st.success("\u2705 \u0417\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0435\u043d\u043e {} \u0443\u043d\u0456\u043a\u0430\u043b\u044c\u043d\u0438\u0445 \u0442\u043e\u0432\u0430\u0440\u0456\u0432".format(len(cached)))
                st.rerun()
    # --- End cache ---

    if st.button(
        "\U0001f680 \u0417\u0456\u0431\u0440\u0430\u0442\u0438 \u0434\u0430\u043d\u0456",
        type="primary", use_container_width=True
    ):
        do_collect(selected, PERIOD_MAP[time_period],
                   use_gt, use_roz, use_prom, use_apify, apify_token,
                   max_products, max_keywords)

def do_collect(selected, period, use_gt, use_roz, use_prom,
               use_apify, apify_token, max_products=50, max_keywords=2):
    db = Database()
    codes = [CATEGORIES[c] for c in selected]

    apify = None
    if APIFY_AVAILABLE and use_apify and apify_token:
        apify = ApifyCollector(apify_token)
        if not apify.available:
            apify = None

    progress = st.progress(0)

    # Google Trends
    gt_data = pd.DataFrame()
    if use_gt and codes:
        progress.progress(10, text="\U0001f4e1 Google Trends ({} \u0437\u0430\u043f\u0438\u0442\u0456\u0432 \u043d\u0430 \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0456\u044e)...".format(max_keywords))
        gt = GoogleTrendsCollector()
        gt.MAX_KEYWORDS = max_keywords  # Pass limit to collector
        gt_data = gt.get_trends_for_categories(codes, geo="UA", timeframe=period)
        if not gt_data.empty:
            db.save_trends(gt_data, "google_trends")

    # Rozetka
    roz_data = pd.DataFrame()
    if use_roz:
        progress.progress(40, text="\U0001f6d2 Rozetka (\u043c\u0430\u043a\u0441 {} \u0442\u043e\u0432\u0430\u0440\u0456\u0432/\u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0456\u044e)...".format(max_products))
        if apify:
            try:
                roz_data = apify.get_rozetka_products(codes, max_per_category=max_products)
            except Exception as e:
                st.warning(f"\u26a0\ufe0f Apify Rozetka: {e}")
                roz_data = pd.DataFrame()
        if roz_data.empty:
            roz = RozetkaScraper()
            roz_data = roz.get_top_products(codes, max_per_category=max_products)
        if not roz_data.empty:
            save_products_to_cache(roz_data, "rozetka")

    # Prom
    prom_data = pd.DataFrame()
    if use_prom:
        progress.progress(70, text="\U0001f6d2 Prom.ua...")
        if apify:
            try:
                prom_data = apify.get_prom_products(codes, max_per_category=max_products)
            except Exception as e:
                st.warning(f"\u26a0\ufe0f Apify Prom: {e}")
                prom_data = pd.DataFrame()
        if prom_data.empty:
            prom = PromScraper()
            prom_data = prom.get_top_products(codes, max_per_category=max_products)
        if not prom_data.empty:
            save_products_to_cache(prom_data, "prom")

    progress.progress(100, text="\u2705 \u0413\u043e\u0442\u043e\u0432\u043e!")

    st.divider()
    st.subheader("\U0001f4ca \u0420\u0435\u0437\u0443\u043b\u044c\u0442\u0430\u0442 \u0437\u0431\u043e\u0440\u0443")

    c1, c2, c3 = st.columns(3)
    with c1:
        n = len(gt_data.columns) if not gt_data.empty else 0
        st.metric("Google Trends", "{} \u0437\u0430\u043f\u0438\u0442\u0456\u0432".format(n))
    with c2:
        st.metric("Rozetka", "{} \u0442\u043e\u0432\u0430\u0440\u0456\u0432".format(len(roz_data)))
    with c3:
        st.metric("Prom.ua", "{} \u0442\u043e\u0432\u0430\u0440\u0456\u0432".format(len(prom_data)))

    total = len(roz_data) + len(prom_data)
    st.success(
        "\u2705 \u0417\u0456\u0431\u0440\u0430\u043d\u043e {} \u0442\u043e\u0432\u0430\u0440\u0456\u0432. "
        "\u041f\u0435\u0440\u0435\u0439\u0434\u0456\u0442\u044c \u0443 "
        "'\u0410\u043d\u0430\u043b\u0456\u0442\u0438\u043a\u0430 \u0442\u043e\u0432\u0430\u0440\u0456\u0432' "
        "\u0434\u043b\u044f \u0430\u043d\u0430\u043b\u0456\u0437\u0443.".format(total)
    )

    st.session_state["gt_data"] = gt_data
    st.session_state["roz_data"] = roz_data
    st.session_state["prom_data"] = prom_data


# =============================================
# PAGE: Analytics (з трендовим скорингом)
# =============================================
def page_analytics():
    st.header("📊 Аналітика трендів")

    df = get_cached_products()

    if df.empty:
        st.warning("Немає даних. Перейдіть у 'Зібрати дані' спочатку.")
        return

    # ---- Рахуємо trend_score для всіх товарів ----
    analyzer = TrendAnalyzer()
    scored_df = analyzer.calculate_trend_scores(df)

    # ---- TABS ----
    tab_hot, tab_brands, tab_drops, tab_categories, tab_charts, tab_table, tab_recs = st.tabs([
        "🔥 Гарячі товари",
        "🏷 Трендові бренди",
        "💰 Великі знижки",
        "📂 Категорії",
        "📊 Графіки",
        "📋 Таблиця",
        "💡 Рекомендації",
    ])

    # ===========================================================
    # TAB 1: Гарячі товари (топ за trend_score)
    # ===========================================================
    with tab_hot:
        st.subheader("🔥 Найтрендовіші товари зараз")
        st.caption(
            "Trend Score = популярність (кількість відгуків) × якість (рейтинг) × знижка. "
            "Чим вищий бал — тим більш затребуваний товар."
        )

        # Фільтри для гарячих
        fh1, fh2, fh3 = st.columns(3)
        with fh1:
            hot_cat_codes = sorted(scored_df["category"].unique().tolist())
            hot_cat_options = ["(всі)"] + [cat_label(c) for c in hot_cat_codes]
            hot_cat_selected = st.selectbox("Категорія:", hot_cat_options, key="hot_cat")
        with fh2:
            hot_n = st.select_slider("Кількість:", options=[10, 20, 50, 100], value=20, key="hot_n")
        with fh3:
            min_reviews = st.number_input("Мін. відгуків:", min_value=0, value=5, step=5, key="hot_minrev")

        hot_df = scored_df.copy()
        if hot_cat_selected != "(всі)":
            # Знаходимо код за лейблом
            reverse_map = {cat_label(c): c for c in hot_cat_codes}
            hot_df = hot_df[hot_df["category"] == reverse_map.get(hot_cat_selected, "")]
        hot_df = hot_df[hot_df["reviews_count"] >= min_reviews]
        hot_df = hot_df.head(hot_n)

        if hot_df.empty:
            st.info("Немає товарів з такими фільтрами.")
        else:
            # Метрики зверху
            m1, m2, m3, m4 = st.columns(4)
            with m1:
                st.metric("🔥 Макс. Score", f"{hot_df['trend_score'].max():.0f}")
            with m2:
                st.metric("⭐ Сер. рейтинг", f"{hot_df['rating'].mean():.1f}")
            with m3:
                st.metric("💬 Сер. відгуків", f"{hot_df['reviews_count'].mean():.0f}")
            with m4:
                avg_p = hot_df['price'].mean()
                st.metric("💰 Сер. ціна", f"{avg_p:,.0f} ₴")

            # Bar chart — топ товари за score
            chart_df = hot_df.head(20).copy()
            chart_df["short_name"] = chart_df["name"].str[:40] + "..."
            fig = px.bar(
                chart_df, x="trend_score", y="short_name", orientation="h",
                color="trend_score",
                color_continuous_scale=["#3b82f6", "#f59e0b", "#ef4444"],
                hover_data=["name", "price", "rating", "reviews_count", "brand"],
                title="🔥 Топ товарів за Trend Score",
                labels={"trend_score": "Trend Score", "short_name": ""},
                template="plotly_dark",
            )
            fig.update_layout(height=max(400, len(chart_df) * 28), yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)

            # Картки товарів
            st.divider()
            for idx, (_, row) in enumerate(hot_df.head(20).iterrows()):
                with st.container():
                    a, b, c, d, e = st.columns([4, 1, 1, 1, 1])
                    with a:
                        name = row.get("name", "N/A")
                        brand = row.get("brand", "")
                        label = row.get("trend_label", "")
                        st.markdown(f"**{idx+1}. {label} {name}**")
                        parts = []
                        if brand and str(brand) != "nan" and len(str(brand)) > 1:
                            parts.append(str(brand))
                        roz_cat = row.get("rozetka_category", "")
                        if roz_cat and str(roz_cat) != "nan":
                            parts.append(str(roz_cat))
                        else:
                            parts.append(str(row.get("category", "")))
                        st.caption(" | ".join(parts))
                    with b:
                        score = float(row.get("trend_score", 0))
                        st.metric("Score", f"{score:.0f}")
                    with c:
                        p = float(row.get("price", 0) or 0)
                        old_p = float(row.get("old_price", 0) or 0)
                        try:
                            delta_val = f"-{old_p - p:.0f} ₴" if old_p > p else None
                        except (ValueError, TypeError):
                            delta_val = None
                        st.metric("Ціна", f"{p:.0f} ₴", delta=delta_val, delta_color="normal")
                    with d:
                        r = float(row.get("rating", 0) or 0)
                        st.metric("Рейтинг", f"⭐ {r:.1f}" if r > 0 else "-")
                    with e:
                        rv = int(float(row.get("reviews_count", 0) or 0))
                        st.metric("Відгуки", f"{rv:,}".replace(",", " ") if rv > 0 else "-")
                    st.divider()

    # ===========================================================
    # TAB 2: Трендові бренди
    # ===========================================================
    with tab_brands:
        st.subheader("🏷 Трендові бренди")
        st.caption("Бренди відсортовані за середнім Trend Score їхніх товарів.")

        brands_df = analyzer.get_trending_brands(df, top_n=20)

        if brands_df.empty:
            st.info("Немає даних про бренди.")
        else:
            # Bar chart
            fig_b = px.bar(
                brands_df, x="avg_score", y="brand", orientation="h",
                color="avg_score",
                color_continuous_scale=["#6366f1", "#f59e0b", "#ef4444"],
                hover_data=["total_reviews", "avg_rating", "products_count", "avg_price"],
                title="Середній Trend Score за брендами",
                labels={"avg_score": "Сер. Trend Score", "brand": ""},
                template="plotly_dark",
            )
            fig_b.update_layout(height=max(400, len(brands_df) * 28), yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig_b, use_container_width=True)

            # Таблиця брендів
            st.dataframe(
                brands_df,
                use_container_width=True,
                column_config={
                    "brand": st.column_config.TextColumn("Бренд"),
                    "avg_score": st.column_config.NumberColumn("Trend Score", format="%.1f"),
                    "total_reviews": st.column_config.NumberColumn("Всього відгуків"),
                    "avg_rating": st.column_config.NumberColumn("Сер. рейтинг", format="%.1f"),
                    "products_count": st.column_config.NumberColumn("Товарів"),
                    "avg_price": st.column_config.NumberColumn("Сер. ціна ₴", format="%.0f"),
                },
            )

    # ===========================================================
    # TAB 3: Великі знижки (цінові аномалії)
    # ===========================================================
    with tab_drops:
        st.subheader("💰 Товари з великими знижками")
        st.caption(
            "Великі знижки = магазин активно просуває товар. "
            "Це сигнал конкуренції або сезонного попиту."
        )

        min_disc = st.slider("Мін. знижка (%):", 10, 70, 20, 5, key="min_disc")
        drops_df = analyzer.get_price_drops(df, min_discount_pct=min_disc)

        if drops_df.empty:
            st.info(f"Немає товарів зі знижкою ≥ {min_disc}%.")
        else:
            st.success(f"Знайдено **{len(drops_df)}** товарів зі знижкою ≥ {min_disc}%")

            # Scatter: знижка vs кількість відгуків
            drops_plot = drops_df.head(100).copy()
            drops_plot["reviews_count"] = pd.to_numeric(drops_plot["reviews_count"], errors="coerce").fillna(1)
            fig_d = px.scatter(
                drops_plot, x="discount_pct", y="reviews_count",
                size="reviews_count", color="discount_pct",
                color_continuous_scale=["#22c55e", "#ef4444"],
                hover_name="name",
                hover_data=["price", "old_price", "rating", "brand"],
                title="Знижка (%) vs Популярність",
                labels={"discount_pct": "Знижка %", "reviews_count": "Відгуків"},
                template="plotly_dark",
            )
            st.plotly_chart(fig_d, use_container_width=True)

            # Таблиця
            show_cols = ["name", "price", "old_price", "discount_pct", "rating", "reviews_count", "brand", "category"]
            show_cols = [c for c in show_cols if c in drops_df.columns]
            st.dataframe(
                drops_df[show_cols].head(50),
                use_container_width=True,
                column_config={
                    "name": st.column_config.TextColumn("Назва", width="large"),
                    "price": st.column_config.NumberColumn("Ціна ₴", format="%.0f"),
                    "old_price": st.column_config.NumberColumn("Стара ціна ₴", format="%.0f"),
                    "discount_pct": st.column_config.NumberColumn("Знижка %", format="%.1f"),
                    "rating": st.column_config.NumberColumn("Рейтинг", format="%.1f"),
                    "reviews_count": st.column_config.NumberColumn("Відгуків"),
                    "brand": st.column_config.TextColumn("Бренд"),
                    "category": st.column_config.TextColumn("Категорія"),
                },
            )

    # ===========================================================
    # TAB 4: Категорії
    # ===========================================================
    with tab_categories:
        st.subheader("📂 Аналіз категорій")

        cats_df = analyzer.get_trending_categories(df)

        if cats_df.empty:
            st.info("Немає даних по категоріях.")
        else:
            # Bubble chart
            cats_df["products_count"] = pd.to_numeric(cats_df["products_count"], errors="coerce").fillna(1)
            fig_c = px.scatter(
                cats_df, x="avg_price", y="avg_score",
                size="products_count", color="avg_score",
                color_continuous_scale=["#6366f1", "#f59e0b", "#ef4444"],
                hover_name="category_name",
                hover_data=["total_reviews", "avg_rating", "products_count", "max_discount_pct"],
                title="Категорії: Ціна vs Trend Score (розмір = кількість товарів)",
                labels={"avg_price": "Сер. ціна ₴", "avg_score": "Trend Score"},
                template="plotly_dark",
            )
            fig_c.update_layout(height=500)
            st.plotly_chart(fig_c, use_container_width=True)

            # Таблиця категорій
            st.dataframe(
                cats_df,
                use_container_width=True,
                column_config={
                    "category_name": st.column_config.TextColumn("Категорія", width="large"),
                    "avg_score": st.column_config.NumberColumn("Trend Score", format="%.1f"),
                    "total_reviews": st.column_config.NumberColumn("Відгуків"),
                    "avg_rating": st.column_config.NumberColumn("Рейтинг", format="%.1f"),
                    "products_count": st.column_config.NumberColumn("Товарів"),
                    "avg_price": st.column_config.NumberColumn("Сер. ціна ₴", format="%.0f"),
                    "max_discount_pct": st.column_config.NumberColumn("Сер. знижка %", format="%.1f"),
                },
            )

    # ===========================================================
    # TAB 5: Графіки (оригінальні)
    # ===========================================================
    with tab_charts:
        st.subheader("📊 Загальні графіки")
        render_charts(scored_df)

    # ===========================================================
    # TAB 6: Таблиця (з trend_score)
    # ===========================================================
    with tab_table:
        st.subheader("📋 Всі товари")
        render_table(scored_df)

    # ===========================================================
    # TAB 7: Рекомендації
    # ===========================================================
    with tab_recs:
        st.subheader("💡 Рекомендації для бізнесу")
        gt_data = st.session_state.get("gt_data", pd.DataFrame())
        recs = analyzer.generate_recommendations(gt_data, df)

        for rec in recs:
            priority = rec.get("priority", "medium")
            icon = "🔴" if priority == "high" else "🟡"
            with st.expander(f"{icon} {rec['title']}", expanded=(priority == 'high')):
                st.write(rec["description"])
                if rec.get("keywords"):
                    st.markdown("**Ключові позиції:** " + ", ".join(str(k) for k in rec["keywords"]))
                if rec.get("action"):
                    st.info(f"📌 Дія: {rec['action']}")


# =============================================
# HELPER: Charts
# =============================================
def render_charts(df):
    if df.empty:
        return

    c1, c2 = st.columns(2)

    with c1:
        fig = px.histogram(
            df, x="price", nbins=40,
            title="💰 Розподіл цін",
            labels={"price": "Ціна (₴)", "count": "Кількість товарів"},
            color_discrete_sequence=["#667eea"],
            template="plotly_dark"
        )
        fig.update_layout(
            yaxis_title="Кількість товарів",
            xaxis_title="Ціна (₴)",
            height=400,
        )
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        if "category" in df.columns:
            cat_counts = df["category"].value_counts().reset_index()
            cat_counts.columns = ["Категорія", "Кількість"]
            # Замінюємо англійські коди на українські
            cat_names = {
                "food": "🍽 Їжа та алкоголь",
                "alcohol": "🥃 Алкоголь",
                "grocery": "🛒 Продукти",
                "chemistry": "🧴 Побут. хімія",
                "home": "🏠 Дім",
                "cosmetics": "💄 Косметика",
                "electronics": "📱 Електроніка",
                "kids": "👶 Дитячі",
                "pets": "🐾 Зоотовари",
                "health": "💊 Здоров'я",
                "energy": "🔋 Зарядні",
                "other": "📦 Інше",
                "mixed": "📦 Змішане",
            }
            cat_counts["Категорія"] = cat_counts["Категорія"].map(
                lambda x: cat_names.get(x, x)
            )
            fig2 = px.pie(
                cat_counts, names="Категорія", values="Кількість",
                title="📂 Товари за категоріями",
                template="plotly_dark"
            )
            fig2.update_layout(height=400)
            fig2.update_traces(textposition="inside", textinfo="percent+label")
            st.plotly_chart(fig2, use_container_width=True)

    # Розподіл Trend Score
    if "trend_score" in df.columns:
        trend_labels_map = {
            "🔵 Низький": "🔵 Низький",
            "🟡 Середній": "🟡 Середній",
            "🟠 Високий": "🟠 Високий",
            "🔴 Гарячий": "🔴 Гарячий",
        }
        fig_ts = px.histogram(
            df, x="trend_score", nbins=30,
            color="trend_label" if "trend_label" in df.columns else None,
            title="🔥 Розподіл Trend Score",
            labels={
                "trend_score": "Trend Score (бали)",
                "count": "Кількість товарів",
                "trend_label": "Рівень тренду",
            },
            category_orders={"trend_label": ["🔵 Низький", "🟡 Середній", "🟠 Високий", "🔴 Гарячий"]},
            color_discrete_map={
                "🔵 Низький": "#6366f1",
                "🟡 Середній": "#22c55e",
                "🟠 Високий": "#f59e0b",
                "🔴 Гарячий": "#ef4444",
            },
            template="plotly_dark",
        )
        fig_ts.update_layout(
            yaxis_title="Кількість товарів",
            xaxis_title="Trend Score (бали)",
            height=400,
            legend_title_text="Рівень тренду",
        )
        st.plotly_chart(fig_ts, use_container_width=True)

    # Ціна vs Рейтинг (з фільтрами)
    if "rating" in df.columns and df["rating"].sum() > 0:
        st.divider()
        st.markdown("#### 💎 Ціна vs Рейтинг")

        df_rated = df[df["rating"] > 0].copy()
        if not df_rated.empty:
            # Фільтри
            sf1, sf2, sf3 = st.columns(3)
            with sf1:
                cat_names_scatter = {
                    "food": "🍽 Їжа та алкоголь", "alcohol": "🥃 Алкоголь",
                    "grocery": "🛒 Продукти", "chemistry": "🧴 Побут. хімія",
                    "home": "🏠 Дім", "cosmetics": "💄 Косметика",
                    "electronics": "📱 Електроніка", "kids": "👶 Дитячі",
                    "pets": "🐾 Зоотовари", "health": "💊 Здоров'я",
                    "energy": "🔋 Зарядні", "other": "📦 Інше", "mixed": "📦 Змішане",
                }
                available_cats = sorted(df_rated["category"].unique().tolist())
                cat_options = {cat_names_scatter.get(c, c): c for c in available_cats}
                selected_cats = st.multiselect(
                    "Категорії:",
                    options=list(cat_options.keys()),
                    default=list(cat_options.keys()),
                    key="scatter_cats"
                )
                selected_codes = [cat_options[c] for c in selected_cats]

            with sf2:
                price_max = int(df_rated["price"].max()) + 1
                scatter_price = st.slider(
                    "Ціна (₴):",
                    0, min(price_max, 50000),
                    (0, min(5000, price_max)),
                    key="scatter_price"
                )

            with sf3:
                scatter_top = st.select_slider(
                    "Показати товарів:",
                    options=[50, 100, 200, 500, 1000],
                    value=200,
                    key="scatter_top"
                )

            # Застосовуємо фільтри
            plot_df = df_rated[df_rated["category"].isin(selected_codes)].copy()
            plot_df = plot_df[
                (plot_df["price"] >= scatter_price[0]) &
                (plot_df["price"] <= scatter_price[1])
            ]
            if "trend_score" in plot_df.columns:
                plot_df = plot_df.sort_values("trend_score", ascending=False)
            plot_df = plot_df.head(scatter_top)

            if plot_df.empty:
                st.info("Немає товарів з такими фільтрами.")
            else:
                st.caption(f"Показано {len(plot_df)} товарів")

                # Додаємо українські назви категорій для легенди
                plot_df["Категорія"] = plot_df["category"].map(
                    lambda x: cat_names_scatter.get(x, x)
                )
                plot_df["_size"] = plot_df["reviews_count"].clip(lower=10)

                fig3 = px.scatter(
                    plot_df, x="price", y="rating",
                    size="_size",
                    size_max=25,
                    color="Категорія",
                    hover_name="name",
                    hover_data={
                        "price": ":.0f",
                        "rating": ":.1f",
                        "reviews_count": True,
                        "brand": True,
                        "trend_score": ":.0f" if "trend_score" in plot_df.columns else False,
                        "_size": False,
                        "Категорія": False,
                    },
                    title=f"Ціна vs Рейтинг (розмір = відгуки, топ-{scatter_top})",
                    labels={
                        "price": "Ціна (₴)",
                        "rating": "Рейтинг ⭐",
                        "reviews_count": "Відгуків",
                        "brand": "Бренд",
                        "trend_score": "Score",
                    },
                    template="plotly_dark",
                )
                fig3.update_layout(
                    height=600,
                    legend_title_text="Категорія",
                    legend=dict(font=dict(size=12)),
                )
                fig3.update_traces(
                    marker=dict(opacity=0.75, line=dict(width=0.5, color="white"))
                )
                st.plotly_chart(fig3, use_container_width=True)

    # Топ бренди
    if "brand" in df.columns:
        brands = df[df["brand"].astype(str).str.len() > 1]
        if not brands.empty:
            top_brands = brands["brand"].value_counts().head(15).reset_index()
            top_brands.columns = ["Бренд", "Кількість"]
            fig4 = px.bar(
                top_brands, x="Кількість", y="Бренд", orientation="h",
                title="🏆 Топ-15 брендів за кількістю товарів",
                labels={"Кількість": "Кількість товарів", "Бренд": ""},
                color="Кількість",
                color_continuous_scale="Viridis",
                template="plotly_dark",
            )
            fig4.update_layout(
                height=500,
                yaxis=dict(autorange="reversed"),
                coloraxis_showscale=False,
            )
            st.plotly_chart(fig4, use_container_width=True)

    # Середня ціна за категоріями
    if "category" in df.columns:
        cat_names = {
            "food": "🍽 Їжа", "alcohol": "🥃 Алкоголь", "grocery": "🛒 Продукти",
            "chemistry": "🧴 Хімія", "home": "🏠 Дім", "cosmetics": "💄 Косметика",
            "electronics": "📱 Електроніка", "kids": "👶 Дитячі", "pets": "🐾 Зоотовари",
            "health": "💊 Здоров'я", "energy": "🔋 Зарядні", "other": "📦 Інше",
            "mixed": "📦 Змішане",
        }
        cat_stats = df.copy()
        cat_stats["cat_label"] = cat_stats["category"].map(lambda x: cat_names.get(x, x))
        cat_stats = cat_stats.groupby("cat_label").agg(
            avg_price=("price", "mean"),
            avg_rating=("rating", "mean"),
            count=("name", "count"),
        ).reset_index()

        fig5 = px.bar(
            cat_stats, x="cat_label", y="avg_price",
            title="📊 Середня ціна за категоріями",
            labels={"avg_price": "Середня ціна (₴)", "cat_label": "Категорія", "avg_rating": "Рейтинг"},
            color="avg_rating",
            color_continuous_scale="RdYlGn",
            hover_data={"count": True, "avg_rating": ":.1f"},
            template="plotly_dark",
        )
        fig5.update_layout(
            height=450,
            xaxis_title="",
            coloraxis_colorbar_title_text="Сер. рейтинг",
        )
        st.plotly_chart(fig5, use_container_width=True)


# =============================================
# HELPER: Table
# =============================================
def render_table(df):
    if df.empty:
        return

    per_page = st.selectbox(
        "Товарів на сторінці:", [25, 50, 100, 200, 500],
        index=1, key="items_per_page"
    )

    total = len(df)
    total_pages = max(1, (total + per_page - 1) // per_page)
    page_num = st.number_input(
        "Сторінка:", min_value=1, max_value=total_pages, value=1, key="page_num"
    )

    start = (page_num - 1) * per_page
    end = start + per_page
    page_df = df.iloc[start:end]

    st.caption(f"Показано {start+1}-{min(end, total)} з {total} товарів | Сторінка {page_num}/{total_pages}")

    display_cols = ["name", "trend_score", "trend_label", "price", "old_price",
                    "rating", "reviews_count", "brand", "category", "rozetka_category", "source"]
    show_cols = [c for c in display_cols if c in page_df.columns]

    display_cols = ["name", "trend_score", "trend_label", "price", "old_price",
                    "rating", "reviews_count", "brand", "category", "rozetka_category", "source"]
    show_cols = [c for c in display_cols if c in page_df.columns]

    # Перекладаємо категорії
    display_df = page_df[show_cols].copy()
    if "category" in display_df.columns:
        display_df["category"] = display_df["category"].map(lambda x: cat_label(x))

    st.dataframe(
        display_df,
        use_container_width=True,
        height=600,
        column_config={
            "name": st.column_config.TextColumn("Назва", width="large"),
            "trend_score": st.column_config.NumberColumn("🔥 Score", format="%.0f"),
            "trend_label": st.column_config.TextColumn("Тренд"),
            "price": st.column_config.NumberColumn("Ціна ₴", format="%.0f"),
            "old_price": st.column_config.NumberColumn("Стара ₴", format="%.0f"),
            "rating": st.column_config.NumberColumn("Рейтинг", format="%.1f"),
            "reviews_count": st.column_config.NumberColumn("Відгуки"),
            "brand": st.column_config.TextColumn("Бренд"),
            "category": st.column_config.TextColumn("Категорія"),
            "rozetka_category": st.column_config.TextColumn("Rozetka"),
            "source": st.column_config.TextColumn("Джерело"),
        }
    )


# =============================================
# PAGE: Google Trends
# =============================================
def page_trends():
    st.header("\U0001f4c8 \u0422\u0440\u0435\u043d\u0434\u0438 Google")

    gt_data = st.session_state.get("gt_data", pd.DataFrame())

    if gt_data.empty:
        st.warning(
            "\u0421\u043f\u043e\u0447\u0430\u0442\u043a\u0443 \u0437\u0456\u0431\u0435\u0440\u0456\u0442\u044c "
            "\u0434\u0430\u043d\u0456 \u0443 \u0440\u043e\u0437\u0434\u0456\u043b\u0456 "
            "'\u0417\u0456\u0431\u0440\u0430\u0442\u0438 \u0434\u0430\u043d\u0456'"
        )
        return

    # Line chart
    fig = px.line(
        gt_data, x=gt_data.index, y=gt_data.columns,
        title="\u0406\u043d\u0442\u0435\u0440\u0435\u0441 \u0437\u0430 \u0447\u0430\u0441\u043e\u043c (Google Trends, \u0423\u043a\u0440\u0430\u0457\u043d\u0430)",
        labels={"value": "\u0406\u043d\u0442\u0435\u0440\u0435\u0441 (0-100)", "variable": "\u0417\u0430\u043f\u0438\u0442"},
        template="plotly_dark"
    )
    fig.update_layout(height=500, hovermode="x unified",
                      legend=dict(orientation="h", y=-0.3))
    st.plotly_chart(fig, use_container_width=True)

    # Heatmap
    if len(gt_data.columns) > 1:
        fig2 = px.imshow(
            gt_data.T, aspect="auto", color_continuous_scale="YlOrRd",
            labels=dict(x="\u0414\u0430\u0442\u0430", y="\u0417\u0430\u043f\u0438\u0442", color="\u0406\u043d\u0442\u0435\u0440\u0435\u0441"),
            title="\u0422\u0435\u043f\u043b\u043e\u0432\u0430 \u043a\u0430\u0440\u0442\u0430"
        )
        fig2.update_layout(height=400)
        st.plotly_chart(fig2, use_container_width=True)

    # Rising trends
    analyzer = TrendAnalyzer()
    rising = analyzer.get_rising_trends(gt_data)
    if not rising.empty:
        st.subheader(
            "\U0001f525 \u0417\u0440\u043e\u0441\u0442\u0430\u044e\u0447\u0456 \u0437\u0430\u043f\u0438\u0442\u0438"
        )
        fig3 = px.bar(
            rising, x="keyword", y="growth_pct",
            color="growth_pct", color_continuous_scale="RdYlGn",
            title="\u0417\u043c\u0456\u043d\u0430 \u0456\u043d\u0442\u0435\u0440\u0435\u0441\u0443 (%)",
            template="plotly_dark"
        )
        st.plotly_chart(fig3, use_container_width=True)
        st.dataframe(rising, use_container_width=True)


# =============================================
# PAGE: Recommendations
# =============================================
def page_recommendations():
    st.header(
        "\U0001f4a1 \u0420\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u0430\u0446\u0456\u0457 "
        "\u0434\u043b\u044f \u0456\u043d\u0442\u0435\u0440\u043d\u0435\u0442-\u043c\u0430\u0433\u0430\u0437\u0438\u043d\u0443"
    )

    gt_data = st.session_state.get("gt_data", pd.DataFrame())
    df = get_cached_products()

    analyzer = TrendAnalyzer()
    recs = analyzer.generate_recommendations(gt_data, df)

    for i, rec in enumerate(recs, 1):
        icon = "\U0001f7e2" if rec.get("priority") == "high" else "\U0001f7e1"
        with st.expander("{} {}".format(icon, rec["title"]), expanded=(i <= 3)):
            st.write(rec["description"])
            if "keywords" in rec:
                kw_str = ", ".join(["`{}`".format(k) for k in rec["keywords"]])
                st.markdown(
                    "**\u041a\u043b\u044e\u0447\u043e\u0432\u0456 \u0441\u043b\u043e\u0432\u0430:** " + kw_str
                )
            if "action" in rec:
                st.success("\u2705 \u0414\u0456\u044f: {}".format(rec["action"]))

    # Additional insights from product data
    if not df.empty:
        st.divider()
        st.subheader(
            "\U0001f4ca \u0406\u043d\u0441\u0430\u0439\u0442\u0438 \u0437 \u0434\u0430\u043d\u0438\u0445"
        )

        c1, c2 = st.columns(2)

        with c1:
            # Best price-to-rating ratio
            df_rated = df[(df["rating"] > 0) & (df["price"] > 0)].copy()
            if not df_rated.empty:
                df_rated["value_score"] = df_rated["rating"] * df_rated["reviews_count"] / (df_rated["price"] + 1)
                top_value = df_rated.nlargest(10, "value_score")
                st.markdown("**\U0001f947 \u041d\u0430\u0439\u043a\u0440\u0430\u0449\u0435 \u0441\u043f\u0456\u0432\u0432\u0456\u0434\u043d\u043e\u0448\u0435\u043d\u043d\u044f \u0446\u0456\u043d\u0430/\u044f\u043a\u0456\u0441\u0442\u044c:**")
                for _, row in top_value.iterrows():
                    st.write("- {} ({:.0f}\u20b4, \u2b50{:.1f}, {} \u0432\u0456\u0434\u0433\u0443\u043a\u0456\u0432)".format(
                        row["name"][:60], row["price"], row["rating"], row["reviews_count"]
                    ))

        with c2:
            # Price gaps
            if "category" in df.columns:
                st.markdown("**\U0001f4b0 \u0426\u0456\u043d\u043e\u0432\u0456 \u043d\u0456\u0448\u0456 \u0437\u0430 \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0456\u044f\u043c\u0438:**")
                for cat in df["category"].unique():
                    cat_df = df[df["category"] == cat]
                    if len(cat_df) > 0:
                        st.write(
                            "- **{}**: {:.0f}\u20b4 - {:.0f}\u20b4 (\u0441\u0435\u0440. {:.0f}\u20b4)".format(
                                cat,
                                cat_df["price"].min(),
                                cat_df["price"].max(),
                                cat_df["price"].mean()
                            )
                        )


# =============================================
# PAGE: Export
# =============================================
def page_export():
    st.header(
        "\U0001f4e5 \u0415\u043a\u0441\u043f\u043e\u0440\u0442 \u0434\u0430\u043d\u0438\u0445"
    )

    df = get_cached_products()

    if df.empty:
        st.warning(
            "\u041d\u0435\u043c\u0430\u0454 \u0434\u0430\u043d\u0438\u0445 \u0434\u043b\u044f \u0435\u043a\u0441\u043f\u043e\u0440\u0442\u0443."
        )
        return

    st.metric(
        "\u0412\u0441\u044c\u043e\u0433\u043e \u0442\u043e\u0432\u0430\u0440\u0456\u0432 \u0443 \u0431\u0430\u0437\u0456:",
        len(df)
    )

    c1, c2, c3 = st.columns(3)

    with c1:
        csv = df.to_csv(index=False).encode("utf-8")
        st.download_button(
            label="\U0001f4c4 \u0417\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0438\u0442\u0438 CSV",
            data=csv,
            file_name="ua_market_products_{}.csv".format(
                datetime.now().strftime("%Y%m%d")
            ),
            mime="text/csv",
            use_container_width=True
        )

    with c2:
        json_str = df.to_json(orient="records", force_ascii=False, indent=2)
        st.download_button(
            label="\U0001f4c4 \u0417\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0438\u0442\u0438 JSON",
            data=json_str,
            file_name="ua_market_products_{}.json".format(
                datetime.now().strftime("%Y%m%d")
            ),
            mime="application/json",
            use_container_width=True
        )

    with c3:
        try:
            buffer = pd.ExcelWriter("temp_export.xlsx", engine="openpyxl")
            df.to_excel(buffer, index=False, sheet_name="Products")
            buffer.close()
            with open("temp_export.xlsx", "rb") as f:
                st.download_button(
                    label="\U0001f4c4 \u0417\u0430\u0432\u0430\u043d\u0442\u0430\u0436\u0438\u0442\u0438 Excel",
                    data=f,
                    file_name="ua_market_products_{}.xlsx".format(
                        datetime.now().strftime("%Y%m%d")
                    ),
                    mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    use_container_width=True
                )
            os.remove("temp_export.xlsx")
        except Exception:
            st.info("Excel \u0435\u043a\u0441\u043f\u043e\u0440\u0442: pip install openpyxl")

    # Preview
    st.subheader(
        "\U0001f440 \u041f\u043e\u043f\u0435\u0440\u0435\u0434\u043d\u0456\u0439 \u043f\u0435\u0440\u0435\u0433\u043b\u044f\u0434"
    )
    st.dataframe(df.head(50), use_container_width=True)

    # Clear cache option
    st.divider()
    if st.button(
        "\U0001f5d1\ufe0f \u041e\u0447\u0438\u0441\u0442\u0438\u0442\u0438 \u0431\u0430\u0437\u0443 \u0434\u0430\u043d\u0438\u0445",
        type="secondary"
    ):
        try:
            if os.path.exists(DB_PATH):
                os.remove(DB_PATH)
            st.success("\u2705 \u0411\u0430\u0437\u0443 \u043e\u0447\u0438\u0449\u0435\u043d\u043e")
            st.rerun()
        except Exception as e:
            st.error(str(e))


if __name__ == "__main__":
    main()
