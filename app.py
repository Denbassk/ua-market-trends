# -*- coding: utf-8 -*-
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
import sqlite3
import json

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
# CONFIG
# =============================================
import yaml
from yaml.loader import SafeLoader

st.set_page_config(
    page_title="UA Market Trends",
    page_icon="\U0001f4ca",
    layout="wide",
    initial_sidebar_state="expanded"
)
# --- AUTH ---
def check_password():
    if st.session_state.get("authenticated"):
        return True

    st.markdown("## \U0001f512 \u0412\u0445\u0456\u0434")
    username = st.text_input("\u041b\u043e\u0433\u0456\u043d", key="login_user")
    password = st.text_input("\u041f\u0430\u0440\u043e\u043b\u044c", type="password", key="login_pass")

    if st.button("\u0412\u0432\u0456\u0439\u0442\u0438", key="login_btn"):
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
                    st.error("\u041d\u0435\u0432\u0456\u0440\u043d\u0438\u0439 \u043f\u0430\u0440\u043e\u043b\u044c")
            else:
                st.error("\u041a\u043e\u0440\u0438\u0441\u0442\u0443\u0432\u0430\u0447\u0430 \u043d\u0435 \u0437\u043d\u0430\u0439\u0434\u0435\u043d\u043e")
    return False

if not check_password():
    st.stop()

st.sidebar.write(f'\u041f\u0440\u0438\u0432\u0456\u0442, **{st.session_state.get("name", "")}**')
if st.sidebar.button("\u0412\u0438\u0439\u0442\u0438"):
    st.session_state["authenticated"] = False
    st.rerun()
# --- END AUTH ---

st.markdown("""
<style>
    .main-header {
        font-size: 2.2rem; font-weight: 700; text-align: center;
        padding: 0.8rem 0;
        background: linear-gradient(90deg, #0057b7 0%, #ffd700 100%);
        -webkit-background-clip: text; -webkit-text-fill-color: transparent;
    }
    .stat-box {
        background: #1e1e2e; border-radius: 12px; padding: 1.2rem;
        text-align: center; border: 1px solid #333;
    }
    .stat-box h2 { margin: 0; font-size: 2rem; color: #ffd700; }
    .stat-box p { margin: 0; color: #aaa; font-size: 0.85rem; }
    div[data-testid="stSidebar"] {
        background: linear-gradient(180deg, #0d1117 0%, #161b22 100%);
    }
</style>
""", unsafe_allow_html=True)

CATEGORIES = {
    "\U0001f35e \u041f\u0440\u043e\u0434\u0443\u043a\u0442\u0438 \u0445\u0430\u0440\u0447\u0443\u0432\u0430\u043d\u043d\u044f": "food",
    "\U0001f3e0 \u0422\u043e\u0432\u0430\u0440\u0438 \u0434\u043b\u044f \u0434\u043e\u043c\u0443": "home",
    "\U0001f484 \u041a\u043e\u0441\u043c\u0435\u0442\u0438\u043a\u0430": "cosmetics",
    "\U0001f4f1 \u0415\u043b\u0435\u043a\u0442\u0440\u043e\u043d\u0456\u043a\u0430": "electronics",
    "\U0001f476 \u0414\u0438\u0442\u044f\u0447\u0456 \u0442\u043e\u0432\u0430\u0440\u0438": "kids",
    "\U0001f43e \u0417\u043e\u043e\u0442\u043e\u0432\u0430\u0440\u0438": "pets",
    "\U0001f50b \u0415\u043d\u0435\u0440\u0433\u043e\u0430\u0432\u0442\u043e\u043d\u043e\u043c\u043d\u0456\u0441\u0442\u044c": "energy",
    "\U0001f48a \u0417\u0434\u043e\u0440\u043e\u0432'\u044f": "health",
}

PERIOD_MAP = {
    "\u041e\u0441\u0442\u0430\u043d\u043d\u0456 7 \u0434\u043d\u0456\u0432": "now 7-d",
    "\u041e\u0441\u0442\u0430\u043d\u043d\u0456 30 \u0434\u043d\u0456\u0432": "today 1-m",
    "\u041e\u0441\u0442\u0430\u043d\u043d\u0456 90 \u0434\u043d\u0456\u0432": "today 3-m",
    "\u041e\u0441\u0442\u0430\u043d\u043d\u0456 6 \u043c\u0456\u0441\u044f\u0446\u0456\u0432": "today 6-m",
    "\u041e\u0441\u0442\u0430\u043d\u043d\u0456\u0439 \u0440\u0456\u043a": "today 12-m",
}

DB_PATH = "data/trends.db"



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
    """Save products to local cache."""
    if df.empty:
        return
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    now = datetime.now().isoformat()
    for _, row in df.iterrows():
        try:
            conn.execute(
                "INSERT INTO product_snapshots "
                "(source, name, price, rating, reviews_count, category, url, collected_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    source,
                    str(row.get("name", "")),
                    float(row.get("price", 0) or 0),
                    float(row.get("rating", 0) or 0),
                    int(row.get("reviews_count", 0) or 0),
                    str(row.get("category", "")),
                    str(row.get("url", "")),
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
    st.set_page_config(page_title="UA Market Trends", page_icon="\U0001f1fa\U0001f1e6", layout="wide")

    if not check_password():
        st.stop()
    st.markdown(
        '<p class="main-header">\U0001f1fa\U0001f1e6 UA Market Trends Analyzer</p>',
        unsafe_allow_html=True
    )


    # SIDEBAR
    with st.sidebar:
        st.image("https://flagcdn.com/w320/ua.png", width=50)

        # Cache stats
        stats = get_cache_stats()
        st.markdown(
            "\U0001f4be **\u041a\u0435\u0448:** {} \u0442\u043e\u0432\u0430\u0440\u0456\u0432 | "
            "\u041e\u043d\u043e\u0432\u043b\u0435\u043d\u043e: {}".format(
                stats["products"], stats["last_update"]
            )
        )
        st.divider()

        page = st.radio(
            "\U0001f4cb \u0420\u043e\u0437\u0434\u0456\u043b:",
            [
                "\U0001f680 \u0417\u0456\u0431\u0440\u0430\u0442\u0438 \u0434\u0430\u043d\u0456",
                "\U0001f4ca \u0410\u043d\u0430\u043b\u0456\u0442\u0438\u043a\u0430 \u0442\u043e\u0432\u0430\u0440\u0456\u0432",
                "\U0001f4c8 \u0422\u0440\u0435\u043d\u0434\u0438 Google",
                "\U0001f4a1 \u0420\u0435\u043a\u043e\u043c\u0435\u043d\u0434\u0430\u0446\u0456\u0457",
                "\U0001f4e5 \u0415\u043a\u0441\u043f\u043e\u0440\u0442 \u0434\u0430\u043d\u0438\u0445",
            ]
        )

    if page.startswith("\U0001f680"):
        page_collect()
    elif page.startswith("\U0001f4ca"):
        page_analytics()
    elif page.startswith("\U0001f4c8"):
        page_trends()
    elif page.startswith("\U0001f4a1"):
        page_recommendations()
    elif page.startswith("\U0001f4e5"):
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
            default=[list(CATEGORIES.keys())[0], list(CATEGORIES.keys())[1]]
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
            cached = apify.get_last_dataset(max_items=max_products * len(selected))
            if not cached.empty:
                save_products_to_cache(cached, "rozetka")
                st.success("\u2705 \u0417\u0431\u0435\u0440\u0435\u0436\u0435\u043d\u043e {} \u0442\u043e\u0432\u0430\u0440\u0456\u0432 \u0443 \u0431\u0430\u0437\u0443".format(len(cached)))
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
# PAGE: Product Analytics
# =============================================
def page_analytics():
    st.header("\U0001f4ca \u0410\u043d\u0430\u043b\u0456\u0442\u0438\u043a\u0430 \u0442\u043e\u0432\u0430\u0440\u0456\u0432")

    # Load from cache
    df = get_cached_products()

    if df.empty:
        st.warning(
            "\u041d\u0435\u043c\u0430\u0454 \u0434\u0430\u043d\u0438\u0445. "
            "\u041f\u0435\u0440\u0435\u0439\u0434\u0456\u0442\u044c \u0443 "
            "'\u0417\u0456\u0431\u0440\u0430\u0442\u0438 \u0434\u0430\u043d\u0456' \u0441\u043f\u043e\u0447\u0430\u0442\u043a\u0443."
        )
        return

    # ---- FILTERS ----
    st.subheader("\U0001f50d \u0424\u0456\u043b\u044c\u0442\u0440\u0438")

    fc1, fc2, fc3, fc4 = st.columns(4)

    with fc1:
        cats = ["(\u0432\u0441\u0456)"] + sorted(df["category"].unique().tolist())
        sel_cat = st.selectbox("\u041a\u0430\u0442\u0435\u0433\u043e\u0440\u0456\u044f:", cats)

    with fc2:
        sources = ["(\u0432\u0441\u0456)"] + sorted(df["source"].unique().tolist())
        sel_src = st.selectbox("\u0414\u0436\u0435\u0440\u0435\u043b\u043e:", sources)

    with fc3:
        price_range = st.slider(
            "\u0426\u0456\u043d\u0430 (\u20b4):",
            min_value=0,
            max_value=int(df["price"].max()) + 1 if df["price"].max() > 0 else 100,
            value=(0, int(df["price"].max()) + 1 if df["price"].max() > 0 else 100)
        )

    with fc4:
        min_rating = st.slider("\u041c\u0456\u043d. \u0440\u0435\u0439\u0442\u0438\u043d\u0433:", 0.0, 5.0, 0.0, 0.1)

    # Search
    search = st.text_input(
        "\U0001f50e \u041f\u043e\u0448\u0443\u043a \u0437\u0430 \u043d\u0430\u0437\u0432\u043e\u044e:",
        placeholder="\u0432\u0432\u0435\u0434\u0456\u0442\u044c \u043d\u0430\u0437\u0432\u0443 \u0442\u043e\u0432\u0430\u0440\u0443..."
    )

    # Apply filters
    filtered = df.copy()
    if sel_cat != "(\u0432\u0441\u0456)":
        filtered = filtered[filtered["category"] == sel_cat]
    if sel_src != "(\u0432\u0441\u0456)":
        filtered = filtered[filtered["source"] == sel_src]
    filtered = filtered[
        (filtered["price"] >= price_range[0]) &
        (filtered["price"] <= price_range[1])
    ]
    if min_rating > 0:
        filtered = filtered[filtered["rating"] >= min_rating]
    if search:
        filtered = filtered[
            filtered["name"].str.contains(search, case=False, na=False)
        ]

    # Remove duplicates by name
    filtered = filtered.drop_duplicates(subset=["name"], keep="first")

    # ---- SORT ----
    sort_options = {
        "\u0420\u0435\u0439\u0442\u0438\u043d\u0433 \u2b07": ("rating", False),
        "\u0412\u0456\u0434\u0433\u0443\u043a\u0438 \u2b07": ("reviews_count", False),
        "\u0426\u0456\u043d\u0430 \u2b06": ("price", True),
        "\u0426\u0456\u043d\u0430 \u2b07": ("price", False),
        "\u041d\u0430\u0437\u0432\u0430 A-Z": ("name", True),
    }
    sort_label = st.selectbox(
        "\u0421\u043e\u0440\u0442\u0443\u0432\u0430\u043d\u043d\u044f:",
        list(sort_options.keys())
    )
    sort_col, sort_asc = sort_options[sort_label]
    if sort_col in filtered.columns:
        filtered = filtered.sort_values(sort_col, ascending=sort_asc)

    # ---- STATS ----
    st.divider()

    c1, c2, c3, c4, c5 = st.columns(5)
    with c1:
        st.metric(
            "\U0001f4e6 \u0422\u043e\u0432\u0430\u0440\u0456\u0432",
            len(filtered)
        )
    with c2:
        avg_price = filtered["price"].mean() if len(filtered) > 0 else 0
        st.metric(
            "\U0001f4b0 \u0421\u0435\u0440. \u0446\u0456\u043d\u0430",
            "{:.0f} \u20b4".format(avg_price)
        )
    with c3:
        avg_rating = filtered["rating"].mean() if len(filtered) > 0 else 0
        st.metric(
            "\u2b50 \u0421\u0435\u0440. \u0440\u0435\u0439\u0442\u0438\u043d\u0433",
            "{:.1f}".format(avg_rating)
        )
    with c4:
        med_price = filtered["price"].median() if len(filtered) > 0 else 0
        st.metric(
            "\U0001f4ca \u041c\u0435\u0434\u0456\u0430\u043d\u0430 \u0446\u0456\u043d\u0438",
            "{:.0f} \u20b4".format(med_price)
        )
    with c5:
        cats_count = filtered["category"].nunique() if len(filtered) > 0 else 0
        st.metric(
            "\U0001f4c2 \u041a\u0430\u0442\u0435\u0433\u043e\u0440\u0456\u0439",
            cats_count
        )

    # ---- CHARTS ----
    st.divider()

    chart_tab1, chart_tab2, chart_tab3 = st.tabs([
        "\U0001f4ca \u0413\u0440\u0430\u0444\u0456\u043a\u0438",
        "\U0001f4cb \u0422\u0430\u0431\u043b\u0438\u0446\u044f",
        "\U0001f3c6 \u0422\u043e\u043f-20 \u043a\u0430\u0440\u0442\u043a\u0438",
    ])

    with chart_tab1:
        render_charts(filtered)

    with chart_tab2:
        render_table(filtered)

    with chart_tab3:
        render_cards(filtered)


def render_charts(df):
    if df.empty:
        return

    c1, c2 = st.columns(2)

    with c1:
        # Price distribution
        fig = px.histogram(
            df, x="price", nbins=40,
            title="\U0001f4b0 \u0420\u043e\u0437\u043f\u043e\u0434\u0456\u043b \u0446\u0456\u043d",
            labels={"price": "\u0426\u0456\u043d\u0430 (\u20b4)", "count": "\u041a\u0456\u043b\u044c\u043a\u0456\u0441\u0442\u044c"},
            color_discrete_sequence=["#667eea"],
            template="plotly_dark"
        )
        st.plotly_chart(fig, use_container_width=True)

    with c2:
        # By category
        if "category" in df.columns:
            cat_counts = df["category"].value_counts().reset_index()
            cat_counts.columns = ["category", "count"]
            fig2 = px.pie(
                cat_counts, names="category", values="count",
                title="\U0001f4c2 \u0422\u043e\u0432\u0430\u0440\u0438 \u0437\u0430 \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0456\u044f\u043c\u0438",
                template="plotly_dark"
            )
            st.plotly_chart(fig2, use_container_width=True)

    # Price vs Rating scatter
    if "rating" in df.columns and df["rating"].sum() > 0:
        df_rated = df[df["rating"] > 0].copy()
        if not df_rated.empty:
            fig3 = px.scatter(
                df_rated, x="price", y="rating",
                size="reviews_count" if "reviews_count" in df_rated.columns else None,
                color="category",
                hover_name="name",
                title="\u0426\u0456\u043d\u0430 vs \u0420\u0435\u0439\u0442\u0438\u043d\u0433",
                labels={
                    "price": "\u0426\u0456\u043d\u0430 (\u20b4)",
                    "rating": "\u0420\u0435\u0439\u0442\u0438\u043d\u0433",
                    "reviews_count": "\u0412\u0456\u0434\u0433\u0443\u043a\u0438"
                },
                template="plotly_dark"
            )
            st.plotly_chart(fig3, use_container_width=True)

    # Top brands
    if "brand" in df.columns:
        brands = df[df["brand"].astype(str).str.len() > 1]
        if not brands.empty:
            top_brands = brands["brand"].value_counts().head(15).reset_index()
            top_brands.columns = ["brand", "count"]
            fig4 = px.bar(
                top_brands, x="count", y="brand", orientation="h",
                title="\U0001f3c6 \u0422\u043e\u043f-15 \u0431\u0440\u0435\u043d\u0434\u0456\u0432",
                labels={"count": "\u041a\u0456\u043b\u044c\u043a\u0456\u0441\u0442\u044c", "brand": ""},
                color="count", color_continuous_scale="Viridis",
                template="plotly_dark"
            )
            st.plotly_chart(fig4, use_container_width=True)

    # Category comparison
    if "category" in df.columns:
        cat_stats = df.groupby("category").agg(
            count=("name", "count"),
            avg_price=("price", "mean"),
            avg_rating=("rating", "mean"),
            max_price=("price", "max"),
            min_price=("price", "min"),
        ).reset_index()

        fig5 = px.bar(
            cat_stats, x="category", y="avg_price",
            title="\u0421\u0435\u0440\u0435\u0434\u043d\u044f \u0446\u0456\u043d\u0430 \u0437\u0430 \u043a\u0430\u0442\u0435\u0433\u043e\u0440\u0456\u044f\u043c\u0438",
            labels={"avg_price": "\u0421\u0435\u0440. \u0446\u0456\u043d\u0430 (\u20b4)", "category": ""},
            color="avg_rating", color_continuous_scale="RdYlGn",
            template="plotly_dark"
        )
        st.plotly_chart(fig5, use_container_width=True)


def render_table(df):
    if df.empty:
        return

    # Items per page
    per_page = st.selectbox(
        "\u0422\u043e\u0432\u0430\u0440\u0456\u0432 \u043d\u0430 \u0441\u0442\u043e\u0440\u0456\u043d\u0446\u0456:",
        [25, 50, 100, 200, 500],
        index=1,
        key="items_per_page"
    )

    total = len(df)
    total_pages = max(1, (total + per_page - 1) // per_page)

    page_num = st.number_input(
        "\u0421\u0442\u043e\u0440\u0456\u043d\u043a\u0430:",
        min_value=1, max_value=total_pages, value=1, key="page_num"
    )

    start = (page_num - 1) * per_page
    end = start + per_page
    page_df = df.iloc[start:end]

    st.caption(
        "\u041f\u043e\u043a\u0430\u0437\u0430\u043d\u043e {}-{} \u0437 {} \u0442\u043e\u0432\u0430\u0440\u0456\u0432 "
        "| \u0421\u0442\u043e\u0440\u0456\u043d\u043a\u0430 {}/{}".format(
            start + 1, min(end, total), total, page_num, total_pages
        )
    )

    # Show columns
    display_cols = ["name", "price", "rating", "reviews_count", "category", "source"]
    if "brand" in page_df.columns:
        display_cols.insert(2, "brand")

    show_cols = [c for c in display_cols if c in page_df.columns]
    st.dataframe(
        page_df[show_cols],
        use_container_width=True,
        height=600,
        column_config={
            "name": st.column_config.TextColumn("\u041d\u0430\u0437\u0432\u0430", width="large"),
            "price": st.column_config.NumberColumn("\u0426\u0456\u043d\u0430 \u20b4", format="%.0f"),
            "rating": st.column_config.NumberColumn("\u0420\u0435\u0439\u0442\u0438\u043d\u0433", format="%.1f"),
            "reviews_count": st.column_config.NumberColumn("\u0412\u0456\u0434\u0433\u0443\u043a\u0438"),
            "category": st.column_config.TextColumn("\u041a\u0430\u0442\u0435\u0433\u043e\u0440\u0456\u044f"),
            "brand": st.column_config.TextColumn("\u0411\u0440\u0435\u043d\u0434"),
            "source": st.column_config.TextColumn("\u0414\u0436\u0435\u0440\u0435\u043b\u043e"),
        }
    )


def render_cards(df):
    if df.empty:
        return

    st.caption(
        "\u0422\u043e\u043f-20 \u0442\u043e\u0432\u0430\u0440\u0456\u0432 \u0437\u0430 \u043e\u0431\u0440\u0430\u043d\u0438\u043c \u0441\u043e\u0440\u0442\u0443\u0432\u0430\u043d\u043d\u044f\u043c"
    )

    for idx, (_, row) in enumerate(df.head(20).iterrows()):
        with st.container():
            a, b, c, d = st.columns([4, 1, 1, 1])
            with a:
                name = row.get("name", "N/A")
                brand = row.get("brand", "")
                source = row.get("source", "")
                st.markdown("**{}. {}**".format(idx + 1, name))
                parts = []
                if brand and str(brand) != "nan" and len(str(brand)) > 1:
                    parts.append(str(brand))
                parts.append(str(row.get("category", "")))
                parts.append(str(source))
                st.caption(" | ".join(parts))
            with b:
                p = row.get("price", 0)
                old_p = row.get("old_price", 0)
                st.metric(
                    "\u0426\u0456\u043d\u0430",
                    "{:.0f} \u20b4".format(p),
                    delta="-{:.0f} \u20b4".format(old_p - p) if old_p > p else None,
                    delta_color="normal"
                )
            with c:
                r = row.get("rating", 0)
                st.metric("\u0420\u0435\u0439\u0442\u0438\u043d\u0433", "\u2b50 {:.1f}".format(r) if r > 0 else "-")
            with d:
                rv = row.get("reviews_count", 0)
                st.metric(
                    "\u0412\u0456\u0434\u0433\u0443\u043a\u0438",
                    "{:,}".format(rv).replace(",", " ") if rv > 0 else "-"
                )
            st.divider()


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
