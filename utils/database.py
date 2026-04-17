# -*- coding: utf-8 -*-
import sqlite3
import os
import pandas as pd
from datetime import datetime


class Database:
    def __init__(self, db_path="data/trends.db"):
        self.db_path = db_path
        os.makedirs(os.path.dirname(db_path), exist_ok=True)
        self._init_db()

    def _init_db(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS product_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT,
                price REAL,
                old_price REAL,
                rating REAL,
                reviews_count INTEGER,
                category TEXT,
                rozetka_category TEXT DEFAULT '',
                source TEXT,
                url TEXT,
                image TEXT DEFAULT '',
                brand TEXT DEFAULT '',
                seller TEXT DEFAULT '',
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS trend_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                keyword TEXT,
                date TEXT,
                interest INTEGER,
                category TEXT,
                source TEXT DEFAULT 'google_trends',
                collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        conn.close()

    def save_products(self, df):
        if df.empty:
            return
        conn = sqlite3.connect(self.db_path)
        now = datetime.now().isoformat()
        for _, row in df.iterrows():
            conn.execute("""
                INSERT INTO product_snapshots
                (name, price, old_price, rating, reviews_count,
                 category, rozetka_category, source, url, image, brand, seller, collected_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                str(row.get("name", "")),
                float(row.get("price", 0) or 0),
                float(row.get("old_price", 0) or 0),
                float(row.get("rating", 0) or 0),
                int(row.get("reviews_count", 0) or 0),
                str(row.get("category", "")),
                str(row.get("rozetka_category", "")),
                str(row.get("source", "")),
                str(row.get("url", "")),
                str(row.get("image", "")),
                str(row.get("brand", "")),
                str(row.get("seller", "")),
                now,
            ))
        conn.commit()
        conn.close()

    def save_trends(self, df, source="google_trends"):
        if df.empty:
            return
        conn = sqlite3.connect(self.db_path)
        now = datetime.now().isoformat()
        for col in df.columns:
            for date_val, interest in df[col].items():
                conn.execute("""
                    INSERT INTO trend_snapshots
                    (keyword, date, interest, category, source, collected_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (col, str(date_val), int(interest), "", source, now))
        conn.commit()
        conn.close()

    def get_all_products(self):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * FROM product_snapshots", conn)
        conn.close()
        return df

    def get_all_trends(self):
        conn = sqlite3.connect(self.db_path)
        df = pd.read_sql_query("SELECT * FROM trend_snapshots", conn)
        conn.close()
        return df

    def get_products_count(self):
        conn = sqlite3.connect(self.db_path)
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM product_snapshots")
        count = cur.fetchone()[0]
        conn.close()
        return count

    def clear_products(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("DELETE FROM product_snapshots")
        conn.commit()
        conn.close()

    def clear_all(self):
        conn = sqlite3.connect(self.db_path)
        conn.execute("DELETE FROM product_snapshots")
        conn.execute("DELETE FROM trend_snapshots")
        conn.commit()
        conn.close()
