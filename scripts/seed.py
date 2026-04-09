"""
Seed script — loads data into all databases.

Usage:
    uv run python -m scripts.seed

Prerequisites:
    Run scripts.migrate first to create database structures.

What to implement in seed():
    Phase 1: Load products.json + customers.json into Postgres and MongoDB
    Phase 2: Initialize Redis inventory counters from Postgres product stock
    Phase 3: Build Neo4j co-purchase graph from historical_orders.json

Seed data files are in the seed_data/ directory.
"""

import os
from pathlib import Path
import json
from datetime import datetime
from decimal import Decimal
from typing import List, Optional
from sqlalchemy.orm import Session
from itertools import combinations
from sqlalchemy import text
from dotenv import load_dotenv
from ecommerce_pipeline.postgres_models import Customer, Order

load_dotenv()

SEED_DIR = Path(__file__).parent.parent / "seed_data"


def seed(engine, mongo_db, redis_client=None, neo4j_driver=None):
    """Load seed data into all databases.

    Add your seeding logic here incrementally as you progress through phases.

    Args:
        engine: SQLAlchemy engine connected to Postgres
        mongo_db: pymongo Database instance
        redis_client: redis.Redis instance or None (Phase 2+)
        neo4j_driver: neo4j.Driver instance or None (Phase 3)

    Tip: Use json.load() to read the files in seed_data/:
        products = json.load(open(SEED_DIR / "products.json"))
        customers = json.load(open(SEED_DIR / "customers.json"))
        historical_orders = json.load(open(SEED_DIR / "historical_orders.json"))
    """
    #pass  # TODO: Phase 1 — load products and customers into Postgres + MongoDB
    # טעינת קבצי ה-JSON
    products = json.load(open(SEED_DIR / "products.json"))
    customers = json.load(open(SEED_DIR / "customers.json"))
    historical_orders = json.load(open(SEED_DIR / "historical_orders.json"))

    # מילון עזר לשליפת מחיר ושם מוצר לפי ID
    product_lookup = {p["id"]: p for p in products}

    with Session(engine) as session:
        # ניקוי נתונים קיימים (לפי המבנה החדש - ללא OrderItem)
        session.query(Order).delete()
        session.query(Customer).delete()
        mongo_db.products.delete_many({})
        mongo_db.order_history.delete_many({})
        if redis_client:
            redis_client.flushdb()

        print("Seeding Customers to Postgres...")
        for c in customers:
            customer = Customer(
                customer_id=c["id"],
                name=c["name"],
                email=c["email"]
            )
            session.add(customer)
        session.flush()

        print("Seeding Products to MongoDB & Redis...")
        if products:
            # 1. שמירה במונגו
            mongo_db.product_catalog.insert_many(products)

            for p in products:
            # מפתח נקי למלאי בלבד - לא מקבל TTL
                redis_client.set(f"inventory:{p['id']}", p.get('stock_quantity', 0))

        print("Seeding Historical Orders (Hybrid Model)...")
        print("Seeding neo4j...")
        for o in historical_orders:
            order_date = datetime.fromisoformat(o["created_at"].replace("Z", "+00:00"))
            p_ids = o.get("product_ids", [])
            
            # חישוב ה-Total והכנת ה-Snapshot למונגו
            order_total = Decimal("0.00")
            snapshot_items = []
            
            for p_id in o.get("product_ids", []):
                p_info = product_lookup.get(p_id)
                if p_info:
                    price = Decimal(str(p_info["price"]))
                    order_total += price
                    
                    snapshot_items.append({
                        "product_id": p_id,
                        "product_name": p_info["name"],
                        "unit_price": float(price),
                        "quantity": 1,
                        "category": p_info.get("category", "General")
                    })
    
            # א. יצירת ההזמנה ב-Postgres (כולל ה-Total שחושב)
            db_order = Order(
                order_id=o["order_id"],
                customer_id=o["customer_id"],
                total_amount=order_total,  # נשמר ב-SQL לדוחות
                created_at=order_date
            )
            session.add(db_order)
            
            # ב. שמירת ה-Snapshot המלא ל-MongoDB (היסטוריה)
            customer_info = next((c for c in customers if c["id"] == o["customer_id"]), None)
            
            mongo_db["order_snapshots"].insert_one({
                "order_id": o["order_id"],
                "customer": {
                    "id": o["customer_id"],
                    "name": customer_info["name"] if customer_info else "Unknown",
                    "email": customer_info["email"] if customer_info else ""
                },
                "items": snapshot_items,
                "total_amount": float(order_total),
                "status": "completed",  # סטטוס לדוגמה
                "created_at": o["created_at"]
            })

            
            # 4. בניית הגרף ב-Neo4j (החלק שפותר את בעיית השמות)
            if neo4j_driver and len(p_ids) > 1:
                with neo4j_driver.session() as neo_session:
                    for p1, p2 in combinations(sorted(p_ids), 2):
                        name1 = product_lookup.get(p1, {}).get("name", f"Product {p1}")
                        name2 = product_lookup.get(p2, {}).get("name", f"Product {p2}")
                        
                        neo_session.run("""
                            MERGE (a:Product {id: $p1}) SET a.name = $name1
                            MERGE (b:Product {id: $p2}) SET b.name = $name2
                            MERGE (a)-[r:BOUGHT_TOGETHER]-(b)
                            ON CREATE SET r.weight = 1
                            ON MATCH SET r.weight = r.weight + 1
                        """, p1=p1, name1=name1, p2=p2, name2=name2)
        
        session.commit()
        # סנכרון ה-Sequence לערך הכי גבוה שהוכנס ידנית
        session.execute(text("SELECT setval(pg_get_serial_sequence('orders', 'order_id'), coalesce(max(order_id), 1)) FROM orders;"))
        session.execute(text("SELECT setval(pg_get_serial_sequence('customers', 'customer_id'), coalesce(max(customer_id), 1)) FROM customers;"))
        session.commit()
    print("Seeding complete successfully!")
        

# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------

def _pg_url() -> str:
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = os.environ.get("POSTGRES_PORT", "5432")
    db = os.environ.get("POSTGRES_DB", "ecommerce")
    user = os.environ.get("POSTGRES_USER", "postgres")
    pwd = os.environ.get("POSTGRES_PASSWORD", "postgres")
    return f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"


def _mongo_db():
    from pymongo import MongoClient

    host = os.environ.get("MONGO_HOST", "localhost")
    port = int(os.environ.get("MONGO_PORT", "27017"))
    db = os.environ.get("MONGO_DB", "ecommerce")
    return MongoClient(host, port)[db]


def _redis_client():
    host = os.environ.get("REDIS_HOST")
    if not host:
        return None
    import redis

    port = int(os.environ.get("REDIS_PORT", "6379"))
    return redis.Redis(host=host, port=port, decode_responses=True)


def _neo4j_driver():
    host = os.environ.get("NEO4J_HOST")
    pwd = os.environ.get("NEO4J_PASSWORD")
    if not host or not pwd:
        return None
    from neo4j import GraphDatabase

    port = os.environ.get("NEO4J_BOLT_PORT", "7687")
    user = os.environ.get("NEO4J_USER", "neo4j")
    return GraphDatabase.driver(f"bolt://{host}:{port}", auth=(user, pwd))


def main():
    from sqlalchemy import create_engine, text

    engine = create_engine(_pg_url(), echo=False)
    mongo_db = _mongo_db()
    redis_client = _redis_client()
    neo4j_driver = _neo4j_driver()

    print("Seeding databases...")
    seed(engine, mongo_db, redis_client, neo4j_driver)
    print("Seeding complete.")

    if neo4j_driver:
        neo4j_driver.close()
    engine.dispose()


if __name__ == "__main__":
    main()
