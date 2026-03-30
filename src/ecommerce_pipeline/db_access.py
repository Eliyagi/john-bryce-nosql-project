"""
DBAccess — the data access layer.

This is one of the files you implement. The web API is already wired up;
every route calls one method on this class. Your job is to replace each
`raise NotImplementedError(...)` with a real implementation.

Work through the phases in order. Read the corresponding lesson file before
starting each phase.

You also implement scripts/migrate.py and scripts/seed.py alongside this file.
"""

from __future__ import annotations

import json
import logging
from itertools import combinations
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import neo4j
    import redis as redis_lib
    from pymongo.database import Database as MongoDatabase
    from sqlalchemy.orm import sessionmaker

    from ecommerce_pipeline.models.requests import OrderItemRequest
    from ecommerce_pipeline.models.responses import (
        CategoryRevenueResponse,
        OrderCustomerEmbed,
        OrderItemResponse,
        OrderResponse,
        OrderSnapshotResponse,
        ProductResponse,
        RecommendationResponse,
    )

logger = logging.getLogger(__name__)

from ecommerce_pipeline.models.responses import (
        CategoryRevenueResponse,
        OrderCustomerEmbed,
        OrderItemResponse,
        OrderResponse,
        OrderSnapshotResponse,
        ProductResponse,
        RecommendationResponse,
    )

class DBAccess:
    def __init__(
        self,
        pg_session_factory: sessionmaker,
        mongo_db: MongoDatabase,
        redis_client: redis_lib.Redis | None = None,
        neo4j_driver: neo4j.Driver | None = None,
    ) -> None:
        self._pg_session_factory = pg_session_factory
        self._mongo_db = mongo_db
        self._redis = redis_client
        self._neo4j = neo4j_driver

    # ── Phase 1 ───────────────────────────────────────────────────────────────

    def create_order(self, customer_id: int, items: list[OrderItemRequest]) -> OrderResponse:
        """Place an order atomically.

        See OrderItemRequest in models/requests.py for the input shape.
        See OrderResponse in models/responses.py for the return shape.

        Raises ValueError if any product has insufficient stock. When that
        happens, no data is modified in any database.

        After the order is persisted transactionally, a denormalized snapshot
        is saved for read access, and downstream counters and graph edges are
        updated (best-effort, does not roll back the order on failure).
        """
        # raise NotImplementedError("Phase 1: implement create_order")
        from decimal import Decimal
        from datetime import datetime
        from ecommerce_pipeline.postgres_models import Customer, Order
        

        pg_session_factory = self._pg_session_factory
    
        with pg_session_factory() as session:
            # 1. וולידציה שהלקוח קיים ב-Postgres
            customer = session.query(Customer).filter_by(customer_id=customer_id).first()
            if not customer:
                raise ValueError(f"Customer with ID {customer_id} does not exist.")

            total_amount = Decimal("0.00")
            snapshot_items = []

            # 2. בדיקת מלאי, קיום מוצר ואיסוף נתונים ל-Snapshot (מול MongoDB)
            for item in items:
                # חשוב: מחפשים לפי שדה "id" כפי שהגדרנו ב-Seed/Migrate
                product_doc = self._mongo_db.products.find_one({"id": item.product_id})
                
                if not product_doc:
                    raise ValueError(f"Product with ID {item.product_id} not found in catalog.")
                
                if product_doc.get("stock_quantity", 0) < item.quantity:
                    raise ValueError(f"Insufficient stock for product: {product_doc.get('name')}")

                # חישוב סכום
                item_price = Decimal(str(product_doc["price"]))
                total_amount += item_price * item.quantity

                # הכנת הפריט ל-Snapshot במונגו
                snapshot_items.append({
                    "product_id": item.product_id,
                    "product_name": product_doc["name"],
                    "unit_price": float(item_price),
                    "quantity": item.quantity
                })

            # 3. יצירת ההזמנה ב-Postgres (רק ה"שלד" עם ה-Total)
            order = Order(
                customer_id=customer_id, 
                total_amount=total_amount
            )
            session.add(order)
            session.flush()  # יצירת order_id לשימוש במונגו

            # 4. עדכון מלאי במונגו ושמירת ה-Snapshot
            for item in items:
                # עדכון מלאי אטומי במונגו
                self._mongo_db.products.update_one(
                    {"id": item.product_id},
                    {"$inc": {"stock_quantity": -item.quantity}}
                )

            # 5. שמירת ה-SNAPSHOT ב-MongoDB (היסטוריית הזמנות עשירה)
            # שים לב לשם הקולקציה: order_history (כפי שהגדרנו ב-Seed)
            self._mongo_db.order_history.insert_one({
                "order_id": order.order_id,
                "customer": {
                    "customer_id": customer.customer_id,
                    "name": customer.name,
                    "email": customer.email
                },
                "items": snapshot_items,
                "total_amount": float(total_amount),
                "created_at": datetime.now()
            })

            session.commit()

            session.refresh(order)

        return OrderResponse(
            order_id=order.order_id,
            customer_id=customer_id,
            total_amount=total_amount,
            created_at=order.created_at.isoformat(),
            # הוספת סטטוס ברירת מחדל (או מה שמוגדר ב-Schema שלך)
            status="completed", 
            # החזרת הפריטים מה-Snapshot שהכנת קודם למונגו
            items=snapshot_items
        )

    def get_product(self, product_id: int) -> ProductResponse | None:
        """Fetch a product by its integer ID.

        See ProductResponse in models/responses.py for the return shape.
        Returns None if not found.
        """
        #raise NotImplementedError("Phase 1: implement get_product")
        # 1. גישה לקולקשן במונגו
        products_col = self._mongo_db.products
        
        # 2. חיפוש לפי השדה 'id' (ה-ID העסקי מה-JSON)
        product_data = products_col.find_one({"id": product_id})
        
        if not product_data:
            return None
            
        # 3. מיפוי השדות ל-ProductResponse
        # שים לב למיפוי של stock_quantity ו-category_fields
        return ProductResponse(
            id=product_data["id"],
            name=product_data["name"],
            price=float(product_data["price"]),
            stock_quantity=product_data.get("stock", 0),  # מיפוי מ-'stock' ל-'stock_quantity'
            category=product_data.get("category", "General"),
            description=product_data.get("description", ""),
            # category_fields מכיל את כל השדות שלא מוגדרים קבוע (כמו 'specs' או שדות דינמיים אחרים)
            category_fields=product_data.get("category_fields", {}) 
        )

    def search_products(
        self,
        category: str | None = None,
        q: str | None = None,
    ) -> list[ProductResponse]:
        """Search the product catalog with optional filters.

        category: exact match on the category field
        q: case-insensitive substring match on the product name
        Both filters are ANDed together. Returns all products if both are None.
        """
        http://127.0.0.1:8000/products?category=electronics&q=Lap
        
        #raise NotImplementedError("Phase 1: implement search_products")
        # 1. בניית השאילתה הדינמית (כמו WHERE ב-SQL)
        query = {}
        
        # סינון לפי קטגוריה (Exact Match)
        if category:
            query["category"] = category
            
        # סינון לפי שם (Case-insensitive substring)
        if q:
            # $regex: חיפוש תת-מחרוזת, $options: 'i' הופך את זה ל-Case Insensitive
            query["name"] = {"$regex": q, "$options": "i"}
            
        # 2. ביצוע השאילתה ב-Collection של המוצרים
        products_cursor = self._mongo_db.products.find(query)
        
        # 3. המרת התוצאות לרשימה של ProductResponse
        results = []
        for p in products_cursor:
            results.append(
                ProductResponse(
                    id=p["id"],
                    name=p["name"],
                    price=float(p["price"]),
                    stock_quantity=p.get("stock", 0),  # מיפוי מ-'stock' ל-'stock_quantity'
                    category=p.get("category", "General"),
                    description=p.get("description", ""),
                    category_fields=p.get("category_fields", {})
                )
            )
            
        return results

    def save_order_snapshot(
        self,
        order_id: int,
        customer: OrderCustomerEmbed,
        items: list[OrderItemResponse],
        total_amount: float,
        status: str,
        created_at: str,
    ) -> str:
        """Save a denormalized order snapshot for fast read access.

        See OrderCustomerEmbed and OrderItemResponse in models/responses.py
        for the input shapes.

        Embeds all customer and product details as they existed at the time
        of the order, so the snapshot remains accurate even if prices or
        names change later.

        Returns a string identifier for the saved document.

        Called internally by create_order after the transactional write
        commits. Not called directly by routes.
        """
        raise NotImplementedError("Phase 1: implement save_order_snapshot")

    def get_order(self, order_id: int) -> OrderSnapshotResponse | None:
        """Fetch a single order snapshot by order_id.

        See OrderSnapshotResponse in models/responses.py for the return shape.
        Returns None if not found.
        """
        raise NotImplementedError("Phase 1: implement get_order")

    def get_order_history(self, customer_id: int) -> list[OrderSnapshotResponse]:
        """Fetch all order snapshots for a customer, sorted by created_at descending.

        Returns an empty list if the customer has no orders.
        """
        raise NotImplementedError("Phase 1: implement get_order_history")

    def revenue_by_category(self) -> list[CategoryRevenueResponse]:
        """Compute total revenue per product category, sorted by total_revenue descending.

        See CategoryRevenueResponse in models/responses.py for the return shape.
        """
        raise NotImplementedError("Phase 1: implement revenue_by_category")

    # ── Phase 2 ───────────────────────────────────────────────────────────────
    #
    # In this phase you also need to:
    #   - Update create_order to DECR Redis inventory counters after the
    #     Postgres transaction succeeds.
    #   - Optionally, add a fast pre-check: before starting the Postgres
    #     transaction, check the Redis counter. If it shows insufficient
    #     stock, fail fast without hitting Postgres.
    #   - Update scripts/seed.py to initialize inventory counters in Redis.
    #   - Add cache-aside logic to get_product (check Redis first, populate
    #     on miss with a 300-second TTL).

    def invalidate_product_cache(self, product_id: int) -> None:
        """Remove a product's cached entry.

        Call this after updating a product's data so the next read fetches
        fresh data from the primary store. No-op if no entry exists.
        """
        raise NotImplementedError("Phase 2: implement invalidate_product_cache")

    def record_product_view(self, customer_id: int, product_id: int) -> None:
        """Record that a customer viewed a product.

        Maintains a bounded, ordered list of the customer's most recently
        viewed products (most recent first, capped at 10 entries).
        """
        raise NotImplementedError("Phase 2: implement record_product_view")

    def get_recently_viewed(self, customer_id: int) -> list[int]:
        """Return up to 10 recently viewed product IDs for a customer.

        Returns IDs as integers, most recently viewed first.
        Returns an empty list if no views have been recorded.
        """
        raise NotImplementedError("Phase 2: implement get_recently_viewed")

    # ── Phase 3 ───────────────────────────────────────────────────────────────
    #
    # In this phase you also need to:
    #   - Update create_order to MERGE co-purchase edges in Neo4j for every
    #     pair of products in the order, incrementing the edge weight.
    #   - Update scripts/migrate.py to create Neo4j constraints.
    #   - Update scripts/seed.py to build the co-purchase graph from
    #     seed_data/historical_orders.json.

    def get_recommendations(self, product_id: int, limit: int = 5) -> list[RecommendationResponse]:
        """Return product recommendations based on co-purchase patterns.

        See RecommendationResponse in models/responses.py for the return shape.
        Sorted by score descending. Returns an empty list if no co-purchase relationships exist.
        """
        raise NotImplementedError("Phase 3: implement get_recommendations")
