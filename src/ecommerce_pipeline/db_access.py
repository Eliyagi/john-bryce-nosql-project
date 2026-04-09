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
        from itertools import combinations
        from ecommerce_pipeline.postgres_models import Customer, Order
        

        pg_session_factory = self._pg_session_factory
        keys_to_rollback = []  # רשימה לשמירת פעולות לביטול במידה והתהליך ייכשל
    
        with pg_session_factory() as session:
           # 1. וולידציה שהלקוח קיים ב-Postgres
            customer = session.query(Customer).filter_by(customer_id=customer_id).first()
            if not customer:
                raise ValueError(f"Customer with ID {customer_id} does not exist.")

            # 2. ניהול מלאי ב-Redis (רק אם קיים)
            if self._redis is not None:
                try:
                    for item in items:
                        inv_key = f"inventory:{item.product_id}"
                        new_stock = self._redis.decrby(inv_key, item.quantity)
                        keys_to_rollback.append((inv_key, item.quantity))

                        if new_stock < 0:
                            raise ValueError(f"Insufficient stock for product {item.product_id}")
                except ValueError as e:
                    for k, q in keys_to_rollback:
                        self._redis.incrby(k, q)
                    raise e

            total_amount = Decimal("0.00")
            snapshot_items = []

            try:
                # 3. שליפת נתונים ממונגו - תמיכה בשני שמות הקולקשנים
                for item in items:
                    product_doc = self._mongo_db.product_catalog.find_one({"id": item.product_id})
                    
                    if not product_doc:
                        raise ValueError(f"Product with ID {item.product_id} not found in catalog.")
                    
                    if product_doc["stock_quantity"] < item.quantity:
                        raise ValueError("Insufficient stock")

                    item_price = Decimal(str(product_doc["price"]))
                    total_amount += item_price * item.quantity

                    snapshot_items.append({
                        "product_id": item.product_id,
                        "product_name": product_doc["name"],
                        "unit_price": float(item_price),
                        "quantity": item.quantity,
                        "category": product_doc.get("category", "general")
                    })

                # 4. יצירת ההזמנה ב-Postgres
                order = Order(
                    customer_id=customer_id, 
                    total_amount=total_amount
                )
                session.add(order)
                session.flush()  # קבלת order_id

                # 5. עדכון מלאי במונגו (סינכרון ה-Master Record)
                for item in items:
                    self._mongo_db.product_catalog.update_one(
                        {"id": item.product_id},
                        {"$inc": {"stock_quantity": -item.quantity}}
                    )

                # 6. שמירת ה-SNAPSHOT ב-MongoDB
                self._mongo_db["order_snapshots"].insert_one({
                    "order_id": order.order_id,
                    "customer": {
                        "customer_id": customer.customer_id,
                        "name": customer.name,
                        "email": customer.email
                    },
                    "items": snapshot_items,
                    "total_amount": float(total_amount),
                    "status": "completed", 
                    "created_at": datetime.now()
                })

                # אישור סופי של הטרנזקציה ב-Postgres
                session.commit()
                session.refresh(order)

                # 1. נכין מילון עזר שמרכז ID לשם (מתוך מה ששלפנו קודם ממונגו)
                product_names = {item["product_id"]: item["product_name"] for item in snapshot_items}
                
                product_ids = [item.product_id for item in items]
                
                if self._neo4j and len(product_ids) > 1:
                    with self._neo4j.session() as neo_session:
                        # combinations מייצר זוגות ללא חשיבות לסדר
                        for p1, p2 in combinations(sorted(product_ids), 2):
                            neo_session.run("""
                                MERGE (a:Product {id: $p1_id})
                                ON CREATE SET a.name = $p1_name
                                
                                MERGE (b:Product {id: $p2_id})
                                ON CREATE SET b.name = $p2_name
                                
                                MERGE (a)-[r:BOUGHT_TOGETHER]-(b)
                                ON CREATE SET r.weight = 1
                                ON MATCH SET r.weight = r.weight + 1
                            """, 
                            p1_id=p1, p1_name=product_names[p1],
                            p2_id=p2, p2_name=product_names[p2])

            except Exception as e:
                # אם משהו נכשל ב-Postgres או ב-Mongo - מחזירים את המלאי ל-Redis!
                for k, q in keys_to_rollback:
                    self._redis.incrby(k, q)
                # ביטול הטרנזקציה ב-Postgres (קורה אוטומטית ביציאה מה-with בגלל השגיאה)
                raise e

        return OrderResponse(
            order_id=order.order_id,
            customer_id=customer_id,
            total_amount=total_amount,
            created_at=order.created_at.isoformat(),
            status="completed", 
            items=snapshot_items
        )

    def get_product(self, product_id: int) -> ProductResponse | None:
        """Fetch a product by its integer ID.

        See ProductResponse in models/responses.py for the return shape.
        Returns None if not found.
        """
        #raise NotImplementedError("Phase 1: implement get_product")
        
        """
        Cache-Aside:
        1. בדוק ב-Redis.
        2. אם קיים - החזר מיד.
        3. אם לא - משוך מ-Mongo, שמור ב-Redis ל-300 שניות, והחזר.
        """
        cache_key = f"product:{product_id}"

        # 1. ניסיון שליפה מ-Redis
        if self._redis is not None:
            try:
                cached_product = self._redis.get(cache_key)
                
                if cached_product:
                    # "פגיעה" ב-Cache: מחזירים את המידע במהירות שיא
                    return ProductResponse.model_validate_json(cached_product)
            except Exception:
                logger.warning(f"Redis error while fetching product {product_id}: {e}")
                # במקרה של תקלה ב-Redis, נמשיך לנסות לשלוף מ-Mongo בלי להחזיר שגיאה
                
        # 2. "פספוס" (Miss): הולכים למקור הנתונים (MongoDB)
        product_data = self._mongo_db.product_catalog.find_one({"id": product_id})
        
        if not product_data:
            return None

        # הפיכת הנתונים ממונגו לאובייקט Pydantic
        product = ProductResponse(**product_data)

        # 3. שמירה ב-Redis עם TTL של 300 שניות (כדי שהבא בתור ימצא את זה ב-Cache)
        # הפקודה setex עושה את השמירה והגדרת זמן התפוגה בפעולה אחת אטומית
        if self._redis is not None:
            try:
                self._redis.setex(
                    cache_key,
                    300,  # 5 דקות של "זיכרון"
                    product.model_dump_json()
                )
            except Exception:
                pass

        return product

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
        cursor = self._mongo_db.product_catalog.find(query)
        results = []
        for doc in cursor:
            doc.pop("_id", None)
            results.append(ProductResponse(**doc))
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
        #raise NotImplementedError("Phase 1: implement save_order_snapshot")

        # 1. הכנת המסמך (Document) לשמירה
        # אנחנו הופכים את אובייקטי ה-Pydantic למילונים (dict) כדי שמונגו יוכל לשמור אותם
        order_snapshot = {
            "order_id": order_id,
            "customer": customer.model_dump(),  # הופך את אובייקט הלקוח ל-JSON
            "items": [item.model_dump() for item in items],  # רשימת מוצרים כ-JSON
            "total_amount": total_amount,
            "status": status,
            "created_at": created_at,
            "snapshot_version": "1.0" # טיפ של מקצוענים: תמיד כדאי לשמור גרסה לסכימה במונגו
        }
        
        # 2. גישה לקולקשן 'orders' במונגו
        orders_col = self._mongo_db["order_snapshots"]
        
        # 3. הכנסה ל-DB
        result = orders_col.insert_one(order_snapshot)
        
        # 4. החזרת ה-ID שמונגו יצר (ה-_id האוטומטי) כמחרוזת
        return str(result.inserted_id)


    def get_order(self, order_id: int) -> OrderSnapshotResponse | None:
        """Fetch a single order snapshot by order_id.

        See OrderSnapshotResponse in models/responses.py for the return shape.
        Returns None if not found.
        """
        #raise NotImplementedError("Phase 1: implement get_order")
        # 1. חיבור לקולקשן של ההזמנות במונגו
        orders_col = self._mongo_db["order_snapshots"]
        
        # 2. חיפוש ההזמנה לפי ה-order_id (ה-ID מפוסטגרס)
        order_data = orders_col.find_one({"order_id": order_id})
        
        if not order_data:
            return None

         # תיקון שדה ה-customer (שינוי customer_id ל-id)
        customer_dict = order_data["customer"]
        if "customer_id" in customer_dict:
            customer_dict["id"] = customer_dict.pop("customer_id")

        # המרה של datetime למחרוזת (ISO format)
        created_at = order_data["created_at"]
        if not isinstance(created_at, str):
            created_at = created_at.isoformat()   

        # 3. החזרת אובייקט OrderSnapshotResponse
        # פיידנטיק יודע לעשות recursive mapping, כך שהמילונים הפנימיים
        # של הלקוח והמוצרים ימופו אוטומטית למודלים המתאימים.
        return OrderSnapshotResponse(
            order_id=order_data["order_id"],
            customer=customer_dict,
            items=order_data["items"],
            total_amount=order_data["total_amount"],
            status=order_data["status"],
            created_at=created_at
        )

    def get_order_history(self, customer_id: int) -> list[OrderSnapshotResponse]:
        """Fetch all order snapshots for a customer, sorted by created_at descending.

        Returns an empty list if the customer has no orders.
        """
        #raise NotImplementedError("Phase 1: implement get_order_history")

        # 1. התחברות לקולקשן 
        orders_col = self._mongo_db["order_snapshots"]
        
        # 2. בניית השאילתה עם מיון (Sort) בסדר יורד (-1)
        # אנחנו מחפשים לפי customer.id כי כך זה נשמר ב-Snapshot
        cursor = orders_col.find({"customer.id": customer_id}).sort("created_at", -1)
        
        # 3. המרת התוצאות לרשימה של אובייקטי Response
        results = []
        for order_data in cursor:
            # results.append(OrderSnapshotResponse(
            #     order_id=order_data["order_id"],
            #     customer=order_data["customer"],
            #     items=order_data["items"],
            #     total_amount=order_data["total_amount"],
            #     status=order_data["status"],
            #     created_at=order_data["created_at"]
            # ))
            results.append(self.get_order(order_data["order_id"]))
            
        return results

    def revenue_by_category(self) -> list[CategoryRevenueResponse]:
        """Compute total revenue per product category, sorted by total_revenue descending.

        See CategoryRevenueResponse in models/responses.py for the return shape.
        """
        #raise NotImplementedError("Phase 1: implement revenue_by_category")
        # 1. הגדרת ה-Pipeline
        pipeline = [
            # או פשוט לסנן לפי סטטוס:
            {"$match": {"status": "completed"}},
            # א. "פתיחת" מערך הפריטים - כל פריט הופך למסמך נפרד
            {"$unwind": "$items"},
            
            # ב. קיבוץ לפי קטגוריה וסיכום ההכנסות (מחיר יחידה * כמות)
            {
                "$group": {
                    "_id": "$items.category", 
                    "total_revenue": {
                        "$sum": { "$multiply": ["$items.unit_price", "$items.quantity"] }
                    }
                }
            },
            
            # ג. מיון לפי ההכנסה הכי גבוהה (Descending)
            {"$sort": {"total_revenue": -1}},
            
            # ד. עיצוב הפלט שיתאים ל-CategoryRevenueResponse
            {
                "$project": {
                    "_id": 0,
                    "category": {"$ifNull": ["$_id", "Unknown"]},
                    "total_revenue": 1
                }
            }
        ]
        
        # 2. הרצת האגרגציה
        results = list(self._mongo_db["order_snapshots"].aggregate(pipeline))
        
        # 3. המרה למודלים של Pydantic
        return [CategoryRevenueResponse(**r) for r in results]

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
        #raise NotImplementedError("Phase 2: implement invalidate_product_cache")

        # אנחנו מוחקים את המפתח של ה-Details בלבד.
        # המלאי (inventory:ID) לא נמחק כי הוא מנוהל בנפרד ולא אמור להיות "פג תוקף".
        if self._redis:
            cache_key = f"product:{product_id}"
        
        # הפקודה delete (או del) ב-Redis מסירה את המפתח לחלוטין.
        # אם המפתח לא קיים, Redis פשוט מתעלם (No-op), וזה בדיוק מה שביקשו.
            self._redis.delete(cache_key)


    def record_product_view(self, customer_id: int, product_id: int) -> None:
        """Record that a customer viewed a product.

        Maintains a bounded, ordered list of the customer's most recently
        viewed products (most recent first, capped at 10 entries).
        """
        #raise NotImplementedError("Phase 2: implement record_product_view")

        """Record that a customer viewed a product (capped at 10)."""
        key = f"customer:{customer_id}:recent_views"
        
        # 1. הסרת המזהה אם הוא כבר קיים ברשימה (כדי שלא יהיו כפילויות)
        # הפקודה lrem עם count=0 מסירה את כל המופעים של הערך
        self._redis.lrem(key, 0, product_id) 
        
        # 2. דחיפת המזהה החדש לראש הרשימה (צד שמאל)
        self._redis.lpush(key, product_id)
        
        # 3. חיתוך הרשימה ל-10 הפריטים הראשונים בלבד (הגדרת גבול)
        # לוקחים את אינדקס 0 עד 9
        self._redis.ltrim(key, 0, 9)

    def get_recently_viewed(self, customer_id: int) -> list[int]:
        """Return up to 10 recently viewed product IDs for a customer.

        Returns IDs as integers, most recently viewed first.
        Returns an empty list if no views have been recorded.
        """
        #raise NotImplementedError("Phase 2: implement get_recently_viewed")

        """Return up to 10 recently viewed product IDs for a customer."""
        key = f"customer:{customer_id}:recent_views"
        
        # lrange מחזירה טווח מהרשימה. 0 עד -1 מחזיר את כל מה שיש (מוגבל ב-10 בגלל ה-trim)
        viewed_ids = self._redis.lrange(key, 0, -1)
        
        # Redis מחזיר Byte Strings, אנחנו צריכים להפוך אותם ל-Integers
        return [int(pid) for pid in viewed_ids]

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
        #raise NotImplementedError("Phase 3: implement get_recommendations")
        query = """
        MATCH (p:Product {id: $product_id})-[r:BOUGHT_TOGETHER]-(rec:Product)
        WHERE rec.id <> $product_id
        RETURN rec.id AS recommended_id, rec.name AS name, r.weight AS score
        ORDER BY score DESC
        LIMIT $limit
        """
        
        recommendations = []
        with self._neo4j.session() as session:
            result = session.run(query, product_id=product_id, limit=limit)
            for record in result:
                # הכל מגיע מהגרף במכה אחת!
                recommendations.append(RecommendationResponse(
                    product_id=record["recommended_id"],
                    name=record["name"] , 
                    score=int(record["score"])
                ))
        return recommendations
