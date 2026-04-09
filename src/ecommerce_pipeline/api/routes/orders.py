from fastapi import APIRouter, Depends, HTTPException

from ecommerce_pipeline.db import get_db_access
from ecommerce_pipeline.db_access import DBAccess
from ecommerce_pipeline.models.requests import CreateOrderRequest
from ecommerce_pipeline.models.responses import OrderResponse, OrderSnapshotResponse, OrderHistoryResponse

router = APIRouter()


@router.post("", response_model=OrderResponse, status_code=201)
def create_order(
    body: CreateOrderRequest,
    db: DBAccess = Depends(get_db_access),
) -> OrderResponse:
    """Place an order."""
    try:
        order = db.create_order(
            customer_id=body.customer_id,
            items=body.items,
        )
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail={"message": str(exc)})
    except ValueError as exc:
        raise HTTPException(status_code=400, detail={"message": str(exc)})
    return order


@router.get("/{order_id}", response_model=OrderSnapshotResponse)
def get_order(
    order_id: int,
    db: DBAccess = Depends(get_db_access),
) -> OrderSnapshotResponse:
    """Fetch an order by ID."""
    try:
        order = db.get_order(order_id)
    except NotImplementedError as exc:
        raise HTTPException(status_code=501, detail={"message": str(exc)})
    if order is None:
        raise HTTPException(status_code=404, detail={"message": "order not found"})
    return order

@router.get("/customer/{customer_id}/history", response_model=OrderHistoryResponse)
def get_customer_orders(customer_id: int, db: DBAccess = Depends(get_db_access)):
    """Fetch all orders by ID.
    """
    # 1. קריאה לפונקציה שכתבת ב-db_access
    history_list = db.get_order_history(customer_id)
    
    # 2. החזרה בפורמט שהמרצה ביקש (אובייקט עם שדה orders)
    return OrderHistoryResponse(orders=history_list)