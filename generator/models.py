from datetime import datetime
from typing import List, Optional, Literal
from pydantic import BaseModel, Field
import uuid

# 지원하는 이벤트 종류
EventType = Literal[
    "item_view", 
    "add_to_cart", 
    "order_created", 
    "payment_completed", 
    "order_cancelled"
]

class OrderItem(BaseModel):
    product_id: str
    product_name: str
    category: str
    price: int
    quantity: int = 1

class EcommerceEvent(BaseModel):
    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_type: EventType
    event_timestamp: datetime
    user_id: str
    session_id: str
    
    # [데이터 감사 & 계보 추적 필드]
    # 💡 sequence_number: 세션(session_id) 단위로 1부터 단조 증가 (추후 Iceberg 레이크하우스 배치 감사용)
    # 💡 correlation_id: 하나의 쇼핑/주문 트랜잭션 수명주기 전체를 묶는 고유 식별자
    sequence_number: int = 1
    correlation_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    
    # 아이템 조회/장바구니 담기 시 사용
    product_id: Optional[str] = None
    
    # 주문/결제/취소 관련 필드
    order_id: Optional[str] = None
    items: Optional[List[OrderItem]] = None
    total_amount: Optional[int] = None
    payment_method: Optional[Literal["credit_card", "easy_pay", "bank_transfer"]] = None
    cancellation_reason: Optional[str] = None

    class Config:
        json_encoders = {
            datetime: lambda v: v.isoformat()
        }
