import json
import random
import time
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Generator
import uuid

from generator.models import EcommerceEvent, OrderItem, EventType

class TrafficGenerator:
    def __init__(
        self,
        products_file: str = "data/products.json",
        duplicate_rate: float = 0.04,  # 4% 확률로 중복 이벤트 발행
        late_data_rate: float = 0.03   # 3% 확률로 3분 지연 이벤트 발생
    ):
        with open(products_file, "r", encoding="utf-8") as f:
            self.products: List[Dict] = json.load(f)
            
        self.duplicate_rate = duplicate_rate
        self.late_data_rate = late_data_rate
        self.user_pool = [f"user_{i:04d}" for i in range(1, 201)]  # 200명의 유저 풀

    def _get_timestamp(self) -> datetime:
        now = datetime.now(timezone.utc)
        # 3% 확률로 네트워크 지연으로 인한 지연(Late) 데이터 생성 (3분 전 타임스탬프)
        if random.random() < self.late_data_rate:
            return now - timedelta(minutes=random.randint(3, 10))
        return now

    def generate_user_journey(self) -> Generator[EcommerceEvent, None, None]:
        """
        한 명의 유저가 일으키는 퍼널(Funnel) 세션 시뮬레이션
        item_view -> (add_to_cart) -> (order_created) -> (payment_completed or cancelled)
        """
        user_id = random.choice(self.user_pool)
        session_id = str(uuid.uuid4())
        correlation_id = str(uuid.uuid4())
        sequence_number = 1
        
        # 1. 상품 조회 (item_view) - 1~3개 상품 조회
        viewed_products = random.sample(self.products, k=random.randint(1, 3))
        cart_candidates = []

        for prod in viewed_products:
            event = EcommerceEvent(
                event_type="item_view",
                event_timestamp=self._get_timestamp(),
                user_id=user_id,
                session_id=session_id,
                sequence_number=sequence_number,
                correlation_id=correlation_id,
                product_id=prod["product_id"]
            )
            sequence_number += 1
            yield event
            cart_candidates.append(prod)

        # 2. 장바구니 담기 (add_to_cart) - 30% 확률
        if random.random() > 0.3 or not cart_candidates:
            return

        selected_prod = random.choice(cart_candidates)
        yield EcommerceEvent(
            event_type="add_to_cart",
            event_timestamp=self._get_timestamp(),
            user_id=user_id,
            session_id=session_id,
            sequence_number=sequence_number,
            correlation_id=correlation_id,
            product_id=selected_prod["product_id"]
        )
        sequence_number += 1

        # 3. 주문 생성 (order_created) - 장바구니 유저 중 50%가 주문 진행 (전체의 15%)
        if random.random() > 0.5:
            return

        order_id = f"ord_{int(time.time())}_{random.randint(100, 999)}"
        order_item = OrderItem(
            product_id=selected_prod["product_id"],
            product_name=selected_prod["product_name"],
            category=selected_prod["category"],
            price=selected_prod["price"],
            quantity=random.randint(1, 2)
        )
        total_amount = order_item.price * order_item.quantity

        order_event = EcommerceEvent(
            event_type="order_created",
            event_timestamp=self._get_timestamp(),
            user_id=user_id,
            session_id=session_id,
            sequence_number=sequence_number,
            correlation_id=correlation_id,
            order_id=order_id,
            items=[order_item],
            total_amount=total_amount,
            payment_method=random.choice(["credit_card", "easy_pay", "bank_transfer"])
        )
        sequence_number += 1
        yield order_event

        # 4. 결제 완료 또는 취소 (payment_completed 90% vs order_cancelled 10%)
        if random.random() < 0.9:
            yield EcommerceEvent(
                event_type="payment_completed",
                event_timestamp=self._get_timestamp(),
                user_id=user_id,
                session_id=session_id,
                sequence_number=sequence_number,
                correlation_id=correlation_id,
                order_id=order_id,
                total_amount=total_amount,
                payment_method=order_event.payment_method
            )
        else:
            yield EcommerceEvent(
                event_type="order_cancelled",
                event_timestamp=self._get_timestamp(),
                user_id=user_id,
                session_id=session_id,
                sequence_number=sequence_number,
                correlation_id=correlation_id,
                order_id=order_id,
                total_amount=total_amount,
                cancellation_reason=random.choice(["단순변심", "결제수단변경", "재고부족"])
            )

    def stream_events(self, count: int = 50, delay_sec: float = 0.1) -> Generator[EcommerceEvent, None, None]:
        """
        연속적인 이벤트 스트림 생성 (중복 이벤트 주입 포함)
        """
        emitted = 0
        recent_events: List[EcommerceEvent] = []

        while emitted < count:
            for event in self.generate_user_journey():
                # 정상 이벤트 발행
                yield event
                emitted += 1
                recent_events.append(event)
                time.sleep(delay_sec)

                # 4% 확률로 최근 이벤트를 중복(Duplication) 재전송 (네트워크 재시도 모사)
                if random.random() < self.duplicate_rate and recent_events:
                    dup_event = random.choice(recent_events)
                    # 동일한 event_id로 다시 한 번 yield
                    yield dup_event
                    emitted += 1

                if emitted >= count:
                    break


if __name__ == "__main__":
    generator = TrafficGenerator()
    print("🚀 가상 이커머스 트래픽 생성 테스트 시작 (10개 이벤트 출력)...")
    for i, event in enumerate(generator.stream_events(count=10, delay_sec=0.05)):
        print(f"[{i+1}] {event.event_type:18} | Time: {event.event_timestamp.isoformat()} | ID: {event.event_id[:8]}... | User: {event.user_id}")
