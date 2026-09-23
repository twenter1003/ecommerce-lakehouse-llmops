import argparse
import logging
import signal
import sys
import time
import uuid
from typing import Optional

from kafka import KafkaProducer
from kafka.errors import KafkaError

from generator.generator import TrafficGenerator
from generator.models import EcommerceEvent

# 로깅 설정 (타임스탬프와 함께 출력)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
logger = logging.getLogger("EcommerceProducer")


class EcommerceProducer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        topic: str = "ecommerce.events"
    ):
        self.topic = topic
        self.running = True
        
        logger.info(f"Connecting to Kafka/Redpanda at {bootstrap_servers}...")
        
        # KafkaProducer 초기화
        # 💡 [면접 포인트] 프로듀서 핵심 파라미터 설계 이유:
        # 1. key_serializer: user_id(문자열)를 바이트로 직렬화 -> 동일 user_id는 항상 같은 파티션에 해싱 배치
        # 2. value_serializer: Pydantic 모델을 JSON 문자열로 직렬화 후 UTF-8 바이트 변환
        # 3. acks='all': 브로커(ISR)가 메시지를 완전히 복제/기록했는지 확인 (데이터 무손실 보장)
        # 4. retries=3: 일시적인 네트워크 장애 발생 시 자동 재시도
        self.producer = KafkaProducer(
            bootstrap_servers=bootstrap_servers,
            key_serializer=lambda k: k.encode("utf-8"),
            value_serializer=lambda v: v.model_dump_json().encode("utf-8"),
            acks="all",
            retries=3,
            linger_ms=10  # 10ms 동안 메시지를 모아 배치 전송하여 처리량 개선
        )
        
        # 트래픽 생성기 (Milestone 1에서 작성한 퍼널 + 중복/지연 주입 로직)
        self.generator = TrafficGenerator()

    def _on_send_success(self, record_metadata):
        """비동기 전송 성공 시 호출되는 콜백 함수"""
        # record_metadata에는 전송된 topic, partition, offset 정보가 들어있음
        pass

    def _on_send_error(self, exc):
        """비동기 전송 실패 시 호출되는 콜백 함수"""
        logger.error(f"❌ Failed to deliver message: {exc}")

    def send_event(self, event: EcommerceEvent) -> None:
        """
        단일 이벤트를 Kafka 토픽으로 비동기 발행
        💡 Key를 event.user_id로 지정하여 파티션 내 유저별 순서(Ordering)를 보장합니다.
        💡 Header에 W3C Trace Context(traceparent)와 스키마 버전을 주입하여 본문 오염을 방지합니다.
        """
        trace_id = uuid.uuid4().hex
        span_id = uuid.uuid4().hex[:16]
        traceparent = f"00-{trace_id}-{span_id}-01".encode("utf-8")
        
        headers = [
            ("traceparent", traceparent),
            ("schema_version", b"1.0.0")
        ]

        future = self.producer.send(
            topic=self.topic,
            key=event.user_id,
            value=event,
            headers=headers
        )
        future.add_callback(self._on_send_success).add_errback(self._on_send_error)

    def start(self, count: int = 100, delay_sec: float = 0.2):
        """
        지정된 개수 또는 무한 스트리밍으로 이벤트를 발행
        :param count: 발행할 총 이벤트 수 (0이면 무한 스트리밍)
        :param delay_sec: 이벤트 간 지연 시간(초) -> 전송 속도 조절
        """
        logger.info(f"🚀 실시간 이벤트 전송 시작 (Target Topic: '{self.topic}')")
        logger.info(f"⚙️ 설정: count={count if count > 0 else '무한'}, delay={delay_sec}초 (초당 약 {1/delay_sec:.1f}건)")

        sent_count = 0
        target_count = count if count > 0 else float("inf")

        try:
            for event in self.generator.stream_events(count=count if count > 0 else 99999999, delay_sec=0):
                if not self.running or sent_count >= target_count:
                    break

                self.send_event(event)
                sent_count += 1

                # 실시간 진행 상황 콘솔 출력 (보기 쉽게 포맷팅)
                action_badge = f"[{event.event_type:17}]"
                print(
                    f"\r⚡ Sent: {sent_count:4d} | {action_badge} | "
                    f"User: {event.user_id} | Session: {event.session_id[:8]}.. | "
                    f"EventID: {event.event_id[:8]}..",
                    end="",
                    flush=True
                )

                time.sleep(delay_sec)

        except KeyboardInterrupt:
            logger.info("\n🛑 사용자에 의해 중단 신호(Ctrl+C)가 감지되었습니다.")
        finally:
            self.stop()

    def stop(self):
        """남아있는 버퍼 메시지를 비우고 안전하게 연결 종료 (Graceful Shutdown)"""
        logger.info("\nFlushing remaining messages & closing Kafka Producer...")
        self.running = False
        self.producer.flush(timeout=5)  # 5초 내 대기 중인 모든 메시지 강제 전송
        self.producer.close()
        logger.info("✅ Kafka Producer safely closed.")


def main():
    parser = argparse.ArgumentParser(description="실시간 이커머스 트래픽 Kafka Producer")
    parser.add_argument("--count", type=int, default=50, help="전송할 이벤트 수 (0 입력 시 무한 전송, 기본: 50)")
    parser.add_argument("--delay", type=float, default=0.2, help="이벤트 간 지연 시간(초, 기본: 0.2초 -> 초당 5건)")
    parser.add_argument("--topic", type=str, default="ecommerce.events", help="발행 대상 토픽 (기본: ecommerce.events)")
    parser.add_argument("--bootstrap", type=str, default="localhost:9092", help="Kafka/Redpanda 주소")

    args = parser.parse_args()

    producer = EcommerceProducer(
        bootstrap_servers=args.bootstrap,
        topic=args.topic
    )

    # Ctrl+C 시그널 핸들러 등록
    def handle_sigint(sig, frame):
        producer.stop()
        sys.exit(0)

    signal.signal(signal.SIGINT, handle_sigint)

    producer.start(count=args.count, delay_sec=args.delay)


if __name__ == "__main__":
    main()
