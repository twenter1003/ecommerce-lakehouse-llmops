# Milestone 1: 이커머스 스키마 정의 및 트래픽 시뮬레이터 구축

> **소요 마일스톤**: Milestone 1  
> **상태**: 완료 (Verified)  
> **핵심 키워드**: Pydantic v2, Funnel Simulation, Duplicate Injection, Late Data Injection, At-Least-Once

---

## 1. 개요 및 배경

데이터 파이프라인의 완성도를 검증하기 위해서는 현실적인 비즈니스 인과관계와 함께, 분산 네트워크에서 흔히 발생하는 **이상 결함(중복 및 지연 데이터)** 이 의도적으로 포함된 스트림이 필요합니다.

Milestone 1에서는 단순 더미 데이터 생성(Faker 등)의 한계를 극복하고, 전환 퍼널(Conversion Funnel)과 분산 환경의 결함을 정밀하게 재현하는 커스텀 트래픽 생성기를 구축했습니다.

---

## 2. 핵심 설계 및 구성 요소

### ① 엄격한 이벤트 스키마 (`generator/models.py`)
* Pydantic v2 기반으로 `item_view`, `add_to_cart`, `order_created`, `payment_completed`, `order_cancelled` 5가지 핵심 이벤트 타입 모델링.
* 주문/결제 상세(`OrderItem`, `total_amount`, `payment_method`) 및 감사 필드(`sequence_number`, `correlation_id`) 포함.

### ② 확률적 퍼널 시뮬레이터 (`generator/generator.py`)
* **1단계 (상품 조회, `item_view`)**: 100% 진입, 1~3개 상품 조회
* **2단계 (장바구니 담기, `add_to_cart`)**: 30% 전환율 (70% 이탈)
* **3단계 (주문서 생성, `order_created`)**: 장바구니 유저 중 50% 진행 (전체의 15%)
* **4단계 (결제/취소, `payment_completed` / `order_cancelled`)**: 90% 결제 성공 vs 10% 주문 취소

---

## 3. 분산 시스템 결함 주입 (Intentional Chaos Injection)

### 1) 중복 데이터 주입 (`duplicate_rate=0.04`, 4%)
* **배경**: 모바일 클라이언트 타임아웃 재전송으로 인한 Kafka의 **At-Least-Once(최소 한 번 전송)** 중복 모사.
* **구현**: 최근 발행된 이벤트를 버퍼에 유지하다가 동일한 `event_id`로 재발행.
* **검증 대상**: 다운스트림 **Spark Structured Streaming의 StateStore 기반 중복 제거(`dropDuplicates`)**.

### 2) 지연 도착 데이터 주입 (`late_data_rate=0.03`, 3%)
* **배경**: 터널/엘리베이터 통과 등 네트워크 일시 단절 후 뒤늦게 도착하는 **비순서(Out-of-order) / 지연(Late) 데이터** 모사.
* **구현**: 현재 시간이 아닌 3분~10분 전 과거 타임스탬프 부여 (`now - timedelta(minutes=randint(3, 10))`).
* **검증 대상**: 다운스트림 **Spark 워터마크(`withWatermark`) 및 윈도우 버퍼링**.

---

## 4. 검증 명령어 및 실행 증거

```bash
# 가상환경 활성화 후 트래픽 생성기 단독 테스트
source .venv/bin/activate
python -m generator.generator
```

### 실행 출력 (중복 주입 확인)
```text
[1] item_view        | Time: 2026-09-23T16:21:33... | ID: e7a2fca6... | User: user_0078
...
[6] item_view        | Time: 2026-09-23T16:21:33... | ID: efedb129... | User: user_0098
[7] add_to_cart      | Time: 2026-09-23T16:21:33... | ID: eeb8caaf... | User: user_0098
[8] item_view        | Time: 2026-09-23T16:21:33... | ID: efedb129... | User: user_0098  <-- 중복 이벤트 재발행 확인!
[9] order_created    | Time: 2026-09-23T16:21:33... | ID: d830d6c9... | User: user_0098
[10] payment_completed
```
