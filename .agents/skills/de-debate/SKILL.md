---
name: de-debate
description: Executes the 3-step Architect vs Critic debate protocol to stress-test data engineering designs against distributed system failure modes (OOM, Clock Skew, Late Data) before code implementation.
---

# Dual-Agent Debate Protocol (de-debate)

데이터 엔지니어링 아키텍처 및 파이프라인 구현 전 오버엔지니어링을 제거하고 실무 타협안을 도출하는 3단계 토론 절차입니다.

## 실행 절차 (3 Steps)

### Step 1. [Architect] 기능 및 데이터 흐름 제안
- **목표**: 비즈니스 요구사항 및 컴포넌트 간 연결 흐름 정의
- **포함 내용**:
  1. 대상 컴포넌트 및 데이터 스키마
  2. 토픽/파티션/버퍼 전략
  3. 기대 효과

### Step 2. [Critic] 분산 환경 맹점 공격 (Stress-Test)
- **필수 공격 포인트 4개**:
  1. **State OOM & I/O**: Stateful 연산(RocksDB/메모리) 누적 및 체크포인팅 병목
  2. **Time & Ordering**: Clock Skew(NTP 오차), Late Data의 허위 유실(False Positive) 판정
  3. **YAGNI & 책임 분리**: "스트리밍에서 할 일인가? 레이크하우스 배치로 넘겨야 하는가?"
  4. **Data Skew**: 파티션 핫스팟 및 봇/헤비유저 쏠림

### Step 3. [Synthesis] 현실적 실무 타협안 (Trade-off Matrix)
- Critic의 지적 중 수용할 항목과 기각할 항목 분리
- 최소 코드(Ponytail & Karpathy)로 동작 가능한 단단한 최종 스펙 정의
- 사용자 승인 요청 후 코드 작성 착수
