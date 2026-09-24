# Agent Instructions (AGENTS.md)

이 프로젝트는 2026년 대한민국 신입 데이터 엔지니어 취업을 위한 **실시간 이커머스 이벤트 레이크하우스 & LLMOps 파이프라인**입니다.
Claude Code 및 Antigravity 에이전트는 사용자와 협업할 때 다음 원칙을 엄격히 준수해야 합니다.
---

## 0. 스킬 선행 실행 원칙 (Mandatory Skills Check)
- 에이전트는 자체 지식으로 임의 추측하여 행동하지 않고, 상황에 맞는 스킬 체인을 항상 먼저 로드하여 원칙을 강제한다 (`.agents/rules/active-skills-execution.md` 준수).
  - 설계/기획: `de-debate`, `ponytail` (YAGNI, 최소 코드)
  - 구현/수정: `karpathy-guidelines` (외과수술식 최소 변경, Simplicity First)
  - 파이프라인: `kafka-infra-ops`, `gcp-spark`, `schema-mapping`
  - 디버깅/완료: `superpowers:systematic-debugging`, `superpowers:verification-before-completion`

---

## 0. 스킬 선행 실행 원칙 (Mandatory Skills Check)
- 에이전트는 자체 지식으로 임의 추측하여 행동하지 않고, 상황에 맞는 스킬 체인을 항상 먼저 로드하여 원칙을 강제한다 (`.agents/rules/active-skills-execution.md` 준수).
  - 설계/기획: `de-debate`, `ponytail` (YAGNI, 최소 코드)
  - 구현/수정: `karpathy-guidelines` (외과수술식 최소 변경, Simplicity First)
  - 파이프라인: `kafka-infra-ops`, `gcp-spark`, `schema-mapping`
  - 디버깅/완료: `superpowers:systematic-debugging`, `superpowers:verification-before-completion`

---

## 1. 페어 프로그래밍 원칙 (Anti-Vibe-Dump)
- **일방적인 대량 코드 생성 금지**: 한 번에 코드를 모두 짜주고 방치하지 않습니다.
- **단계별 이해 & 피드백 루프**: 각 마일스톤과 코드의 '이유(Why)'를 명확히 설명하고 주석을 작성, 사용자가 코드를 직접 실행 및 확인하며 이해하는 것을 최우선으로 합니다.
- **모호한 점은 질문**: 아키텍처나 비즈니스 로직에 모호함이 생기면 임의로 넘겨짚지 않고 사용자에게 질문합니다.
- **실무 관점에서 문제 해결**: 포트폴리오라는 특성을 배제하고 실무에서 사용하는 기법, 퍼즐식의 접근이 아닌 실무에서 일어날 수 있는 변수를 고려하는 접근으로 문제 해결합니다.
- **듀얼 에이전트 토론 프로토콜 (Architect vs Critic)**:
  - 모든 설계 및 구현 전, 다음 3단계 토론을 반드시 거쳐 실무 타협안을 도출한다.
    1. **[Architect]**: 비즈니스 요구사항을 반영한 기능/아키텍처/데이터 흐름 설계안 제안
    2. **[Critic]**: 분산 환경 한계(OOM, I/O, Clock Skew, Late Data), 오버엔지니어링, 면접 압박 질문 관점에서 냉혹한 비판
    3. **[Synthesis]**: 두 관점을 종합하여 실무 표준에 부합하는 현실적인 타협안(Trade-off) 결정 및 사용자 승인 후 구현

---

## 2. 블로그(Velog) / TIL 작성 트리거
- 사용자가 **"오늘 여기까지"** (또는 당일 작업을 마친다는 표현)를 입력하면:
  - 그날 진행한 내용에 대한 **Velog 포스팅용 마크다운 초안**을 생성합니다.
  - 포함할 내용:
    1. 오늘 구현한 아키텍처 및 배경
    2. 기술 선택 이유 (Trade-off: 왜 Kafka인가, 왜 Redpanda인가 등)
    3. 발생한 에러와 해결 과정 (Troubleshooting)
    4. 기술 면접 대비 핵심 질문과 답변 (CS/DE 개념)

---

## 3. 프로젝트 기술 스택 & 환경
- **개발 환경**: Apple Silicon Mac M5 (CPU 10 / GPU 10), RAM 24GB, Docker Desktop, Python 3.14.3 (`.venv`)
- **도메인**: 이커머스 (클릭스트림, 주문/결제/취소 트랜잭션, 검색)
- **파이프라인 아키텍처**: 사용자에게 물어보면서 결정되면 여기에 하나씩 추가.

---

## 4. 진행 현황 (Roadmap)
- [x] **Milestone 1**: 가상환경 셋업, 이커머스 스키마 정의 및 이벤트 생성기(Funnel, 중복/지연 주입) 작성 완료
- [x] **Milestone 2**: Docker 기반 Kafka(Redpanda) 인프라 구축 & 실시간 Producer 연동 (토론 기반 재설계 및 검증 완료)
- [x] **Milestone 4**: Apache Iceberg 레이크하우스 적재 & dbt 골드 마트 모델링 (토론 기반 재설계, 스몰 파일 방어, dbt 테스트 검증 완료)
- [x] **Milestone 5 (LLMOps)**: 상품 임베딩 파이프라인 & Vector DB 적재 + LLM 서빙 API(FastAPI) (증분 임베딩 캐시, Iceberg 연동 실시간 개인화 추천 완료)
- [ ] **Milestone 6**: Airflow 오케스트레이션 & 모니터링 & 최종 정리

