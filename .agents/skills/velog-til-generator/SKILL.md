---
name: velog-til-generator
description: Generates structured, copy-paste ready Velog/TIL technical blog posts when user says '오늘 여기까지' (wrap-up), focusing on architectural background, trade-offs, troubleshooting, and DE interview Q&As.
---

# Velog TIL Generator Runbook

사용자가 "오늘 여기까지" 또는 세션 종료를 요청할 때 아래 4대 구성 요소를 포함하는 마크다운 문서를 생성합니다.

## 필수 포함 섹션

### 1. 오늘의 아키텍처 개요 및 구현 배경
- 구현한 파이프라인 단계와 컴포넌트 간 데이터 흐름 (다이어그램 포함)

### 2. 핵심 기술 선택 및 Trade-off (Why)
- 대체 기술 대비 이 기술을 선택한 이유 (예: Kafka vs Redpanda, StateStore 실시간 vs Iceberg 배치)
- Architect vs Critic 토론을 통해 걸러낸 오버엔지니어링 요소

### 3. 트러블슈팅 로그 (Troubleshooting)
- 발생했던 에러 메시지(원문), 원인 분석, 해결한 방법 및 검증 결과

### 4. 2026 데이터 엔지니어 기술 면접 대비 Q&A
- 오늘 작업한 내용에서 면접관이 물어볼 만한 핵심 CS/DE 질문 2~3개와 모범 답변 (단답형이 아닌 실무 경험에 기반한 답변)
