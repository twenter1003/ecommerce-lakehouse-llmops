# 📚 파이프라인 마일스톤 문서 (Milestones Documentation)

이 디렉터리는 **실시간 이커머스 이벤트 레이크하우스 & LLMOps 파이프라인**의 각 마일스톤별 아키텍처 설계, 기술 선택 이유(Trade-offs), 트러블슈팅, 운영 런북 및 면접 대비 질문을 단계별로 체계적으로 정리한 공식 기술 문서입니다.

---

## 🗺️ 마일스톤 진행 현황 및 문서 링크

| 마일스톤 | 문서 링크 | 핵심 기술 | 상태 |
| :--- | :--- | :--- | :--- |
| **Milestone 1** | [Milestone 1: 스키마 & 트래픽 생성기](milestone_1.md) | Pydantic v2, Funnel Simulation, Chaos Injection (Dups/Late) | ✅ 완료 |
| **Milestone 2** | [Milestone 2: Redpanda & 실시간 Producer](milestone_2.md) | Redpanda, Kafka Headers, W3C Trace Context, de-debate | ✅ 완료 |
| **Milestone 3** | [Milestone 3: Spark Structured Streaming](milestone_3.md) | PySpark, Watermarking, StateStore Deduplication, Checkpointing | ✅ 완료 |
| **Milestone 4** | [Milestone 4: Apache Iceberg & dbt 골드 마트](milestone_4.md) | Apache Iceberg, Hidden Partitioning, dbt-duckdb, Compaction | ✅ 완료 |
| **Milestone 5** | [Milestone 5: LLMOps 상품 임베딩 & 서빙 API](milestone_5.md) | Gemini Embeddings, Qdrant, FastAPI, Semantic Search & RAG | ✅ 완료 |
| **Milestone 6** | Milestone 6: Airflow 오케스트레이션 & 모니터링 | Apache Airflow, DAG Authoring, End-to-End Test | ⏳ 대기 |
