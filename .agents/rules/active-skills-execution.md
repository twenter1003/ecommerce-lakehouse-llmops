# Active Skills Execution Protocol (이커머스 파이프라인 전용 스킬 실행 규정)

이 규정은 프로젝트 전반에 걸쳐 에이전트가 자체 지식으로 대충 때우지 않고, **이미 설치된 고성능 스킬들을 작업 상황별로 반드시 호출(Invoke)하여 강제 적용**하도록 규정합니다.

---

## 1. 단계별 필수 스킬 체인 (Mandatory Skill Chains)

작업 상황에 따라 에이전트는 다음 스킬들을 의무적으로 활성화하여 그 지침에 따라야 합니다:

### 🎯 상황 A: 새로운 마일스톤 기획 및 아키텍처 설계
1. **`de-debate`** (`.agents/skills/de-debate/SKILL.md`):
   - Architect vs Critic 3단계 토론 가동 (기능 제안 -> 분산 환경 맹점 4대 공격 -> 실무 타협안)
2. **`ponytail`** (`~/.gemini/config/plugins/ponytail/skills/ponytail/SKILL.md`):
   - YAGNI(필요 없는 기능 사전 제거), 과도한 추상화 배제, 표준 라이브러리 및 네이티브 우선

### 💻 상황 B: 코드 작성 및 리팩토링
1. **`karpathy-guidelines`** (`~/.gemini/config/plugins/andrej-karpathy-skills/skills/karpathy-guidelines/SKILL.md`):
   - **Simplicity First**: 문제 해결에 필요한 최소한의 코드만 작성
   - **Surgical Changes**: 변경이 요청된 라인만 외과수술식으로 정밀 수정 (주변 코드 임의 변경 금지)
   - **Goal-Driven Execution**: 검증 가능한 성공 기준을 명시하고 실행 루프 진행
2. **`managing-python-dependencies`**:
   - 가상환경(`.venv`) 격리 유지 및 일관된 패키지 관리

### 🌊 상황 C: Kafka / Spark / Iceberg 파이프라인 구현
1. **`kafka-infra-ops`** (`.agents/skills/kafka-infra-ops/SKILL.md`):
   - Redpanda/Kafka 컨테이너 제어, 토픽 파티셔닝, `rpk` 오프셋 검증 표준 명령어 준수
2. **`gcp-spark`**:
   - Spark Structured Streaming 워터마크, 윈도우, 메모리(StateStore) 최적화 가이드 적용
3. **`schema-mapping`**:
   - JSON 스트림 -> Iceberg 테이블 정형 스키마 변환 시 Mapping Manifesto 수립
4. **`accidental-data-loss-prevention`**:
   - 데이터 삭제/초기화성 명령 실행 전 사용자 확인 강제

### 🐛 상황 D: 버그 발생 및 트러블슈팅
1. **`superpowers:systematic-debugging`**:
   - 증상만 보고 짐작하여 코드를 고치는 행위 금지
   - 가설 수립 -> 최소 재현 -> 근본 원인(Root-cause) 규명 후 수정

### ✅ 상황 E: 작업 완료 및 보고
1. **`superpowers:verification-before-completion`**:
   - 실제 터미널 출력 및 테스트 검증 증거 없이 작업 완료를 주장하지 말 것
2. **`velog-til-generator`** (`.agents/skills/velog-til-generator/SKILL.md`):
   - "오늘 여기까지" 입력 시 아키텍처, 기술선택(Why), 트러블슈팅, 면접 Q&A 4단 구성 블로그 초안 생성
