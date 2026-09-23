---
name: kafka-infra-ops
description: Quick-reference cheatsheet and operations for Redpanda/Kafka docker containers, topic partition management, message inspection, and consumer verification using rpk.
---

# Kafka & Redpanda Infrastructure Operations

## 1. 컨테이너 상태 제어
```bash
# Redpanda + Console 백그라운드 구동
docker compose -f docker/docker-compose.kafka.yml up -d

# 중단
docker compose -f docker/docker-compose.kafka.yml down
```

## 2. 토픽 및 파티션 제어 (rpk)
```bash
# 토픽 목록 조회
docker exec redpanda rpk topic list

# 토픽 생성 (파티션 3개, 복제본 1개)
docker exec redpanda rpk topic create ecommerce.events --partitions 3 --replicas 1

# 토픽 파티션 및 설정 상세 조회
docker exec redpanda rpk topic describe ecommerce.events
```

## 3. 메시지 적재 및 파티션 분산 검증
```bash
# 특정 파티션의 최신 N개 메시지 조회 (Key, Value, Partition, Offset)
docker exec redpanda rpk topic consume ecommerce.events --partitions 0 -n 3
docker exec redpanda rpk topic consume ecommerce.events --partitions 1 -n 3
docker exec redpanda rpk topic consume ecommerce.events --partitions 2 -n 3
```

## 4. 웹 UI
- **Redpanda Console**: `http://localhost:8080` (Topics, Brokers, Schema 확인)
