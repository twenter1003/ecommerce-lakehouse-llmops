import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator

# 프로젝트 루트 경로
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(PROJECT_ROOT)

default_args = {
    "owner": "data_engineering",
    "depends_on_past": False,
    "start_date": datetime(2026, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}


def check_kafka_health():
    """Redpanda / Kafka 브로커 헬스체크"""
    from kafka import KafkaConsumer
    consumer = KafkaConsumer(
        bootstrap_servers=["localhost:9092"],
        request_timeout_ms=5000
    )
    topics = consumer.topics()
    consumer.close()
    if "ecommerce.events" not in topics:
        raise ValueError("Topic 'ecommerce.events' not found on Kafka broker")
    print(f"Kafka Health OK. Available topics: {topics}")


def run_iceberg_compaction():
    """Apache Iceberg 스몰 파일 컴팩션 유지보수 프로시저 실행"""
    from pipelines.streaming.iceberg_sink import create_spark_session, compact_iceberg_table
    spark = create_spark_session()
    result = compact_iceberg_table(spark)
    spark.stop()
    print(f"Iceberg Compaction Result: {result}")


def sync_catalog_embeddings():
    """Qdrant 증분 카탈로그 임베딩 동기화"""
    from pipelines.llmops.embed_catalog import index_catalog
    points = index_catalog(force_reindex=False)
    print(f"Catalog Vector Sync Complete. Total points in Qdrant: {points}")


def verify_serving_health():
    """FastAPI LLMOps 서빙 엔드포인트 헬스체크"""
    from fastapi.testclient import TestClient
    from services.api.app import app
    with TestClient(app) as client:
        resp = client.get("/health")
        if resp.status_code != 200 or resp.json().get("status") != "healthy":
            raise RuntimeError(f"FastAPI Healthcheck failed: {resp.text}")
        print(f"FastAPI Serving Health OK: {resp.json()}")


with DAG(
    dag_id="ecommerce_lakehouse_orchestration",
    default_args=default_args,
    description="이커머스 실시간 레이크하우스 & LLMOps 배치 오케스트레이션 파이프라인",
    schedule="0 2 * * *",  # 매일 새벽 2시 정기 배치 실행
    catchup=False,
    tags=["lakehouse", "iceberg", "dbt", "llmops", "ecommerce"],
) as dag:

    # Task 1: 브로커 연결 검증
    t1_check_kafka = PythonOperator(
        task_id="check_kafka_health",
        python_callable=check_kafka_health,
    )

    # Task 2: Iceberg 스몰 파일 컴팩션
    t2_compact_iceberg = PythonOperator(
        task_id="run_iceberg_maintenance",
        python_callable=run_iceberg_compaction,
    )

    # Task 3: dbt Gold 데이터 마트 모델 빌드
    t3_dbt_run = BashOperator(
        task_id="run_dbt_gold_marts",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f".venv/bin/dbt run "
            f"--profiles-dir pipelines/dbt/ecommerce_analytics "
            f"--project-dir pipelines/dbt/ecommerce_analytics"
        ),
    )

    # Task 4: dbt 데이터 품질 테스트 SLA 검증
    t4_dbt_test = BashOperator(
        task_id="run_dbt_data_tests",
        bash_command=(
            f"cd {PROJECT_ROOT} && "
            f".venv/bin/dbt test "
            f"--profiles-dir pipelines/dbt/ecommerce_analytics "
            f"--project-dir pipelines/dbt/ecommerce_analytics"
        ),
    )

    # Task 5: 신규/수정 상품 Vector DB 증분 동기화
    t5_sync_embeddings = PythonOperator(
        task_id="sync_catalog_embeddings",
        python_callable=sync_catalog_embeddings,
    )

    # Task 6: LLM 서빙 API 상태 검증
    t6_verify_api = PythonOperator(
        task_id="verify_serving_health",
        python_callable=verify_serving_health,
    )

    # 태스크 의존성 파이프라인 (Linear DAG)
    t1_check_kafka >> t2_compact_iceberg >> t3_dbt_run >> t4_dbt_test >> t5_sync_embeddings >> t6_verify_api
