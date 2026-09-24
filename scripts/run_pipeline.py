import os
import sys
import time
import subprocess
import logging

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(PROJECT_ROOT)

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("PipelineRunner")


def run_stage(stage_name: str, func) -> float:
    logger.info(f"▶️ Starting Stage: [{stage_name}]")
    t0 = time.time()
    func()
    duration = time.time() - t0
    logger.info(f"✅ Completed Stage: [{stage_name}] in {duration:.2f}s")
    return duration


def stage_kafka_produce():
    cmd = [
        os.path.join(PROJECT_ROOT, ".venv/bin/python"),
        "-m", "generator.producer",
        "--count", "20",
        "--delay", "0.01"
    ]
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)


def stage_iceberg_sink():
    cmd = [
        os.path.join(PROJECT_ROOT, ".venv/bin/python"),
        "pipelines/streaming/iceberg_sink.py",
        "--once"
    ]
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)


def stage_iceberg_compact():
    cmd = [
        os.path.join(PROJECT_ROOT, ".venv/bin/python"),
        "pipelines/streaming/iceberg_sink.py",
        "--compact"
    ]
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)


def stage_dbt_run():
    cmd = [
        os.path.join(PROJECT_ROOT, ".venv/bin/dbt"),
        "run",
        "--profiles-dir", "pipelines/dbt/ecommerce_analytics",
        "--project-dir", "pipelines/dbt/ecommerce_analytics"
    ]
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)


def stage_dbt_test():
    cmd = [
        os.path.join(PROJECT_ROOT, ".venv/bin/dbt"),
        "test",
        "--profiles-dir", "pipelines/dbt/ecommerce_analytics",
        "--project-dir", "pipelines/dbt/ecommerce_analytics"
    ]
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)


def stage_qdrant_sync():
    cmd = [
        os.path.join(PROJECT_ROOT, ".venv/bin/python"),
        "pipelines/llmops/embed_catalog.py"
    ]
    subprocess.run(cmd, cwd=PROJECT_ROOT, check=True)


def stage_api_verify():
    from fastapi.testclient import TestClient
    from services.api.app import app
    with TestClient(app) as client:
        # 1. Health
        h = client.get("/health").json()
        assert h["status"] == "healthy", f"Health status: {h}"
        # 2. Search
        s = client.post("/api/v1/search", json={"query": "블레이저", "limit": 2}).json()
        assert s["total_hits"] > 0, f"Search empty: {s}"
        # 3. Recommend
        r = client.post("/api/v1/recommend", json={"user_id": "user_0167", "limit": 2}).json()
        assert "recommendations" in r, f"Rec failed: {r}"
        # 4. Ask
        a = client.post("/api/v1/ask", json={"question": "출근용 옷", "limit": 1}).json()
        assert len(a["answer"]) > 0, f"Ask failed: {a}"


def main():
    logger.info("=================================================================")
    logger.info("🚀 E-Commerce Lakehouse & LLMOps End-to-End Pipeline Execution")
    logger.info("=================================================================")

    stages = [
        ("1. Kafka Event Ingestion", stage_kafka_produce),
        ("2. Spark -> Iceberg Streaming Sink", stage_iceberg_sink),
        ("3. Iceberg Small File Compaction", stage_iceberg_compact),
        ("4. dbt Gold Marts Transformation", stage_dbt_run),
        ("5. dbt Data Quality SLA Tests", stage_dbt_test),
        ("6. Qdrant Vector DB Incremental Sync", stage_qdrant_sync),
        ("7. LLMOps Serving API Verification", stage_api_verify),
    ]

    report = []
    total_start = time.time()
    for name, func in stages:
        d = run_stage(name, func)
        report.append((name, d))

    total_time = time.time() - total_start
    logger.info("=================================================================")
    logger.info("📊 End-to-End Pipeline Execution Report")
    logger.info("=================================================================")
    for name, d in report:
        logger.info(f"  - {name:<40} : {d:6.2f}s [SUCCESS]")
    logger.info(f"🎉 Total Execution Time: {total_time:.2f}s")
    logger.info("=================================================================")


if __name__ == "__main__":
    main()
