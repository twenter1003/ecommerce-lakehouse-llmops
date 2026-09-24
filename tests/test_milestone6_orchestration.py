import os
import sys
import unittest

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(PROJECT_ROOT)


class TestMilestone6Orchestration(unittest.TestCase):
    def test_01_dag_integrity_and_structure(self):
        """Airflow DAG 문법 유효성, 태스크 수 및 의존성 순환 오류 없음 검증"""
        from dags.ecommerce_lakehouse_dag import dag

        self.assertIsNotNone(dag)
        self.assertEqual(dag.dag_id, "ecommerce_lakehouse_orchestration")
        self.assertEqual(len(dag.tasks), 6)

        expected_task_ids = {
            "check_kafka_health",
            "run_iceberg_maintenance",
            "run_dbt_gold_marts",
            "run_dbt_data_tests",
            "sync_catalog_embeddings",
            "verify_serving_health",
        }
        actual_task_ids = {t.task_id for t in dag.tasks}
        self.assertEqual(expected_task_ids, actual_task_ids)

        # 의존성 체인 검증 (선형 파이프라인)
        t1 = dag.get_task("check_kafka_health")
        t2 = dag.get_task("run_iceberg_maintenance")
        t3 = dag.get_task("run_dbt_gold_marts")
        t4 = dag.get_task("run_dbt_data_tests")
        t5 = dag.get_task("sync_catalog_embeddings")
        t6 = dag.get_task("verify_serving_health")

        self.assertIn(t2, t1.downstream_list)
        self.assertIn(t3, t2.downstream_list)
        self.assertIn(t4, t3.downstream_list)
        self.assertIn(t5, t4.downstream_list)
        self.assertIn(t6, t5.downstream_list)

    def test_02_kafka_health_task_callable(self):
        """Kafka 헬스체크 태스크 실행 성공 검증"""
        from dags.ecommerce_lakehouse_dag import check_kafka_health
        try:
            check_kafka_health()
        except Exception as e:
            self.fail(f"check_kafka_health raised exception: {e}")

    def test_03_verify_serving_health_task_callable(self):
        """FastAPI 헬스체크 태스크 실행 성공 검증"""
        from dags.ecommerce_lakehouse_dag import verify_serving_health
        try:
            verify_serving_health()
        except Exception as e:
            self.fail(f"verify_serving_health raised exception: {e}")


if __name__ == "__main__":
    unittest.main()
