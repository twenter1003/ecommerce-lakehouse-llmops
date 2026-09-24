import os
import unittest
import duckdb
import subprocess


class TestMilestone4Lakehouse(unittest.TestCase):
    def setUp(self):
        self.project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        self.warehouse_dir = os.path.join(self.project_root, "warehouse")
        self.iceberg_path = os.path.join(self.warehouse_dir, "ecommerce", "silver_events")
        self.duckdb_path = os.path.join(self.warehouse_dir, "gold.duckdb")

    def test_iceberg_table_exists_and_queryable(self):
        """Apache Iceberg Silver 테이블이 정상 생성되고 DuckDB iceberg 확장으로 스캔 가능한지 검증"""
        self.assertTrue(os.path.exists(self.iceberg_path), f"Iceberg table path not found: {self.iceberg_path}")

        con = duckdb.connect()
        con.execute("LOAD iceberg;")
        res = con.execute(f"SELECT count(*) FROM iceberg_scan('{self.iceberg_path}')").fetchone()
        con.close()

        self.assertIsNotNone(res)
        self.assertGreater(res[0], 0, "Iceberg table should contain at least 1 record")

    def test_dbt_models_and_tests_pass(self):
        """dbt run 및 dbt test가 오류 없이 통과하는지 검증"""
        dbt_cmd = [
            os.path.join(self.project_root, ".venv/bin/dbt"),
            "test",
            "--profiles-dir", "pipelines/dbt/ecommerce_analytics",
            "--project-dir", "pipelines/dbt/ecommerce_analytics"
        ]
        result = subprocess.run(dbt_cmd, cwd=self.project_root, capture_output=True, text=True)
        self.assertEqual(
            result.returncode, 0,
            f"dbt test failed:\nSTDOUT:\n{result.stdout}\nSTDERR:\n{result.stderr}"
        )
        self.assertIn("PASS=12", result.stdout)


if __name__ == "__main__":
    unittest.main()
