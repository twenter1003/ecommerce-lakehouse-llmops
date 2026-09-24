import unittest
from datetime import datetime, timezone, timedelta
from pyspark.sql import SparkSession
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    TimestampType
)


class TestSparkDeduplication(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = (
            SparkSession.builder
            .appName("TestSparkDeduplication")
            .master("local[1]")
            .config("spark.sql.shuffle.partitions", "1")
            .getOrCreate()
        )
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_deduplication_drops_duplicate_events(self):
        """
        검증: 동일한 (event_id, event_timestamp)를 가진 중복 이벤트가
        dropDuplicates에 의해 정확히 1건만 남고 제거되는지 확인
        """
        schema = StructType([
            StructField("event_id", StringType(), False),
            StructField("event_timestamp", TimestampType(), False),
            StructField("user_id", StringType(), False)
        ])

        now = datetime.now(timezone.utc)
        data = [
            # 정상 이벤트 1
            ("evt_001", now, "user_A"),
            # 중복 이벤트 1 (동일 event_id, 동일 timestamp) -> 4% 중복 모사
            ("evt_001", now, "user_A"),
            # 정상 이벤트 2
            ("evt_002", now + timedelta(seconds=1), "user_B"),
            # 지연 이벤트 3 (7분 전 타임스탬프) -> 3% 지연 모사 (15분 워터마크 범위 이내)
            ("evt_003", now - timedelta(minutes=7), "user_C")
        ]

        df = self.spark.createDataFrame(data, schema)

        # 워터마크 및 중복 제거 적용
        deduped_df = (
            df
            .withWatermark("event_timestamp", "15 minutes")
            .dropDuplicates(["event_id", "event_timestamp"])
        )

        result = deduped_df.collect()

        # 총 4건 중 1건 중복이 제거되어 3건이어야 함
        self.assertEqual(len(result), 3, "중복 이벤트가 정확히 1건 제거되어야 합니다.")
        
        event_ids = [row["event_id"] for row in result]
        self.assertIn("evt_001", event_ids)
        self.assertIn("evt_002", event_ids)
        self.assertIn("evt_003", event_ids)
        self.assertEqual(event_ids.count("evt_001"), 1, "evt_001은 중복 없이 단 1건만 존재해야 합니다.")


if __name__ == "__main__":
    unittest.main()
