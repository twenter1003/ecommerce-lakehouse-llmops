import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, expr

# 현재 디렉터리를 모듈 탐색 경로에 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from pipelines.streaming.schema import ecommerce_event_schema

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SparkCleanseStream")


def create_spark_session() -> SparkSession:
    """
    Kafka 연동 및 로컬 최적화가 적용된 SparkSession 초기화
    """
    logger.info("Initializing SparkSession with Kafka connector...")
    return (
        SparkSession.builder
        .appName("EcommerceCleanseStream")
        .master("local[*]")
        # Spark 4.0.0 + Scala 2.13용 공식 Kafka 커넥터 패키지 지정
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0")
        # 💡 [성능 최적화] 로컬 환경에서 불필요한 200개 셔플 파티션 방지 (카프카 파티션 수인 3으로 제한)
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )


def run_cleanse_stream(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "ecommerce.events",
    checkpoint_dir: str = "checkpoints/cleanse_stream",
    starting_offsets: str = "latest",
    once: bool = False
):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    logger.info(f"Subscribing to Kafka topic: '{topic}' at {bootstrap_servers} (offsets: {starting_offsets})")

    # 1. Kafka 소스 스트림 읽기
    kafka_raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("includeHeaders", "true")  # 💡 Kafka Record Headers를 포함하도록 활성화
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. 메시지 헤더(W3C Traceparent) 및 본문(JSON) 파싱
    traceparent_expr = expr("filter(headers, h -> h.key = 'traceparent')[0].value").cast("string")

    parsed_df = kafka_raw_df.select(
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("broker_timestamp"),  # Kafka 내장 타임스탬프 (Clock Skew 방어)
        traceparent_expr.alias("traceparent"),
        from_json(
            col("value").cast("string"),
            ecommerce_event_schema,
            {"columnNameOfCorruptRecord": "_corrupt_record"}
        ).alias("data")
    )

    # 3. 데이터 평탄화 및 불량 레코드 필터링
    clean_df = (
        parsed_df
        .select(
            col("kafka_partition"),
            col("kafka_offset"),
            col("broker_timestamp"),
            col("traceparent"),
            col("data.*")
        )
        # 파싱 오류가 발생한 레코드는 제거 (실무에서는 DLQ로 보낼 수 있음)
        .filter(col("_corrupt_record").isNull())
        .drop("_corrupt_record")
        # ISO 문자열 타임스탬프를 Spark TimestampType으로 변환
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
    )

    # 4. ⭐ [핵심 면접 포인트] 워터마크(Watermarking) 및 상태 기반 중복 제거(Deduplication)
    # 💡 withWatermark: 15분 워터마크 설정 (생성기에서 주입하는 최대 10분 지연 데이터 수용)
    # 💡 dropDuplicates: 워터마크 컬럼(event_timestamp)을 반드시 포함시켜야
    #    워터마크가 지난 오래된 상태가 StateStore에서 정리(Eviction)되어 OOM을 방지합니다.
    deduped_df = (
        clean_df
        .withWatermark("event_timestamp", "15 minutes")
        .dropDuplicates(["event_id", "event_timestamp"])
    )

    writer = (
        deduped_df.writeStream
        .format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("checkpointLocation", checkpoint_dir)
    )

    if once:
        logger.info("Running in single-batch verification mode (availableNow=True)...")
        query = writer.trigger(availableNow=True).start()
    else:
        logger.info("Starting Streaming Query (Console Sink, 2s micro-batch)...")
        query = writer.trigger(processingTime="2 seconds").start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("Stopping Streaming Query gracefully...")
        query.stop()
        spark.stop()
        logger.info("Streaming Query stopped.")


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Spark Structured Streaming Cleansing Pipeline")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka broker address")
    parser.add_argument("--topic", default="ecommerce.events", help="Kafka topic")
    parser.add_argument("--checkpoint", default="checkpoints/cleanse_stream", help="Checkpoint directory")
    parser.add_argument("--starting-offsets", default="latest", help="starting offsets: earliest or latest")
    parser.add_argument("--once", action="store_true", help="Process all available records and stop (for testing)")
    args = parser.parse_args()

    run_cleanse_stream(
        bootstrap_servers=args.bootstrap,
        topic=args.topic,
        checkpoint_dir=args.checkpoint,
        starting_offsets=args.starting_offsets,
        once=args.once
    )
