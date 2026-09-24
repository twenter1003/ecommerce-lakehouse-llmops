import os
import sys
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_timestamp, expr

# 프로젝트 루트 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from pipelines.streaming.schema import ecommerce_event_schema

# 로깅 설정
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("SparkIcebergSink")


def create_spark_session(warehouse_dir: str = None) -> SparkSession:
    """
    Kafka 연동 및 Apache Iceberg Hadoop Catalog가 구성된 SparkSession 생성
    """
    if warehouse_dir is None:
        warehouse_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../warehouse"))
    os.makedirs(warehouse_dir, exist_ok=True)

    packages = [
        "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.0",
        "org.apache.iceberg:iceberg-spark-runtime-4.0_2.13:1.11.0"
    ]
    packages_str = ",".join(packages)

    logger.info(f"Initializing SparkSession with Iceberg catalog at: {warehouse_dir}")
    return (
        SparkSession.builder
        .appName("EcommerceIcebergSink")
        .master("local[*]")
        .config("spark.jars.packages", packages_str)
        # 💡 Iceberg SQL 확장 기능 활성화 (MERGE, CALL, Time-Travel 등 지원)
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        # 💡 Iceberg Catalog 설정 (로컬 파일 시스템 기반 HadoopCatalog)
        .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.iceberg.type", "hadoop")
        .config("spark.sql.catalog.iceberg.warehouse", f"file://{warehouse_dir}")
        # 💡 셔플 파티션 최적화 (로컬 리소스 절약)
        .config("spark.sql.shuffle.partitions", "3")
        .getOrCreate()
    )


def init_iceberg_silver_table(spark: SparkSession):
    """
    Iceberg 네임스페이스 및 Silver 레이어 테이블 DDL 생성 (Hidden Partitioning: days(event_timestamp))
    """
    logger.info("Ensuring Iceberg namespace and 'silver_events' table exist...")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.ecommerce")
    spark.sql("""
    CREATE TABLE IF NOT EXISTS iceberg.ecommerce.silver_events (
        kafka_partition INT,
        kafka_offset BIGINT,
        broker_timestamp TIMESTAMP,
        traceparent STRING,
        event_id STRING,
        event_type STRING,
        event_timestamp TIMESTAMP,
        user_id STRING,
        session_id STRING,
        sequence_number INT,
        correlation_id STRING,
        product_id STRING,
        order_id STRING,
        items ARRAY<STRUCT<product_id: STRING, product_name: STRING, category: STRING, price: INT, quantity: INT>>,
        total_amount BIGINT,
        payment_method STRING,
        cancellation_reason STRING
    )
    USING iceberg
    PARTITIONED BY (days(event_timestamp))
    """)
    logger.info("Iceberg 'silver_events' table is ready.")


def compact_iceberg_table(spark: SparkSession, table_name: str = "ecommerce.silver_events"):
    """
    Iceberg 내장 프로시저 rewrite_data_files를 실행하여 스트리밍 적재 시 발생한 스몰 파일들을 병합(Compaction)
    """
    logger.info(f"Running Iceberg Compaction (rewrite_data_files) on {table_name}...")
    result = spark.sql(f"CALL iceberg.system.rewrite_data_files(table => '{table_name}')").collect()
    logger.info(f"Compaction finished: {result}")
    return result


def run_iceberg_stream(
    bootstrap_servers: str = "localhost:9092",
    topic: str = "ecommerce.events",
    checkpoint_dir: str = "checkpoints/iceberg_silver",
    starting_offsets: str = "earliest",
    trigger_interval: str = "1 minute",
    once: bool = False
):
    spark = create_spark_session()
    spark.sparkContext.setLogLevel("WARN")

    # 테이블 초기화
    init_iceberg_silver_table(spark)

    logger.info(f"Subscribing to Kafka topic '{topic}' at {bootstrap_servers} (offsets: {starting_offsets})")

    # 1. Kafka 스트림 소스 연결
    kafka_raw_df = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", starting_offsets)
        .option("includeHeaders", "true")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. W3C Traceparent 헤더 및 JSON 본문 파싱
    traceparent_expr = expr("filter(headers, h -> h.key = 'traceparent')[0].value").cast("string")

    parsed_df = kafka_raw_df.select(
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("broker_timestamp"),
        traceparent_expr.alias("traceparent"),
        from_json(
            col("value").cast("string"),
            ecommerce_event_schema,
            {"columnNameOfCorruptRecord": "_corrupt_record"}
        ).alias("data")
    )

    # 3. 데이터 정제 및 불량 레코드 격리
    clean_df = (
        parsed_df
        .select(
            col("kafka_partition"),
            col("kafka_offset"),
            col("broker_timestamp"),
            col("traceparent"),
            col("data.*")
        )
        .filter(col("_corrupt_record").isNull())
        .drop("_corrupt_record")
        .withColumn("event_timestamp", to_timestamp(col("event_timestamp")))
    )

    # 4. 워터마크(15분) 및 중복 제거
    deduped_df = (
        clean_df
        .withWatermark("event_timestamp", "15 minutes")
        .dropDuplicates(["event_id", "event_timestamp"])
    )

    # 5. Apache Iceberg Lakehouse 싱크 작성
    checkpoint_path = os.path.abspath(checkpoint_dir)
    writer = (
        deduped_df.writeStream
        .format("iceberg")
        .outputMode("append")
        .option("checkpointLocation", checkpoint_path)
    )

    if once:
        logger.info("Processing available records and committing to Iceberg (availableNow=True)...")
        query = writer.trigger(availableNow=True).toTable("iceberg.ecommerce.silver_events")
    else:
        logger.info(f"Starting continuous streaming to Iceberg with trigger {trigger_interval}...")
        query = writer.trigger(processingTime=trigger_interval).toTable("iceberg.ecommerce.silver_events")

    try:
        query.awaitTermination()
        logger.info("Stream processing completed successfully.")
    except KeyboardInterrupt:
        logger.info("Stopping stream gracefully...")
        query.stop()
    finally:
        spark.stop()


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Spark Structured Streaming to Apache Iceberg Sink")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="ecommerce.events", help="Kafka topic name")
    parser.add_argument("--checkpoint", default="checkpoints/iceberg_silver", help="Checkpoint directory")
    parser.add_argument("--starting-offsets", default="earliest", help="earliest or latest")
    parser.add_argument("--trigger", default="1 minute", help="Trigger interval (e.g. '30 seconds', '1 minute')")
    parser.add_argument("--once", action="store_true", help="Process all available records and stop")
    parser.add_argument("--compact", action="store_true", help="Run Iceberg Compaction (rewrite_data_files) and exit")
    args = parser.parse_args()

    if args.compact:
        spark = create_spark_session()
        compact_iceberg_table(spark)
        spark.stop()
    else:
        run_iceberg_stream(
            bootstrap_servers=args.bootstrap,
            topic=args.topic,
            checkpoint_dir=args.checkpoint,
            starting_offsets=args.starting_offsets,
            trigger_interval=args.trigger,
            once=args.once
        )
