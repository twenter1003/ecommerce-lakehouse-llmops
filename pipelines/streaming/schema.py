from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
    LongType,
    TimestampType,
    ArrayType
)

# [주문 상품 상세 스키마]
order_item_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("product_name", StringType(), True),
    StructField("category", StringType(), True),
    StructField("price", IntegerType(), True),
    StructField("quantity", IntegerType(), True)
])

# [이커머스 이벤트 본문 스키마]
# 💡 columnNameOfCorruptRecord 옵션을 위해 '_corrupt_record' 필드를 선언하여
#    파싱에 실패한 불량 JSON 행을 버리지 않고 격리 추적할 수 있도록 설계합니다.
ecommerce_event_schema = StructType([
    StructField("event_id", StringType(), False),
    StructField("event_type", StringType(), False),
    StructField("event_timestamp", StringType(), False),  # ISO 8601 문자열 -> 이후 Timestamp 변환
    StructField("user_id", StringType(), False),
    StructField("session_id", StringType(), False),
    
    # 감사 & 계보 추적 필드
    StructField("sequence_number", IntegerType(), True),
    StructField("correlation_id", StringType(), True),
    
    # 조회 / 장바구니 필드
    StructField("product_id", StringType(), True),
    
    # 주문 / 결제 / 취소 필드
    StructField("order_id", StringType(), True),
    StructField("items", ArrayType(order_item_schema), True),
    StructField("total_amount", LongType(), True),
    StructField("payment_method", StringType(), True),
    StructField("cancellation_reason", StringType(), True),
    
    # 불량 레코드 격리용 필드
    StructField("_corrupt_record", StringType(), True)
])
