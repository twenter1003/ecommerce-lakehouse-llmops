import os
import sys
import logging
from typing import List, Tuple

import duckdb
from qdrant_client.models import Filter, FieldCondition, MatchValue

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from services.api.schemas import ProductResult
from services.api.search_service import SearchService

logger = logging.getLogger("RecommendService")


class RecommendService:
    def __init__(self, search_service: SearchService, warehouse_dir: str = None):
        self.search_service = search_service
        if warehouse_dir is None:
            self.warehouse_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../warehouse"))
        else:
            self.warehouse_dir = warehouse_dir
        self.iceberg_table_path = os.path.join(self.warehouse_dir, "ecommerce", "silver_events")

    def get_recent_user_views(self, user_id: str, limit: int = 5) -> List[str]:
        """
        DuckDB를 통해 Apache Iceberg Silver 테이블에서 해당 사용자의 최근 item_view 상품 ID 목록 조회
        """
        if not os.path.exists(self.iceberg_table_path):
            logger.warning(f"Iceberg table path does not exist: {self.iceberg_table_path}")
            return []

        try:
            con = duckdb.connect()
            con.execute("LOAD iceberg;")
            query = f"""
            SELECT product_id
            FROM iceberg_scan('{self.iceberg_table_path}')
            WHERE user_id = ? 
              AND event_type = 'item_view' 
              AND product_id IS NOT NULL
            ORDER BY event_timestamp DESC
            LIMIT ?
            """
            rows = con.execute(query, [user_id, limit]).fetchall()
            con.close()
            # 중복 제거 순서 유지
            seen = set()
            result = []
            for r in rows:
                p_id = r[0]
                if p_id and p_id not in seen:
                    seen.add(p_id)
                    result.append(p_id)
            return result
        except Exception as e:
            logger.warning(f"Failed to query user views from Iceberg: {e}")
            return []

    def recommend_for_user(self, user_id: str, limit: int = 5) -> Tuple[List[str], List[ProductResult]]:
        """
        사용자의 최근 클릭 상품을 기반으로 Qdrant에서 유사 상품을 찾아 추천 (Personalized Semantic Recommendation)
        """
        recent_pids = self.get_recent_user_views(user_id, limit=3)

        if not recent_pids:
            logger.info(f"User '{user_id}' has no prior view history. Providing popular default recommendations.")
            # Cold-start: 상위 인기 카테고리 상품 검색
            defaults = self.search_service.search(query="인기 베이직 데일리룩", limit=limit)
            return [], defaults

        # 가장 최근에 본 상품의 상세 텍스트를 검색 쿼리로 활용하여 유사 상품 추천
        # (실무에서는 아이템 임베딩 간 코사인 거리 평균 계산)
        target_pid = recent_pids[0]
        # Qdrant에서 target_pid 검색
        target_results = self.search_service.client.query_points(
            collection_name="ecommerce_products",
            query_filter=Filter(must=[FieldCondition(key="product_id", match=MatchValue(value=target_pid))]),
            limit=1
        )

        if target_results.points:
            target_payload = target_results.points[0].payload
            query_text = target_payload.get("search_text", target_payload.get("product_name", ""))
            candidates = self.search_service.search(query=query_text, limit=limit + len(recent_pids))
            # 이미 본 상품은 추천 목록에서 제외
            filtered = [c for c in candidates if c.product_id not in recent_pids][:limit]
            return recent_pids, filtered
        else:
            defaults = self.search_service.search(query="인기 베이직 데일리룩", limit=limit)
            return recent_pids, defaults
