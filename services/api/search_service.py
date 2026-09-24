import os
import sys
import logging
from typing import List, Optional

from qdrant_client import QdrantClient
from qdrant_client.models import Filter, FieldCondition, MatchValue, Range

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from pipelines.llmops.embedding_provider import EmbeddingProvider
from pipelines.llmops.embed_catalog import COLLECTION_NAME, get_qdrant_client
from services.api.schemas import ProductResult

logger = logging.getLogger("SearchService")


class SearchService:
    def __init__(self, qdrant_path: str = None):
        self.client = get_qdrant_client(qdrant_path)
        self.embedding_provider = EmbeddingProvider()

    def search(
        self,
        query: str,
        category: Optional[str] = None,
        max_price: Optional[int] = None,
        limit: int = 5
    ) -> List[ProductResult]:
        """
        Qdrant 기반 시맨틱 유사도 검색 + 메타데이터 페이로드 필터링
        """
        query_vector = self.embedding_provider.embed_query(query)

        # 페이로드 필터 구성
        must_conditions = []
        if category:
            must_conditions.append(FieldCondition(key="category", match=MatchValue(value=category)))
        if max_price is not None:
            must_conditions.append(FieldCondition(key="price", range=Range(lte=max_price)))

        query_filter = Filter(must=must_conditions) if must_conditions else None

        response = self.client.query_points(
            collection_name=COLLECTION_NAME,
            query=query_vector,
            query_filter=query_filter,
            limit=limit
        )

        results = []
        for point in response.points:
            payload = point.payload or {}
            results.append(ProductResult(
                product_id=payload.get("product_id", ""),
                product_name=payload.get("product_name", ""),
                category=payload.get("category", ""),
                sub_category=payload.get("sub_category", ""),
                price=payload.get("price", 0),
                description=payload.get("description", ""),
                tags=payload.get("tags", []),
                score=round(float(point.score), 4)
            ))
        return results

    def get_points_count(self) -> int:
        try:
            info = self.client.get_collection(COLLECTION_NAME)
            return info.points_count
        except Exception:
            return 0
