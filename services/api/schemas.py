from typing import List, Optional
from pydantic import BaseModel, Field


class SearchRequest(BaseModel):
    query: str = Field(..., description="자연어 검색 질의 (예: 봄에 입기 좋은 단정한 출근용 자켓)", min_length=1)
    category: Optional[str] = Field(None, description="카테고리 필터 (예: 아우터, 상의, 하의, 신발, 잡화)")
    max_price: Optional[int] = Field(None, description="최대 가격 제한 (원)", ge=0)
    limit: int = Field(5, description="반환 상품 수", ge=1, le=20)


class ProductResult(BaseModel):
    product_id: str
    product_name: str
    category: str
    sub_category: str
    price: int
    description: str
    tags: List[str]
    score: float = Field(..., description="벡터 유사도 코사인 점수 (0.0 ~ 1.0)")


class SearchResponse(BaseModel):
    query: str
    total_hits: int
    results: List[ProductResult]


class RecommendRequest(BaseModel):
    user_id: str = Field(..., description="추천 대상 사용자 ID (예: user_0167)")
    limit: int = Field(5, description="추천 상품 수", ge=1, le=20)


class RecommendResponse(BaseModel):
    user_id: str
    recent_viewed_product_ids: List[str] = Field(..., description="Iceberg 레이크하우스에서 조회한 최근 클릭 상품 목록")
    recommendations: List[ProductResult]


class AskRequest(BaseModel):
    question: str = Field(..., description="쇼핑 가이드 질의 (예: 격식 있는 자리에서 입을 옷 추천해줘)")
    limit: int = Field(3, description="참조할 검색 상품 수", ge=1, le=5)


class AskResponse(BaseModel):
    question: str
    answer: str
    referenced_products: List[ProductResult]


class HealthResponse(BaseModel):
    status: str
    vector_db_points: int
    embedding_provider: str
