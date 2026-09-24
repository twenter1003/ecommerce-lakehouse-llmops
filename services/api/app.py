import os
import sys
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from services.api.schemas import (
    SearchRequest,
    SearchResponse,
    RecommendRequest,
    RecommendResponse,
    AskRequest,
    AskResponse,
    HealthResponse
)
from services.api.search_service import SearchService
from services.api.recommend_service import RecommendService
from services.api.rag_service import RAGService

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("EcommerceLLMOpsAPI")

# 전역 서비스 인스턴스
search_service: SearchService = None
recommend_service: RecommendService = None
rag_service: RAGService = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global search_service, recommend_service, rag_service
    logger.info("Initializing LLMOps Services (Qdrant & Embedding Engines)...")
    search_service = SearchService()
    recommend_service = RecommendService(search_service)
    rag_service = RAGService(search_service)
    logger.info("LLMOps Services successfully initialized.")
    yield
    logger.info("Shutting down LLMOps Services...")
    if search_service and search_service.client:
        search_service.client.close()


app = FastAPI(
    title="E-Commerce Real-Time Lakehouse LLMOps API",
    description="Apache Iceberg 실시간 레이크하우스와 Qdrant 벡터 DB를 결합한 시맨틱 검색, 개인화 추천 및 RAG 서빙 API",
    version="1.0.0",
    lifespan=lifespan
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/health", response_model=HealthResponse, tags=["Health"])
def health_check():
    """
    서버 및 벡터 DB 상태 확인
    """
    pts = search_service.get_points_count() if search_service else 0
    provider_name = search_service.embedding_provider.provider_type if search_service else "unknown"
    return HealthResponse(
        status="healthy",
        vector_db_points=pts,
        embedding_provider=provider_name
    )


@app.post("/api/v1/search", response_model=SearchResponse, tags=["Semantic Search"])
def search_products(req: SearchRequest):
    """
    자연어 기반 상품 시맨틱 검색 (카테고리/가격 메타데이터 필터링 결합)
    """
    try:
        results = search_service.search(
            query=req.query,
            category=req.category,
            max_price=req.max_price,
            limit=req.limit
        )
        return SearchResponse(
            query=req.query,
            total_hits=len(results),
            results=results
        )
    except Exception as e:
        logger.error(f"Search failed: {e}")
        raise HTTPException(status_code=500, detail=f"Search failed: {str(e)}")


@app.post("/api/v1/recommend", response_model=RecommendResponse, tags=["Personalization"])
def recommend_products(req: RecommendRequest):
    """
    Apache Iceberg Silver 레이어의 사용자 최근 클릭스트림 이력을 기반으로 한 실시간 개인화 추천
    """
    try:
        recent_views, recs = recommend_service.recommend_for_user(
            user_id=req.user_id,
            limit=req.limit
        )
        return RecommendResponse(
            user_id=req.user_id,
            recent_viewed_product_ids=recent_views,
            recommendations=recs
        )
    except Exception as e:
        logger.error(f"Recommendation failed: {e}")
        raise HTTPException(status_code=500, detail=f"Recommendation failed: {str(e)}")


@app.post("/api/v1/ask", response_model=AskResponse, tags=["RAG Assistant"])
def ask_shopping_assistant(req: AskRequest):
    """
    카탈로그 검색 증강 생성(RAG) 기반 AI 쇼핑 어시스턴트 질의응답
    """
    try:
        answer, prods = rag_service.ask(
            question=req.question,
            limit=req.limit
        )
        return AskResponse(
            question=req.question,
            answer=answer,
            referenced_products=prods
        )
    except Exception as e:
        logger.error(f"RAG Assistant failed: {e}")
        raise HTTPException(status_code=500, detail=f"RAG Assistant failed: {str(e)}")


if __name__ == "__main__":
    import uvicorn
    uvicorn.run("services.api.app:app", host="0.0.0.0", port=8000, reload=True)
