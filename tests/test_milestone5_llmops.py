import os
import sys
import unittest
from fastapi.testclient import TestClient

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from services.api.app import app
from pipelines.llmops.embed_catalog import index_catalog


class TestMilestone5LLMOps(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        # 1. 테스트 실행 전 Qdrant 카탈로그 인덱싱 보장
        index_catalog(force_reindex=False)
        # 2. FastAPI TestClient 초기화
        cls.client = TestClient(app)

    def test_01_health_endpoint(self):
        """헬스체크 엔드포인트 및 Qdrant 포인트 수 검증"""
        with TestClient(app) as client:
            response = client.get("/health")
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["status"], "healthy")
            self.assertGreaterEqual(data["vector_db_points"], 8)
            self.assertIn(data["embedding_provider"].lower(), ["fastembed", "gemini"])

    def test_02_semantic_search_with_filter(self):
        """시맨틱 검색 및 카테고리/가격 페이로드 필터링 검증"""
        with TestClient(app) as client:
            payload = {
                "query": "출근용 단정한 블레이저 자켓",
                "category": "아우터",
                "max_price": 100000,
                "limit": 3
            }
            response = client.post("/api/v1/search", json=payload)
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertGreater(data["total_hits"], 0)
            first_prod = data["results"][0]
            self.assertEqual(first_prod["category"], "아우터")
            self.assertLessEqual(first_prod["price"], 100000)
            self.assertIn("블레이저", first_prod["product_name"])

    def test_03_personalized_recommendation_via_iceberg(self):
        """Iceberg 레이크하우스 사용자 클릭 이력 연동 개인화 추천 검증"""
        with TestClient(app) as client:
            payload = {
                "user_id": "user_0167",
                "limit": 3
            }
            response = client.post("/api/v1/recommend", json=payload)
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertEqual(data["user_id"], "user_0167")
            self.assertIn("recommendations", data)
            self.assertIsInstance(data["recommendations"], list)

    def test_04_rag_shopping_assistant(self):
        """RAG 기반 AI 쇼핑 어시스턴트 질의응답 검증"""
        with TestClient(app) as client:
            payload = {
                "question": "간절기에 가볍게 입을 아우터 추천해줘",
                "limit": 2
            }
            response = client.post("/api/v1/ask", json=payload)
            self.assertEqual(response.status_code, 200)
            data = response.json()
            self.assertIn("answer", data)
            self.assertGreater(len(data["answer"]), 10)
            self.assertGreaterEqual(len(data["referenced_products"]), 1)


if __name__ == "__main__":
    unittest.main()
