import os
import sys
import logging
from typing import List, Tuple
from dotenv import load_dotenv

PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "../.."))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from services.api.schemas import ProductResult
from services.api.search_service import SearchService

logger = logging.getLogger("RAGService")


class RAGService:
    def __init__(self, search_service: SearchService):
        self.search_service = search_service
        self.api_key = os.getenv("GEMINI_API_KEY")
        if self.api_key:
            try:
                from google import genai
                self.client = genai.Client(api_key=self.api_key)
                self.has_llm = True
                self.model_name = "gemini-3.5-flash-lite"
                self.fallback_model = "gemini-3.6-flash"
                logger.info(f"Initialized Gemini LLM RAG client ({self.model_name})")
            except Exception as e:
                logger.warning(f"Failed to init Gemini client: {e}. Using deterministic RAG formatter.")
                self.has_llm = False
        else:
            self.has_llm = False

    def ask(self, question: str, limit: int = 3) -> Tuple[str, List[ProductResult]]:
        """
        검색 증강 생성(RAG): Qdrant 시맨틱 검색 결과 상품을 컨텍스트로 LLM 답변 생성
        """
        # 1. 관련 상품 검색 (Retrieval)
        products = self.search_service.search(query=question, limit=limit)

        if not products:
            return "죄송합니다, 문의하신 조건에 부합하는 상품을 찾지 못했습니다.", []

        # 2. 컨텍스트 구성
        context_lines = []
        for p in products:
            context_lines.append(
                f"- [{p.category} > {p.sub_category}] {p.product_name} | 가격: {p.price:,}원 | 설명: {p.description} (태그: {', '.join(p.tags)})"
            )
        context_str = "\n".join(context_lines)

        # 3. LLM 생성 (Generation)
        if self.has_llm:
            try:
                prompt = f"""
당신은 이커머스 전문 AI 쇼핑 어시스턴트입니다. 고객의 질문에 대해 아래 제공된 카탈로그 상품 정보만을 기반으로 친절하고 전문적으로 추천 답변을 작성해주세요.

[추천 후보 상품 목록]
{context_str}

[고객 질문]
{question}

고객이 만족할 수 있도록 상품의 특성과 가격대를 짚어주며 추천해주세요.
"""
                try:
                    response = self.client.models.generate_content(
                        model=self.model_name,
                        contents=prompt
                    )
                except Exception as e1:
                    logger.warning(f"Primary model {self.model_name} failed: {e1}. Retrying with {self.fallback_model}...")
                    response = self.client.models.generate_content(
                        model=self.fallback_model,
                        contents=prompt
                    )
                answer_text = response.text
                return answer_text, products
            except Exception as e:
                logger.warning(f"Gemini LLM call failed: {e}. Falling back to structured response.")

        # Fallback Deterministic Response
        recs = [f"'{p.product_name}' ({p.price:,}원)" for p in products[:2]]
        answer_text = (
            f"고객님의 질문 '{question}'에 맞추어 카탈로그에서 가장 적합한 상품을 찾았습니다.\n"
            f"추천 상품으로는 {', '.join(recs)} 등이 있습니다. "
            f"상세 스펙과 가격 정보를 확인해보세요!"
        )
        return answer_text, products
