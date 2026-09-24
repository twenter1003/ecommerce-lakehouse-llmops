import os
import logging
from typing import List

logger = logging.getLogger("EmbeddingProvider")


class EmbeddingProvider:
    """
    이커머스 상품 카탈로그 임베딩 엔진 (Dual-Engine 지원)
    1. GEMINI_API_KEY가 설정되어 있는 경우: Google Gemini text-embedding-004 (768 차원)
    2. API 키가 없거나 오프라인 환경인 경우: FastEmbed ONNX BAAI/bge-small-en-v1.5 (384 차원)
    """
    def __init__(self):
        self.api_key = os.getenv("GEMINI_API_KEY")
        if self.api_key:
            try:
                from google import genai
                self.client = genai.Client(api_key=self.api_key)
                self.provider_type = "gemini"
                self.model_name = "text-embedding-004"
                self.dimension = 768
                logger.info(f"Using Google Gemini Embedding API ({self.model_name}, dim={self.dimension})")
            except Exception as e:
                logger.warning(f"Failed to initialize Gemini Client ({e}). Falling back to FastEmbed.")
                self._init_fastembed()
        else:
            self._init_fastembed()

    def _init_fastembed(self):
        from fastembed import TextEmbedding
        self.provider_type = "fastembed"
        self.model_name = "BAAI/bge-small-en-v1.5"
        self.dimension = 384
        logger.info(f"Using Local FastEmbed Engine ({self.model_name}, dim={self.dimension})")
        self.model = TextEmbedding(model_name=self.model_name)

    def embed_texts(self, texts: List[str]) -> List[List[float]]:
        """
        문자열 리스트를 밀집 벡터(Dense Vector) 리스트로 변환
        """
        if not texts:
            return []

        if self.provider_type == "gemini":
            try:
                response = self.client.models.embed_content(
                    model=self.model_name,
                    contents=texts
                )
                return [emb.values for emb in response.embeddings]
            except Exception as e:
                logger.error(f"Gemini Embeddings call failed: {e}. Falling back to FastEmbed for this batch.")
                return [v.tolist() for v in self.model.embed(texts)]
        else:
            return [v.tolist() for v in self.model.embed(texts)]

    def embed_query(self, query: str) -> List[float]:
        """
        단일 검색 질의를 밀집 벡터로 변환
        """
        results = self.embed_texts([query])
        return results[0] if results else []
