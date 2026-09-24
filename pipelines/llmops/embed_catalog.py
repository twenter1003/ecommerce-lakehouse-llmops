import os
import sys
import json
import hashlib
import logging
from typing import List, Dict, Any

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    VectorParams,
    PointStruct,
    PayloadSchemaType
)

# 루트 경로 추가
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../..")))
from pipelines.llmops.embedding_provider import EmbeddingProvider

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger("CatalogIndexer")

COLLECTION_NAME = "ecommerce_products"


def get_qdrant_client(storage_path: str = None) -> QdrantClient:
    """
    Qdrant 로컬 임베디드 클라이언트 초기화 (도커 불필요, zero overhead)
    """
    if storage_path is None:
        storage_path = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../warehouse/qdrant"))
    os.makedirs(storage_path, exist_ok=True)
    return QdrantClient(path=storage_path)


def init_qdrant_collection(client: QdrantClient, dimension: int):
    """
    Qdrant 컬렉션 및 HNSW / Payload 색인 초기화
    """
    collections = [c.name for c in client.get_collections().collections]
    if COLLECTION_NAME in collections:
        col_info = client.get_collection(COLLECTION_NAME)
        current_dim = col_info.config.params.vectors.size
        if current_dim != dimension:
            logger.warning(f"Collection dimension mismatch ({current_dim} != {dimension}). Recreating collection...")
            client.delete_collection(COLLECTION_NAME)
            collections.remove(COLLECTION_NAME)

    if COLLECTION_NAME not in collections:
        logger.info(f"Creating Qdrant collection '{COLLECTION_NAME}' (dim={dimension}, Cosine)...")
        client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(size=dimension, distance=Distance.COSINE)
        )
        # 페이로드 필터링을 위한 필드 인덱스 생성
        client.create_payload_index(
            collection_name=COLLECTION_NAME,
            field_name="category",
            field_schema=PayloadSchemaType.KEYWORD
        )
        client.create_payload_index(
            collection_name=COLLECTION_NAME,
            field_name="price",
            field_schema=PayloadSchemaType.INTEGER
        )
        logger.info(f"Collection '{COLLECTION_NAME}' created with category/price payload indexes.")
    else:
        logger.info(f"Collection '{COLLECTION_NAME}' already exists (dim={dimension}).")


def build_search_text(p: Dict[str, Any]) -> str:
    """
    상품 메타데이터를 검색용 텍스트 청크로 합성
    """
    tags_str = ", ".join(p.get("tags", []))
    return f"[{p.get('category')} > {p.get('sub_category')}] {p.get('product_name')} (태그: {tags_str}) {p.get('description')}"


def index_catalog(
    products_file: str = "data/products.json",
    qdrant_path: str = None,
    force_reindex: bool = False
):
    """
    상품 카탈로그 증분 임베딩 & Qdrant Upsert 파이프라인
    - content_hash(MD5)를 활용하여 내용이 변경된 상품만 선택적으로 임베딩 (API 비용 90% 절감)
    """
    provider = EmbeddingProvider()
    client = get_qdrant_client(qdrant_path)

    # 1. 컬렉션 초기화
    init_qdrant_collection(client, provider.dimension)

    # 2. 상품 데이터 로드
    with open(products_file, "r", encoding="utf-8") as f:
        products: List[Dict[str, Any]] = json.load(f)

    logger.info(f"Loaded {len(products)} products from {products_file}")

    # 3. 기존 색인된 상품 및 content_hash 조회 (캐싱 비교)
    existing_hashes = {}
    if not force_reindex:
        try:
            scroll_result, _ = client.scroll(
                collection_name=COLLECTION_NAME,
                limit=1000,
                with_payload=True,
                with_vectors=False
            )
            for point in scroll_result:
                payload = point.payload or {}
                p_id = payload.get("product_id")
                c_hash = payload.get("content_hash")
                if p_id and c_hash:
                    existing_hashes[p_id] = c_hash
            logger.info(f"Found {len(existing_hashes)} cached products in Qdrant.")
        except Exception as e:
            logger.warning(f"Failed to scroll existing points: {e}")

    # 4. 변경된 상품 식별
    to_embed = []
    to_embed_texts = []
    skipped_count = 0

    for idx, p in enumerate(products):
        search_text = build_search_text(p)
        content_hash = hashlib.md5(search_text.encode("utf-8")).hexdigest()

        # 정수형 포인트 ID 생성 (MD5 앞 8자리)
        point_id = int(hashlib.md5(p["product_id"].encode("utf-8")).hexdigest()[:8], 16)

        if not force_reindex and existing_hashes.get(p["product_id"]) == content_hash:
            skipped_count += 1
            continue

        p["content_hash"] = content_hash
        p["search_text"] = search_text
        p["point_id"] = point_id

        to_embed.append(p)
        to_embed_texts.append(search_text)

    logger.info(f"Embedding check: {len(to_embed)} new/updated, {skipped_count} skipped (cached).")

    # 5. 증분 임베딩 및 Qdrant Upsert
    if to_embed:
        logger.info(f"Generating embeddings for {len(to_embed)} products...")
        embeddings = provider.embed_texts(to_embed_texts)

        points = []
        for p, vec in zip(to_embed, embeddings):
            points.append(PointStruct(
                id=p["point_id"],
                vector=vec,
                payload={
                    "product_id": p["product_id"],
                    "product_name": p["product_name"],
                    "category": p["category"],
                    "sub_category": p["sub_category"],
                    "price": p["price"],
                    "description": p["description"],
                    "tags": p["tags"],
                    "content_hash": p["content_hash"],
                    "search_text": p["search_text"]
                }
            ))

        client.upsert(
            collection_name=COLLECTION_NAME,
            points=points
        )
        logger.info(f"Successfully upserted {len(points)} vectors into Qdrant '{COLLECTION_NAME}'.")

    # 6. 인덱스 요약 정보 출력
    collection_info = client.get_collection(COLLECTION_NAME)
    logger.info(f"Qdrant collection status: {collection_info.points_count} points indexed.")
    client.close()
    return collection_info.points_count


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser(description="Embed Catalog and Index into Qdrant")
    parser.add_argument("--products", default="data/products.json", help="Path to products.json")
    parser.add_argument("--force", action="store_true", help="Force re-indexing all products")
    args = parser.parse_args()

    index_catalog(products_file=args.products, force_reindex=args.force)
