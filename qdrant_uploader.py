from __future__ import annotations

import argparse
import os
import re
from pathlib import Path
from typing import Dict, Generator, Iterable, List, Tuple

from langchain_qdrant import QdrantVectorStore, FastEmbedSparse, RetrievalMode
from langchain_huggingface import HuggingFaceEmbeddings
from qdrant_client import QdrantClient


HEADER_LINE_RE = re.compile(r"^\[(?P<key>[a-z_]+)\]\s*(?P<value>.*)$", re.IGNORECASE)


def iterate_articles(file_path: str, limit: int | None = None) -> Generator[Tuple[Dict[str, str], str], None, None]:
    """Yield (metadata, text) for each article/law in the file.

    Format:
      [key] value\n
      ... (multiple header lines)

      <blank line>\n
      <article text (can span multiple lines)>\n
      <next header or EOF>
    """
    articles_yielded = 0
    metadata: Dict[str, str] = {}
    body_lines: List[str] = []
    in_header = False

    with open(file_path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.rstrip("\n")

            header_match = HEADER_LINE_RE.match(line)
            if header_match:
                # Starting a new header possibly means the previous article is complete
                if metadata and body_lines:
                    text = "\n".join(body_lines).strip()
                    if text:
                        yield metadata, text
                        articles_yielded += 1
                        if limit is not None and articles_yielded >= limit:
                            return
                    metadata = {}
                    body_lines = []

                in_header = True
                key = header_match.group("key").strip()
                value = header_match.group("value").strip()
                metadata[key] = value
                continue

            # Non-header line
            if in_header and line.strip() == "":
                # End of header section; body begins afterward
                in_header = False
                continue

            # Body line
            body_lines.append(line)

    # Flush last article at EOF
    if metadata and body_lines:
        text = "\n".join(body_lines).strip()
        if text:
            yield metadata, text


def batch_iterable(iterable: Iterable[Tuple[Dict[str, str], str]], batch_size: int) -> Generator[List[Tuple[Dict[str, str], str]], None, None]:
    batch: List[Tuple[Dict[str, str], str]] = []
    for item in iterable:
        batch.append(item)
        if len(batch) >= batch_size:
            yield batch
            batch = []
    if batch:
        yield batch


def resolve_input_file_path(user_path: str) -> str:
    # Accept absolute path, relative path, or bare filename (assume under output/)
    p = Path(user_path)
    if p.exists():
        return str(p)
    candidate = Path("output") / user_path
    if candidate.exists():
        return str(candidate)
    raise FileNotFoundError(f"Input file not found: {user_path}")


def ensure_collection(client: QdrantClient, collection_name: str, append: bool) -> None:
    exists = client.collection_exists(collection_name=collection_name)
    if exists and not append:
        client.delete_collection(collection_name=collection_name)


def build_embeddings() -> Tuple[HuggingFaceEmbeddings, FastEmbedSparse]:
    dense = HuggingFaceEmbeddings(model_name="ai-forever/FRIDA")
    sparse = FastEmbedSparse(model_name="Qdrant/bm25")
    return dense, sparse


def upload(
    file_path: str,
    collection_name: str,
    qdrant_url: str,
    batch_size: int,
    append: bool,
    limit: int | None,
) -> None:
    source_file = os.path.basename(file_path)
    file_stem = Path(file_path).stem

    client = QdrantClient(url=qdrant_url)
    ensure_collection(client, collection_name, append=append)

    dense_embeddings, sparse_embeddings = build_embeddings()

    # We'll initialize the store on the first batch using from_texts, then add_texts for subsequent batches
    store: QdrantVectorStore | None = None

    total_uploaded = 0
    article_index = 0

    # Determine starting integer ID
    next_point_id = 1
    try:
        if append and client.collection_exists(collection_name=collection_name):
            count_result = client.count(collection_name=collection_name, exact=False)
            # Fallback to 0 if count_result is missing for any reason
            current_count = getattr(count_result, "count", 0) or 0
            next_point_id = int(current_count) + 1
    except Exception:
        # If counting fails, start from 1
        next_point_id = 1

    for batch in batch_iterable(iterate_articles(file_path, limit=limit), batch_size=batch_size):
        texts: List[str] = []
        metadatas: List[Dict[str, str]] = []
        ids: List[int] = []

        for meta, text in batch:
            article_index += 1
            # Derive an id that is stable and informative
            number = meta.get("article_number") or meta.get("law_number") or str(article_index)
            article_uid = f"{file_stem}-art-{number}-{article_index}"

            meta_with_common = dict(meta)
            meta_with_common["source_file"] = source_file
            meta_with_common["article_uid"] = article_uid

            texts.append(text)
            metadatas.append(meta_with_common)
            ids.append(next_point_id)
            next_point_id += 1

        if not texts:
            continue

        if store is None:
            store = QdrantVectorStore.from_texts(
                texts=texts,
                embedding=dense_embeddings,
                sparse_embedding=sparse_embeddings,
                url=qdrant_url,
                collection_name=collection_name,
                retrieval_mode=RetrievalMode.HYBRID,
                vector_name="dense",
                sparse_vector_name="sparse",
                metadatas=metadatas,
                ids=ids,
            )
        else:
            store.add_texts(texts=texts, metadatas=metadatas, ids=ids)

        total_uploaded += len(texts)
        print(f"Uploaded batch: {len(texts)} | Total: {total_uploaded}")

    print(f"Done. Uploaded {total_uploaded} items into collection '{collection_name}'.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Upload legal articles/laws into Qdrant (HYBRID mode)")
    parser.add_argument("--file", required=True, help="Path to input file or name under output/")
    parser.add_argument("--collection", default=None, help="Qdrant collection name (default: file name stem)")
    parser.add_argument("--qdrant-url", default="http://127.0.0.1:6333", help="Qdrant URL")
    parser.add_argument("--batch-size", type=int, default=256, help="Upload batch size")
    parser.add_argument("--append", action="store_true", help="Append to existing collection instead of dropping it")
    parser.add_argument("--limit", type=int, default=None, help="Limit number of articles to upload (testing)")

    args = parser.parse_args()

    file_path = resolve_input_file_path(args.file)
    collection_name = args.collection or Path(file_path).stem

    upload(
        file_path=file_path,
        collection_name=collection_name,
        qdrant_url=args.qdrant_url,
        batch_size=args.batch_size,
        append=args.append,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()


