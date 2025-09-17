from dotenv import load_dotenv
import os
import argparse
from typing import List, Dict, Tuple
from collections import defaultdict
import vertexai
from embeddings import get_embedding_function
from pinecone import Pinecone
from pinecone.exceptions import NotFoundException
from langchain_community.document_loaders import PyPDFDirectoryLoader
from langchain_text_splitters import RecursiveCharacterTextSplitter
from langchain.schema.document import Document

# Load environment variables
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env')
load_dotenv(env_path)

# Initialize Vertex AI for embeddings
vertexai.init(
    project=os.getenv("GOOGLE_CLOUD_PROJECT"),
    location="europe-west1"
)

# Initialize Pinecone
pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
INDEX_NAME = os.getenv("PINECONE_INDEX_NAME", "prototipo-3-onayu-longerchunks")

# Constants for document processing
CHUNK_SIZE = 3000
CHUNK_OVERLAP = 400
DATA_PATH = os.getenv("PDF_DATA_PATH", "data/raw")

def main():
    """Main function to process PDFs and upload to Pinecone."""
    parser = argparse.ArgumentParser()
    parser.add_argument("--reset", action="store_true", help="Reset the Pinecone index.")
    args = parser.parse_args()

    # Initialize embedding function
    embedding_function = get_embedding_function()
    
    # Initialize Pinecone index
    index = pc.Index(INDEX_NAME)
    
    if args.reset:
        clear_pinecone_index(index)

    # 1) Cargar por páginas
    page_docs = load_documents()

    # 2) Unir por PDF y construir mapa de spans por página
    merged_docs = merge_pages_by_source(page_docs)

    # 3) Chunks grandes + cálculo de páginas por chunk
    chunks = split_documents_with_pages(merged_docs)

    # 4) Metadata final (id, pages, page_start/end)
    chunks_with_metadata = prepare_chunks_for_pinecone(chunks)
    
    # 5) Subir a Pinecone (por namespace=nombre_pdf)
    upload_to_pinecone(chunks_with_metadata, index, embedding_function)

def load_documents() -> List[Document]:
    """Load PDF documents from the data directory, one Document per page."""
    print(f"📚 Loading PDF documents by pages from {DATA_PATH}")
    document_loader = PyPDFDirectoryLoader(DATA_PATH, mode='page')
    return document_loader.load()

def merge_pages_by_source(page_docs: List[Document]) -> List[Document]:
    """
    Une todas las páginas de cada PDF en un solo Document grande por archivo,
    guardando un mapa de rangos de caracteres por página para calcular luego
    qué páginas toca cada chunk.
    """
    print("🧵 Merging pages per source and building page span maps")
    by_source: Dict[str, List[Document]] = {}
    for d in page_docs:
        source = os.path.basename(d.metadata.get("source", "unknown"))
        by_source.setdefault(source, []).append(d)

    merged_docs: List[Document] = []
    for source, docs in by_source.items():
        docs.sort(key=lambda x: x.metadata.get("page", 0))

        combined_text_parts: List[str] = []
        page_spans: List[Tuple[int, int, int]] = []  # (start_idx, end_idx, page_number)
        cursor = 0

        for i, d in enumerate(docs):
            page_num = int(d.metadata.get("page", i))
            text = d.page_content or ""
            if combined_text_parts:
                sep = "\n"
                combined_text_parts.append(sep)
                cursor += len(sep)
            start = cursor
            combined_text_parts.append(text)
            cursor += len(text)
            end = cursor
            page_spans.append((start, end, page_num))

        combined_text = "".join(combined_text_parts)
        all_pages_sorted = [p for *_, p in page_spans]

        merged_docs.append(
            Document(
                page_content=combined_text,
                metadata={
                    "source": source,
                    "page_spans": page_spans,
                    "all_pages": all_pages_sorted,
                },
            )
        )
    print(f"   ✅ Merged into {len(merged_docs)} documents (one per PDF)")
    return merged_docs

def split_documents_with_pages(merged_docs: List[Document]) -> List[Document]:
    """
    Hace chunks grandes sobre el Document unido y calcula las páginas que abarca cada chunk
    usando los índices de caracteres del split y el mapa page_spans.
    """
    print("✂️ Splitting merged documents into large chunks with page tracking")
    splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        length_function=len,
        is_separator_regex=False,
        add_start_index=True,  # 👈 imprescindible para mapear a páginas
    )

    chunks: List[Document] = []

    for base_doc in merged_docs:
        page_spans: List[Tuple[int, int, int]] = base_doc.metadata.get("page_spans", [])
        split_docs = splitter.split_documents([base_doc])

        for ch in split_docs:
            start_idx = ch.metadata.get("start_index", None)
            if start_idx is None:
                pages = base_doc.metadata.get("all_pages", [])
            else:
                end_idx = start_idx + len(ch.page_content)
                pages = pages_overlapping_range(start_idx, end_idx, page_spans)

            if pages:
                pages_sorted = sorted(set(int(p) for p in pages))
                page_start = pages_sorted[0]
                page_end = pages_sorted[-1]
            else:
                pages_sorted = []
                page_start = None
                page_end = None

            ch.metadata = {
                "source": os.path.basename(base_doc.metadata.get("source", "unknown")),
                "pages": pages_sorted,     # lista de enteros (temporal)
                "page_start": page_start,  # int o None
                "page_end": page_end,      # int o None
            }
            chunks.append(ch)

    print(f"   ✅ Produced {len(chunks)} chunks")
    return chunks

def pages_overlapping_range(start_idx: int, end_idx: int, page_spans: List[Tuple[int, int, int]]) -> List[int]:
    """
    Devuelve las páginas cuyos rangos [p_start, p_end) se solapan con [start_idx, end_idx).
    page_spans: lista de tuplas (p_start, p_end, page_number).
    """
    pages = []
    for p_start, p_end, p_num in page_spans:
        if not (end_idx <= p_start or start_idx >= p_end):
            pages.append(p_num)
    return pages

def prepare_chunks_for_pinecone(chunks: List[Document]) -> List[Document]:
    """Prepare document chunks with proper metadata for Pinecone (chunk_index por documento)."""
    print("🏷️ Preparing chunks with metadata")
    processed_chunks = []
    per_doc_counter = defaultdict(int)  # 👈 contador por 'source'

    for chunk in chunks:
        source = os.path.basename(chunk.metadata.get("source", "unknown"))
        pages_nums: List[int] = chunk.metadata.get("pages", [])

        # índice local por documento (empieza en 0)
        per_doc_counter[source] += 1
        local_idx = per_doc_counter[source] - 1

        # etiqueta de páginas para el ID
        if pages_nums:
            pg_label = f"{pages_nums[0]}-{pages_nums[-1]}" if len(pages_nums) > 1 else f"{pages_nums[0]}"
        else:
            pg_label = "unknown"

        chunk_id = f"{source}:{pg_label}:{local_idx}"

        # Pinecone v3: 'pages' debe ser List[str]
        pages_str: List[str] = [str(p) for p in pages_nums]

        # Metadata limpia (sin None)
        meta = {
            "id": chunk_id,
            "source": source,
            "pages": pages_str,          # List[str]
            "chunk_index": local_idx,    # 👈 ahora por documento
        }
        if chunk.metadata.get("page_start") is not None:
            meta["page_start"] = int(chunk.metadata["page_start"])
        if chunk.metadata.get("page_end") is not None:
            meta["page_end"] = int(chunk.metadata["page_end"])

        chunk.metadata = meta
        processed_chunks.append(chunk)

    return processed_chunks

def upload_to_pinecone(chunks: List[Document], index, embedding_function) -> None:
    """Upload document chunks to Pinecone with namespaces per document."""
    print("⬆️ Uploading chunks to Pinecone with namespaces")
    
    # Group chunks by document source
    chunks_by_document: Dict[str, List[Document]] = {}
    for chunk in chunks:
        source = chunk.metadata.get("source", "unknown")
        doc_name = os.path.basename(source).replace('.pdf', '').replace(' ', '_')
        chunks_by_document.setdefault(doc_name, []).append(chunk)
    
    # Process each document separately
    total_uploaded = 0
    for doc_name, doc_chunks in chunks_by_document.items():
        print(f"📄 Processing document: {doc_name} ({len(doc_chunks)} chunks)")
        
        vectors = []
        for chunk in doc_chunks:
            embedding = embedding_function.embed_query(chunk.page_content)

            # Construimos metadata final (sin None; pages ya son strings)
            meta = {
                "text": chunk.page_content,
                "source": chunk.metadata["source"],
                "pages": chunk.metadata.get("pages", []),  # List[str]
                "chunk_index": chunk.metadata["chunk_index"],
            }
            if "page_start" in chunk.metadata:
                meta["page_start"] = chunk.metadata["page_start"]  # int
            if "page_end" in chunk.metadata:
                meta["page_end"] = chunk.metadata["page_end"]      # int

            vector = {
                "id": chunk.metadata["id"],
                "values": embedding,
                "metadata": meta,
            }
            vectors.append(vector)
        
        batch_size = 15
        for i in range(0, len(vectors), batch_size):
            batch = vectors[i:i + batch_size]
            index.upsert(vectors=batch, namespace=doc_name)
            uploaded_count = min(i + batch_size, len(vectors))
            print(f"   ✅ Uploaded {uploaded_count}/{len(vectors)} vectors to namespace '{doc_name}'")
        
        total_uploaded += len(vectors)
        print(f"🎉 Document '{doc_name}' completed: {len(vectors)} vectors uploaded")
    
    print(f"🚀 Total uploaded: {total_uploaded} vectors across {len(chunks_by_document)} namespaces")

def clear_pinecone_index(index):
    """Borra todos los namespaces existentes del índice de forma segura."""
    print("✨ Clearing Pinecone Index (all namespaces)")

    try:
        stats = index.describe_index_stats()
    except NotFoundException:
        print("⚠️ Index not found; nothing to clear.")
        return

    namespaces = []
    if isinstance(stats, dict):
        ns_info = stats.get("namespaces") or {}
        namespaces = list(ns_info.keys())
    else:
        ns_info = getattr(stats, "namespaces", {}) or {}
        namespaces = list(ns_info.keys())

    if not namespaces:
        try:
            index.delete(delete_all=True, namespace="")
            print("   ✅ Cleared default namespace")
        except NotFoundException:
            print("   ✅ Nothing to clear (no namespaces found)")
        return

    for ns in namespaces:
        try:
            index.delete(delete_all=True, namespace=ns)
            print(f"   ✅ Cleared namespace '{ns}'")
        except NotFoundException:
            print(f"   ⚠️ Namespace '{ns}' already not found; skipping")

if __name__ == "__main__":
    main()
