from dotenv import load_dotenv
import os
import argparse
from typing import List
import vertexai
from embeddings import get_embedding_function
from pinecone import Pinecone
from langchain_pinecone import PineconeVectorStore
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
        print("✨ Clearing Pinecone Index")
        index.delete(delete_all=True)

    # Load and process documents
    documents = load_documents()
    chunks = split_documents(documents)
    chunks_with_metadata = prepare_chunks_for_pinecone(chunks)
    
    # Upload to Pinecone
    upload_to_pinecone(chunks_with_metadata, index, embedding_function)

def load_documents() -> List[Document]:
    """Load PDF documents from the data directory."""
    print(f"📚 Loading PDF documents from {DATA_PATH}")
    document_loader = PyPDFDirectoryLoader(DATA_PATH, mode='single')
    return document_loader.load()

def split_documents(documents: List[Document]) -> List[Document]:
    """Split documents into smaller chunks."""
    print("✂️ Splitting documents into chunks")
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=CHUNK_SIZE,
        chunk_overlap=CHUNK_OVERLAP,
        length_function=len,
        is_separator_regex=False,
    )
    return text_splitter.split_documents(documents)

def prepare_chunks_for_pinecone(chunks: List[Document]) -> List[Document]:
    """Prepare document chunks with proper metadata for Pinecone."""
    print("🏷️ Preparing chunks with metadata")
    processed_chunks = []
    
    for i, chunk in enumerate(chunks):
        # Extract source filename and page number
        source = os.path.basename(chunk.metadata.get("source", "unknown"))
        page = chunk.metadata.get("page", 0)
        
        # Create a unique ID for the chunk
        chunk_id = f"{source}:{page}:{i}"
        
        # Update metadata
        chunk.metadata.update({
            "id": chunk_id,
            "source": source,
            "page": page,
            "chunk_index": i,
        })
        
        processed_chunks.append(chunk)
    
    return processed_chunks

def upload_to_pinecone(chunks: List[Document], index, embedding_function) -> None:
    """Upload document chunks to Pinecone with namespaces per document."""
    print("⬆️ Uploading chunks to Pinecone with namespaces")
    
    # Group chunks by document source
    chunks_by_document = {}
    for chunk in chunks:
        source = chunk.metadata.get("source", "unknown")
        doc_name = os.path.basename(source).replace('.pdf', '').replace(' ', '_')
        
        if doc_name not in chunks_by_document:
            chunks_by_document[doc_name] = []
        chunks_by_document[doc_name].append(chunk)
    
    # Process each document separately
    total_uploaded = 0
    for doc_name, doc_chunks in chunks_by_document.items():
        print(f"📄 Processing document: {doc_name} ({len(doc_chunks)} chunks)")
        
        # Prepare vectors for this document
        vectors = []
        for chunk in doc_chunks:
            # Generate embedding for the chunk
            embedding = embedding_function.embed_query(chunk.page_content)
            
            # Create vector record
            vector = {
                "id": chunk.metadata["id"],
                "values": embedding,
                "metadata": {
                    "text": chunk.page_content,
                    "source": chunk.metadata["source"],
                    "page": chunk.metadata["page"],
                    "chunk_index": chunk.metadata["chunk_index"]
                }
            }
            vectors.append(vector)
        
        # Upload to Pinecone with namespace
        batch_size = 15
        for i in range(0, len(vectors), batch_size):
            batch = vectors[i:i + batch_size]
            index.upsert(vectors=batch, namespace=doc_name)
            uploaded_count = min(i + batch_size, len(vectors))
            print(f"   ✅ Uploaded {uploaded_count}/{len(vectors)} vectors to namespace '{doc_name}'")
        
        total_uploaded += len(vectors)
        print(f"🎉 Document '{doc_name}' completed: {len(vectors)} vectors uploaded")
    
    print(f"🚀 Total uploaded: {total_uploaded} vectors across {len(chunks_by_document)} namespaces")


if __name__ == "__main__":
    main()
