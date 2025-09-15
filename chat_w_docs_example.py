import os
import time
import warnings
from dotenv import load_dotenv

# Suprimir warnings específicos
warnings.filterwarnings("ignore", category=UserWarning, module="vertexai._model_garden._model_garden_models")
warnings.filterwarnings("ignore", category=UserWarning, module="langsmith.client")
import vertexai
from langchain_google_vertexai import ChatVertexAI
from langchain_core.prompts import PromptTemplate
from langchain_pinecone import PineconeVectorStore
from pinecone import Pinecone
from langchain import hub
from langchain.chains.combine_documents import create_stuff_documents_chain
from langchain.chains.retrieval import create_retrieval_chain

# Load environment variables
env_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '.env')
load_dotenv(env_path)

# Initialize Vertex AI
vertexai.init(
    project=os.getenv("GOOGLE_CLOUD_PROJECT"),
    location="europe-west1"
)

if __name__ == "__main__":
    print("🔄 Inicializando chat con documentos...")

    # Inicializar modelo Gemini
    llm = ChatVertexAI(
        model="gemini-2.5-flash-lite",
        max_output_tokens=200
    )
    print("✨ Modelo inicializado")

    # Inicializar Pinecone
    pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))
    index_name = "prototipo-2-onayu"  # Usando el mismo índice que en query_rag.py
    index = pc.Index(index_name)

    # Obtener función de embeddings desde el módulo embeddings.py
    from database.rag.embeddings import get_embedding_function
    embedding_function = get_embedding_function()

    # Inicializar almacén de vectores
    vectorstore = PineconeVectorStore(
        index=index,
        embedding=embedding_function,
        text_key="text"
    )
    print("📚 Almacén de vectores conectado")

    # Configurar cadena RAG
    retrieval_qa_chat_prompt = hub.pull("langchain-ai/retrieval-qa-chat")
    combine_docs_chain = create_stuff_documents_chain(llm, retrieval_qa_chat_prompt)
    retrieval_chain = create_retrieval_chain(
        retriever=vectorstore.as_retriever(),
        combine_docs_chain=combine_docs_chain
    )

    # Bucle de chat interactivo
    print("\n🤖 ¡Chat inicializado! Escribe 'salir' para terminar la conversación")
    while True:
        query = input("\n💭 Tú: ").strip()
        
        if query.lower() in ['exit', 'quit', 'salir']:
            print("\n👋 ¡Hasta luego!")
            break
            
        if not query:
            print("❌ Por favor, ingresa una consulta válida")
            continue
            
        try:
            print("\n🔍 Buscando y procesando...")
            t0 = time.time()
            
            result = retrieval_chain.invoke({"input": query})
            
            t1 = time.time()
            print(f"\n🤖 Asistente: {result['answer']}")
            print(f"\n⏱️ Tiempo de respuesta: {t1 - t0:.2f}s")
            
        except Exception as e:
            print(f"\n❌ Error al procesar la consulta: {str(e)}")