import os
from dotenv import load_dotenv
import vertexai
from langchain_pinecone import PineconeVectorStore
from pinecone import Pinecone
from embeddings import get_embedding_function

# === Configuración inicial ===
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), '.env')
load_dotenv(env_path)

# Inicializar cliente de embeddings
vertexai.init(
    project=os.getenv("GOOGLE_CLOUD_PROJECT"),
    location="europe-west1"
)
embedding_function = get_embedding_function()

# Inicializar Pinecone
pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))

index_name = "prototipo-2-onayu"
index = pc.Index(index_name)

# Conectar el índice a LangChain
vectorstore = PineconeVectorStore(index, embedding_function, text_key="text")

# === Función de consulta interactiva ===
def print_document_result(doc, index):
    """Imprime un documento resultado de forma formateada."""
    print(f"\n📄 Documento {index + 1}")
    print("=" * 80)
    print(f"Fuente: {doc.metadata.get('source', 'Desconocido')}")
    print(f"Página: {doc.metadata.get('page', 'N/A')}")
    print("-" * 80)
    print(doc.page_content)
    print("=" * 80)

def interactive_query():
    """Función principal para consultas interactivas."""
    print("\n🔍 Bienvenido al sistema de consulta de documentos")
    print("Escribe 'salir' para terminar el programa")
    
    while True:
        print("\n" + "=" * 80)
        query = input("\n💭 Ingresa tu consulta: ").strip()
        
        if query.lower() in ['salir', 'exit', 'quit']:
            print("\n👋 ¡Hasta luego!")
            break
            
        if not query:
            print("❌ Por favor ingresa una consulta válida")
            continue
            
        try:
            print("\n🔎 Buscando documentos relevantes...")
            results = vectorstore.similarity_search(query, k=5)
            
            if not results:
                print("\n❌ No se encontraron documentos relevantes")
                continue
                
            print(f"\n✨ Se encontraron {len(results)} documentos relevantes:")
            for i, doc in enumerate(results):
                print_document_result(doc, i)
                
        except Exception as e:
            print(f"\n❌ Error al procesar la consulta: {str(e)}")

if __name__ == "__main__":
    interactive_query()