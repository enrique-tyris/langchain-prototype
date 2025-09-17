import os
from dotenv import load_dotenv
import vertexai
from langchain_pinecone import PineconeVectorStore
from pinecone import Pinecone
from embeddings import get_embedding_function

# === Configuración inicial ===
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env')
load_dotenv(env_path)

# Inicializar cliente de embeddings
vertexai.init(
    project=os.getenv("GOOGLE_CLOUD_PROJECT"),
    location="europe-west1"
)
embedding_function = get_embedding_function()

# Inicializar Pinecone
pc = Pinecone(api_key=os.getenv("PINECONE_API_KEY"))

index_name = "prototipo-3-onayu-longerchunks"
index = pc.Index(index_name)

# Conectar el índice a LangChain
vectorstore = PineconeVectorStore(index, embedding_function, text_key="text")

# Variable global para almacenar el namespace seleccionado
selected_namespace = None

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

def select_namespace():
    """Solicita al usuario que seleccione un namespace (documento)."""
    global selected_namespace
    
    print("\n🔍 Bienvenido al sistema de consulta de documentos")
    print("\n📋 Primero, selecciona el documento que quieres consultar:")
    print("   Ejemplo: 824, 825, 844, 854, 855")
    
    while True:
        namespace_input = input("\n📄 Ingresa el número del documento: ").strip()
        
        if not namespace_input:
            print("❌ Por favor ingresa un número de documento")
            continue
            
        # Construir el namespace completo
        selected_namespace = f"contrato-{namespace_input}"
        
        # Verificar que el namespace existe
        try:
            # Hacer una consulta de prueba para verificar que el namespace existe
            test_results = vectorstore.similarity_search("test", k=1, namespace=selected_namespace)
            print(f"✅ Documento '{selected_namespace}' seleccionado correctamente")
            return selected_namespace
        except Exception as e:
            print(f"❌ No se encontró el documento '{selected_namespace}'. Error: {str(e)}")
            print("💡 Verifica que el documento existe y está cargado en Pinecone")
            continue

def interactive_query():
    """Función principal para consultas interactivas."""
    global selected_namespace
    
    # Seleccionar namespace primero
    select_namespace()
    
    print(f"\n🎯 Consultando específicamente en: {selected_namespace}")
    print("Escribe 'salir' para terminar el programa")
    print("Escribe 'cambiar' para seleccionar otro documento")
    
    while True:
        print("\n" + "=" * 80)
        query = input("\n💭 Ingresa tu consulta: ").strip()
        
        if query.lower() in ['salir', 'exit', 'quit']:
            print("\n👋 ¡Hasta luego!")
            break
            
        if query.lower() in ['cambiar', 'change']:
            select_namespace()
            print(f"\n🎯 Consultando específicamente en: {selected_namespace}")
            continue
            
        if not query:
            print("❌ Por favor ingresa una consulta válida")
            continue
            
        try:
            print(f"\n🔎 Buscando en documento '{selected_namespace}'...")
            results = vectorstore.similarity_search(query, k=5, namespace=selected_namespace)
            
            if not results:
                print(f"\n❌ No se encontraron documentos relevantes en '{selected_namespace}'")
                continue
                
            print(f"\n✨ Se encontraron {len(results)} documentos relevantes:")
            for i, doc in enumerate(results):
                print_document_result(doc, i)
                
        except Exception as e:
            print(f"\n❌ Error al procesar la consulta: {str(e)}")

if __name__ == "__main__":
    interactive_query()