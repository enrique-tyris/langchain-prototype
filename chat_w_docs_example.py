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
    index_name = "prototipo-3-onayu-longerchunks"  # Usando el mismo índice que en query_rag.py
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

    # Variable global para almacenar el namespace seleccionado
    selected_namespace = None

    def select_namespace():
        """Solicita al usuario que seleccione un namespace (documento)."""
        global selected_namespace
        
        print("\n🔍 Bienvenido al chat con documentos")
        print("\n📋 Primero, selecciona el documento que quieres consultar:")
        print("   Ejemplo: 824, 825, 855")
        
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

    # Configurar cadena RAG
    retrieval_qa_chat_prompt = hub.pull("langchain-ai/retrieval-qa-chat")
    combine_docs_chain = create_stuff_documents_chain(llm, retrieval_qa_chat_prompt)

    # Seleccionar namespace primero
    select_namespace()

    # Verificar que el namespace tiene contenido antes de configurar el retriever
    print(f"🔍 Verificando contenido del documento '{selected_namespace}'...")
    test_results = vectorstore.similarity_search("test", k=1, namespace=selected_namespace)
    if not test_results:
        print(f"❌ El documento '{selected_namespace}' no contiene datos o no existe")
        print("💡 Verifica que el documento fue cargado correctamente en Pinecone")
        exit(1)
    
    # Configurar retriever con namespace específico
    retriever = vectorstore.as_retriever(
        search_kwargs={"namespace": selected_namespace, "k": 5}
    )
    retrieval_chain = create_retrieval_chain(
        retriever=retriever,
        combine_docs_chain=combine_docs_chain
    )

    # Bucle de chat interactivo
    print(f"\n🤖 ¡Chat inicializado para documento '{selected_namespace}'!")
    print("Escribe 'salir' para terminar la conversación")
    print("Escribe 'cambiar' para seleccionar otro documento")

    while True:
        query = input("\n💭 Tú: ").strip()
        
        if query.lower() in ['exit', 'quit', 'salir']:
            print("\n👋 ¡Hasta luego!")
            break
            
        if query.lower() in ['cambiar', 'change']:
            select_namespace()
            
            # Verificar que el nuevo namespace tiene contenido
            print(f"🔍 Verificando contenido del documento '{selected_namespace}'...")
            test_results = vectorstore.similarity_search("test", k=1, namespace=selected_namespace)
            if not test_results:
                print(f"❌ El documento '{selected_namespace}' no contiene datos o no existe")
                print("💡 Verifica que el documento fue cargado correctamente en Pinecone")
                continue
            
            # Reconfigurar retriever con nuevo namespace
            retriever = vectorstore.as_retriever(
                search_kwargs={"namespace": selected_namespace, "k": 5}
            )
            retrieval_chain = create_retrieval_chain(
                retriever=retriever,
                combine_docs_chain=combine_docs_chain
            )
            print(f"\n🤖 Chat reconfigurado para documento '{selected_namespace}'")
            continue
            
        if not query:
            print("❌ Por favor, ingresa una consulta válida")
            continue
            
        try:
            print(f"\n🔍 Buscando en documento '{selected_namespace}'...")
            t0 = time.time()
            
            result = retrieval_chain.invoke({"input": query})
            
            t1 = time.time()
            print(f"\n🤖 Asistente: {result['answer']}")
            print(f"\n⏱️ Tiempo de respuesta: {t1 - t0:.2f}s")
            
        except Exception as e:
            print(f"\n❌ Error al procesar la consulta: {str(e)}")