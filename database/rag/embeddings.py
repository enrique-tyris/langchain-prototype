import os
import time
from dotenv import load_dotenv
import vertexai
from langchain_google_vertexai import VertexAIEmbeddings

def get_embedding_function(debug=False):
    """
    Creates and returns a VertexAI embedding function.
    
    Args:
        debug (bool): If True, prints debug logs. Defaults to False.
    
    Returns:
        VertexAIEmbeddings: The embedding function instance
    """
    # Load environment variables (.env with GOOGLE_CLOUD_PROJECT and credentials)
    t0 = time.time()
    # Get the project root directory (parent of database/rag/)
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    env_path = os.path.join(project_root, '.env')
    if debug:
        print(f"[LOG] Buscando .env en: {env_path}")
    load_dotenv(env_path)
    t1 = time.time()
    if debug:
        print(f"[LOG] Tiempo en cargar variables .env: {t1 - t0:.2f} s")

    if not os.getenv("GOOGLE_CLOUD_PROJECT"):
        raise ValueError("GOOGLE_CLOUD_PROJECT environment variable is not set")

    # Initialize Vertex AI
    t2 = time.time()
    vertexai.init(
        project=os.getenv("GOOGLE_CLOUD_PROJECT"),
        location="europe-west1"  # Using same region as working example
    )
    if debug:
        print('\n')
    # Create embeddings client
    embeddings = VertexAIEmbeddings(os.getenv("EMBEDDING_MODEL"))  # Using default model that worked in your environment
    t3 = time.time()
    if debug:
        print('\n')
        print(f"[LOG] Tiempo en inicializar Vertex AI + modelo: {t3 - t2:.2f} s")
    return embeddings

if __name__ == "__main__":
    try:
        # Get the embedding function with debug enabled
        embedding_function = get_embedding_function(debug=True)
        
        # 🔹 Llamada dummy para calentar (no medimos cold start aparte)
        print("[LOG] Ejecutando llamada dummy para evitar cold start...")
        td0 = time.time()
        _ = embedding_function.embed_query("Hola, esta es una llamada de prueba. Ignórame.")
        td1 = time.time()
        print(f"[LOG] Tiempo en dummy call: {td1 - td0:.2f} s\n")
        
        # 🔹 Inferencia real después de dummy
        text = "Hello World"
        print(f"\nCreando embeddings para texto: '{text}'")
        t4 = time.time()
        embeddings = embedding_function.embed_query(text)
        t5 = time.time()
        print(f"[LOG] Tiempo en inferencia (post-dummy): {t5 - t4:.2f} s")
        
        # Print results
        print("\n=== Resultados ===")
        print(f"Dimensión del embedding: {len(embeddings)}")
        print("Primeros 5 valores:", embeddings[:5])
        
    except Exception as e:
        print(f"Error: {str(e)}")
