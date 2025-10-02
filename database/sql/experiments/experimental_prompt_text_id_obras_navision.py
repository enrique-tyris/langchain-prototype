import os
from dotenv import load_dotenv
import vertexai
from langchain_google_vertexai import ChatVertexAI
from langchain_core.prompts import PromptTemplate

# Load environment variables
env_path = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), '.env')
load_dotenv(env_path)

# Initialize Vertex AI
vertexai.init(
    project=os.getenv("GOOGLE_CLOUD_PROJECT"),
    location="europe-west1"
)

def get_catalog_from_database():
    """Obtiene el catálogo de obras desde la base de datos Navision."""
    import pyodbc
    
    # Configurar las variables de entorno
    def _env(k: str):
        v = os.environ.get(k)
        if v is None:
            raise RuntimeError(f"Falta variable de entorno: {k}")
        return v

    def get_connection():
        """Conexión mediante FreeTDS ODBC."""
        host = _env("NAVISION_DB_SERVICE_HOST")
        port = os.environ.get("NAVISION_DB_SERVICE_PORT", "1433")
        db   = _env("NAVISION_DB_NAME")
        uid  = _env("NAVISION_DB_USERNAME")
        pwd  = _env("NAVISION_DB_PASSWORD")
        tls  = os.environ.get("AYUDB_FREETDS_SSLPROTO", "tls1")
        
        conn_str = (
            "DRIVER={FreeTDS};"
            f"Server={host};Port={port};DATABASE={db};"
            f"UID={uid};PWD={pwd};"
            "TDS_Version=7.2;"
            "Encrypt=yes;"
            f"sslprotocol={tls};"
            "ClientCharset=UTF-8;"
        )
        
        return pyodbc.connect(conn_str, timeout=10)

    # Obtener catálogo de obras
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT 
                    No_,
                    Description,
                    [Job Posting Group],
                    Estado,
                    CONVERT(varchar, [Last Date Modified], 23) as Last_Date_Modified,
                    CONVERT(varchar, [Creation Date], 23) as Creation_Date
                FROM [obras ayu] 
                ORDER BY No_
            """)
            rows = cur.fetchall()
            
            # Formatear como líneas del catálogo
            catalog_lines = []
            for row in rows:
                line = f"No_={row[0]} | Desc={row[1]} | Group={row[2]} | Estado={row[3]} | Mod={row[4]} | Creacion={row[5]}"
                catalog_lines.append(line)
            
            return "\n".join(catalog_lines)

def create_work_search_system():
    """Crea el sistema de búsqueda de obras."""
    
    # Inicializar modelo Gemini
    llm = ChatVertexAI(
        model="gemini-2.5-flash-lite",
        max_output_tokens=5000,
        temperature=0.1  # Baja temperatura para respuestas más consistentes
    )
    
    # Template del prompt del sistema
    system_prompt = """[SYSTEM]
Eres un buscador de obras de construcción. 
Tu único objetivo es identificar la obra correcta dentro de un catálogo de obras que se presenta en texto plano.

Cada obra del catálogo aparece en una línea con el formato:
No_=<ID numérico> | Desc=<descripción> | Group=<grupo> | Estado=<estado numérico> | Mod=<última fecha modificación> | Creacion=<fecha creación>

Reglas de interpretación:
- "No_" es el identificador único de la obra. **Siempre debes devolver este número como respuesta principal.**
- La columna "Desc" contiene una pequeña descripción textual del proyecto. Es la señal más importante para encontrar coincidencias.
- La columna "Group" aporta contexto (tipo de obra, ej. O CIVIL, EDIF RES, UTESUPLIDO, etc.).
- Las columnas "Mod" y "Creacion" indican la fecha de última modificación y creación de la obra.
- Si hay coincidencia clara, devuelve SOLO:
  No_: <ID>
- Si no hay coincidencia clara o hay varias posibles, devuelve un bloque:
  Candidatos:
  1) No_=<ID numérico> | Desc=<descripción> | Group=<grupo> | Estado=<estado numérico> | Mod=<última fecha modificación> | Creacion=<fecha creación>
  2) No_=<ID numérico> | Desc=<descripción> | Group=<grupo> | Estado=<estado numérico> | Mod=<última fecha modificación> | Creacion=<fecha creación>
  3) No_=<ID numérico> | Desc=<descripción> | Group=<grupo> | Estado=<estado numérico> | Mod=<última fecha modificación> | Creacion=<fecha creación>

No inventes obras. Usa exclusivamente las que aparecen en el catálogo.

[CATÁLOGO]
{catalog}

[USER]
Quiero la obra: "{user_query}" """

    # Crear template de prompt
    prompt_template = PromptTemplate(
        input_variables=["catalog", "user_query"],
        template=system_prompt
    )
    
    return llm, prompt_template

def search_work(user_query):
    """Busca una obra en el catálogo."""
    
    print("🔄 Cargando catálogo de obras...")
    catalog = get_catalog_from_database()
    print(f"✅ Catálogo cargado: {len(catalog.split(chr(10)))} obras")
    
    print("🤖 Inicializando sistema de búsqueda...")
    llm, prompt_template = create_work_search_system()
    
    # Crear prompt completo
    full_prompt = prompt_template.format(
        catalog=catalog,
        user_query=user_query
    )
    
    print(f"🔍 Buscando: '{user_query}'")
    
    # Ejecutar búsqueda
    response = llm.invoke(full_prompt)
    
    return response.content

if __name__ == "__main__":
    print("🏗️ Sistema de Búsqueda de Obras de Construcción")
    print("=" * 50)
    
    while True:
        query = input("\n💭 Describe la obra que buscas: ").strip()
        
        if query.lower() in ['exit', 'quit', 'salir']:
            print("\n👋 ¡Hasta luego!")
            break
            
        if not query:
            print("❌ Por favor, ingresa una descripción de la obra")
            continue
            
        try:
            result = search_work(query)
            print(f"\n🎯 Resultado:\n{result}")
            
        except Exception as e:
            print(f"\n❌ Error al buscar la obra: {str(e)}")
            print("💡 Verifica la conexión a la base de datos y las variables de entorno")
