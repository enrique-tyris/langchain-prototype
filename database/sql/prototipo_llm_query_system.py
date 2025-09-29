import os
import argparse
from dotenv import load_dotenv
import pyodbc
from typing import Dict, Any, Optional
import json

# Configurar las variables de entorno (.env tres niveles arriba)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
env_path = os.path.join(project_root, '.env')
load_dotenv(env_path)

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

# Definición de todas las queries disponibles
QUERIES = {
    "kpir": {
        "description": "Desviación Económica (Desv. K-PIR): diferencia entre costes reales y presupuestados",
        "keywords": ["kpir", "k-pir", "desviación", "económica", "costes", "presupuesto"],
        "query": """
        SELECT v.[K DE PIR] AS kpir
        FROM [VERSA] v
        WHERE v.[obra] = ?
        """,
        "format_result": lambda row: f"📊 K-PIR: {row[0]:,.2f}"
    },
    
    "precio": {
        "description": "Precio de la obra: presupuesto vigente con IVA (incluyendo desviaciones)",
        "keywords": ["precio", "presupuesto", "iva", "coste", "total", "vigente"],
        "query": """
        SELECT o.[Presupuesto Vigente+IVA] AS presupuesto
        FROM [obras ayu] o
        WHERE o.[No_] = ?
          AND o.[Job Posting Group] IN ('1:EDIF RES', '2:EDIF NOR', '4:O CIVIL')
        """,
        "format_result": lambda row: f"💰 Precio total (con desviaciones): {row[0]:,.2f} €"
    },
    
    "margenes": {
        "description": "Análisis de márgenes de la obra",
        "keywords": ["margen", "márgenes", "beneficio", "rentabilidad", "ganancia"],
        "query": """
        SELECT 
            o.[Presupuesto Vigente+IVA] AS presupuesto,
            o.[Coste Real] AS coste_real,
            (o.[Presupuesto Vigente+IVA] - o.[Coste Real]) AS margen_bruto,
            CASE 
                WHEN o.[Presupuesto Vigente+IVA] > 0 
                THEN ((o.[Presupuesto Vigente+IVA] - o.[Coste Real]) / o.[Presupuesto Vigente+IVA]) * 100
                ELSE 0 
            END AS margen_porcentaje
        FROM [obras ayu] o
        WHERE o.[No_] = ?
        """,
        "format_result": lambda row: f"💰 Presupuesto: {row[0]:,.2f} €\n💸 Coste Real: {row[1]:,.2f} €\n📈 Margen Bruto: {row[2]:,.2f} €\n📊 Margen %: {row[3]:,.2f}%"
    },
    
    "hitos": {
        "description": "Fechas importantes y hitos de la obra",
        "keywords": ["hitos", "fechas", "adjudicación", "firma", "replanteo", "recepción", "fin"],
        "query": """
        SELECT
            o.[Fecha adjudicación] as adjudicacion,
            o.[Fecha firma contrato] as firma_contrato,
            o.[Fecha acta de replanteo] as replanteo,
            o.[Fecha acta recep_ definitiva] as recepcion,
            o.[Fecha Fin Vigente] as fin_contrato
        FROM [obras ayu] o
        WHERE o.[No_] = ?
        """,
        "format_result": lambda row: f"📅 Adjudicación: {row[0]}\n📝 Firma Contrato: {row[1]}\n🔨 Replanteo: {row[2]}\n✅ Recepción: {row[3]}\n🏁 Fin Contrato: {row[4]}"
    },
    
    "certificaciones": {
        "description": "Certificaciones de obra y pagos",
        "keywords": ["certificación", "certificaciones", "pago", "pagos", "facturación"],
        "query": """
        SELECT 
            COUNT(*) as total_certificaciones,
            SUM([Importe]) as importe_total_certificado,
            MAX([Fecha]) as ultima_certificacion
        FROM [Certificaciones Obra] 
        WHERE [Nº proyecto] = ?
        """,
        "format_result": lambda row: f"📋 Total Certificaciones: {row[0]}\n💰 Importe Total Certificado: {row[1]:,.2f} €\n📅 Última Certificación: {row[2]}"
    },
    
    "s_curve": {
        "description": "Curva S de progreso de la obra",
        "keywords": ["curva", "s", "progreso", "avance", "porcentaje", "completado"],
        "query": """
        SELECT 
            [Porcentaje Completado] as porcentaje_completado,
            [Fecha] as fecha_progreso,
            [Observaciones] as observaciones
        FROM [Progreso Obra] 
        WHERE [Nº proyecto] = ?
        ORDER BY [Fecha] DESC
        """,
        "format_result": lambda rows: "\n".join([f"📊 {row[0]:.1f}% - {row[1]} - {row[2] or 'Sin observaciones'}" for row in rows])
    },
    
    "plazo": {
        "description": "Información sobre plazos y duración de la obra",
        "keywords": ["plazo", "duración", "tiempo", "días", "meses", "vencimiento"],
        "query": """
        SELECT 
            o.[Plazo inicial_AYU] as plazo_inicial,
            o.[Fecha Fin Vigente] as fecha_fin,
            DATEDIFF(day, GETDATE(), o.[Fecha Fin Vigente]) as dias_restantes,
            CASE 
                WHEN DATEDIFF(day, GETDATE(), o.[Fecha Fin Vigente]) < 0 THEN 'VENCIDO'
                WHEN DATEDIFF(day, GETDATE(), o.[Fecha Fin Vigente]) < 30 THEN 'CRÍTICO'
                ELSE 'NORMAL'
            END as estado_plazo
        FROM [obras ayu] o
        WHERE o.[No_] = ?
        """,
        "format_result": lambda row: f"⏱️ Plazo Inicial: {row[0]} días\n📅 Fecha Fin: {row[1]}\n⏰ Días Restantes: {row[2]}\n🚨 Estado: {row[3]}"
    }
}

def select_query_by_intent(user_question: str) -> Optional[str]:
    """
    Selecciona la query más apropiada basándose en la pregunta del usuario.
    """
    question_lower = user_question.lower()
    
    # Buscar coincidencias por keywords
    best_match = None
    best_score = 0
    
    for query_id, query_info in QUERIES.items():
        score = 0
        for keyword in query_info["keywords"]:
            if keyword in question_lower:
                score += 1
        
        if score > best_score:
            best_score = score
            best_match = query_id
    
    return best_match if best_score > 0 else None

def execute_query(query_id: str, obra_id: str) -> Any:
    """
    Ejecuta la query seleccionada con el ID de obra proporcionado.
    """
    if query_id not in QUERIES:
        raise ValueError(f"Query '{query_id}' no encontrada")
    
    query_info = QUERIES[query_id]
    query = query_info["query"]
    
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (obra_id,))
            
            if query_id in ["s_curve"]:
                # Para queries que devuelven múltiples filas
                rows = cur.fetchall()
                return query_info["format_result"](rows)
            else:
                # Para queries que devuelven una sola fila
                row = cur.fetchone()
                if row:
                    return query_info["format_result"](row)
                else:
                    return "❌ No se encontraron datos para esta obra"

def main():
    parser = argparse.ArgumentParser(
        description='Sistema LLM para consultas de obras en Navision'
    )
    parser.add_argument('--pregunta', type=str, required=True, 
                       help='Pregunta sobre la obra (ej: "¿Cuál es el precio de la obra?")')
    parser.add_argument('--id', type=str, required=True, 
                       help='Código de obra')
    parser.add_argument('--list-queries', action='store_true', 
                       help='Listar todas las queries disponibles')
    
    args = parser.parse_args()
    
    if args.list_queries:
        print("🔍 QUERIES DISPONIBLES:")
        print("=" * 60)
        for query_id, query_info in QUERIES.items():
            print(f"\n📋 {query_id.upper()}:")
            print(f"   {query_info['description']}")
            print(f"   Keywords: {', '.join(query_info['keywords'])}")
        return
    
    # Seleccionar query basándose en la pregunta
    selected_query = select_query_by_intent(args.pregunta)
    
    if not selected_query:
        print("❌ No se pudo determinar qué información buscar.")
        print("💡 Usa --list-queries para ver las opciones disponibles.")
        return
    
    print(f"🔍 Pregunta: {args.pregunta}")
    print(f"🏗️ Obra: {args.id}")
    print(f"📋 Query seleccionada: {selected_query.upper()}")
    print("=" * 60)
    
    try:
        result = execute_query(selected_query, args.id)
        print(result)
    except Exception as e:
        print(f"❌ Error ejecutando la query: {str(e)}")

if __name__ == "__main__":
    main()