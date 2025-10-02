import os
import warnings
from typing import Dict, Any
from dotenv import load_dotenv
import vertexai
from langchain_google_vertexai import ChatVertexAI
from langchain_core.messages import AIMessage

from tools.queries import TOOLS
from database.sql.executor import execute_query

# Suprimir warnings específicos
warnings.filterwarnings("ignore", category=UserWarning, module="vertexai._model_garden._model_garden_models")
warnings.filterwarnings("ignore", category=UserWarning, module="langsmith.client")

SYSTEM = """Eres un asistente que mapea lenguaje natural a UNA sola herramienta.
Elige exactamente UNA tool y pasa sus argumentos correctos. No inventes valores.
Si el usuario menciona un código de obra, úsalo como 'obra_code'."""

llm = ChatVertexAI(
    model=os.getenv("CHAT_MODEL"),
    max_output_tokens=1000,
    temperature=0
).bind_tools(TOOLS)

def run_nl_to_sql(nl_text: str) -> Dict[str, Any]:
    ai: AIMessage = llm.invoke([("system", SYSTEM), ("user", nl_text)])

    # Debug opcional
    print(f"Debug - Respuesta del modelo: {ai.content}")
    print(f"Debug - Tool calls: {ai.tool_calls}")

    if not ai.tool_calls:
        raise ValueError("No se llamó a ninguna herramienta. Revisa el prompt o añade few-shot.")

    call = ai.tool_calls[0]
    tool_name = call["name"]        # <-- aquí está el nombre de la tool
    args = call["args"] or {}       # {'obra_code': '855', ...}

    # Normalización ligera del parámetro (opcional)
    if "obra_code" in args and isinstance(args["obra_code"], str):
        args["obra_code"] = args["obra_code"].strip()

    # Construimos el intent que espera el executor
    intent = {"query_key": tool_name, **args}
    print(f"Debug - Intent: {intent}")

    return execute_query(intent)    # usa navision por defecto (qmark)
