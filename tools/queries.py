# tools/queries.py
from pydantic import BaseModel, Field
from langchain_core.tools import tool

class ObraCodeArgs(BaseModel):
    obra_code: str = Field(..., description="Código de obra (OBRA_CODE)")

@tool("contactos_obra_por_codigo", args_schema=ObraCodeArgs)
def t_contactos_obra_por_codigo(obra_code: str):
    """
    Devuelve contactos (cargo_id, nombre, movil) para una obra.
    """
    return {"query_key": "contactos_obra_por_codigo", "obra_code": obra_code.strip()}

@tool("cronograma_hitos_por_codigo", args_schema=ObraCodeArgs)
def t_cronograma_hitos_por_codigo(obra_code: str):
    """
    Devuelve fechas clave de la obra (recepción, adjudicación, firma contrato, replanteo, fin contrato).
    """
    return {"query_key": "cronograma_hitos_por_codigo", "obra_code": obra_code.strip()}

TOOLS = [t_contactos_obra_por_codigo, t_cronograma_hitos_por_codigo]