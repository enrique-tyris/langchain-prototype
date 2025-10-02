# database/executor.py
from typing import Dict, Any, List
from pathlib import Path
from database.sql.navision_connector import get_connection
from database.sql.registry import REGISTRY

def _read_sql(path: str) -> str:
    sql = Path(path).read_text(encoding="utf-8")
    return sql

def _build_positional_args(intent: Dict[str, Any], param_order: List[str]) -> tuple:
    # Convierte dict → tupla en el orden exacto de los "?"
    return tuple(intent[p] for p in param_order)

def execute_query(intent: Dict[str, Any]) -> Dict[str, Any]:
    """
    intent = {"query_key": "...", "<param>": ...}
    """
    qk = intent["query_key"]
    spec = REGISTRY[qk]

    # Validación de requeridos
    missing = [p for p in spec.required_params if p not in intent or intent[p] is None]
    if missing:
        raise ValueError(f"Faltan parámetros requeridos {missing} para '{qk}'")

    sql = _read_sql(spec.sql_path)
    args = _build_positional_args(intent, spec.param_order)

    with get_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, args)
        cols = [c[0] for c in cur.description] if cur.description else []
        rows = [dict(zip(cols, r)) for r in cur.fetchall()]
        # Limpieza: quitar campos None (tu UI no los quiere mostrar)
        rows = [{k: v for k, v in row.items() if v is not None} for row in rows]

    return {"query_key": qk, "rowcount": len(rows), "rows": rows}
