from dataclasses import dataclass
from typing import Dict, List

@dataclass(frozen=True)
class QuerySpec:
    sql_path: str
    required_params: List[str]
    param_order: List[str]

REGISTRY: Dict[str, QuerySpec] = {
    "contactos_obra_por_codigo": QuerySpec(
        sql_path="database/sql/contactos_obra_por_codigo.sql",
        required_params=["obra_code"],
        param_order=["obra_code"],
    ),
    "cronograma_hitos_por_codigo": QuerySpec(
        sql_path="database/sql/cronograma_hitos_por_codigo.sql",
        required_params=["obra_code"],
        param_order=["obra_code"],
    ),
}
