import os
import pandas as pd
from pathlib import Path
from query_margenes_navision import get_connection

# === LISTA DE PROYECTOS ===
# Cambiando a strings para evitar problemas de conversión de tipos
JOB_IDS = [
    "887", "880", "877", "869", "855", "850", "847", "844", "829", "821", "818", "813",
    "791", "790", "789", "772", "771", "756", "751", "733", "719", "708", "695", "677", "672"
]

# === QUERY DETALLE OBRA (corregida para manejar tipos de datos) ===
QUERY = """
SELECT 
    [Total Price (LCY)] as venta,
    [Total Cost Prev]   as coste,
    [Total Cost (LCY)]  as gasto,
    Actividad,
    [Posting Date]      as fecha,
    [Vendor No_]        as codigoproveedor,
    [Document No_]      as documento
FROM ayu.dbo.movproyecto m
WHERE CAST(m.[Job No_] AS VARCHAR(50)) = ? AND empresa = 1
"""

# === CARPETA DE SALIDA ===
OUTDIR = Path("data/detalle_obra")
OUTDIR.mkdir(exist_ok=True)

def main():
    with get_connection() as conn:
        for job in JOB_IDS:
            try:
                print(f"▶ Procesando obra {job}...")
                df = pd.read_sql(QUERY, conn, params=[job])

                # Persistir en disco
                outfile = OUTDIR / f"detalle_obra_{job}.csv"
                df.to_csv(outfile, index=False, encoding="utf-8-sig")
                print(f"   ✅ Guardado en {outfile} ({len(df)} filas)")
                
            except Exception as e:
                print(f"   ❌ Error procesando obra {job}: {str(e)}")
                continue

if __name__ == "__main__":
    main()
