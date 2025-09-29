import os
import argparse
from datetime import date, datetime
from dotenv import load_dotenv
import pyodbc
import matplotlib.pyplot as plt

# Configurar las variables de entorno (.env tres niveles arriba)
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
env_path = os.path.join(project_root, '.env')
load_dotenv(env_path)

# --- helpers mínimos ---
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

def get_produccion_diaria(job_id: str):
    """
    Devuelve lista de tuplas (fecha, importe_diario) para el proyecto.
    """
    query = """
    SELECT
        dp.[Fecha] AS t,
        SUM(dp.[Importe]) AS y
    FROM [detalles produccion] dp
    WHERE
        dp.[Nº Proyecto] = ? AND
        (dp.[Tipo] = 0 OR dp.[Tipo] = 1)
    GROUP BY dp.[Fecha]
    ORDER BY dp.[Fecha]
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (job_id,))
            rows = cur.fetchall()
            return [(r[0], float(r[1])) for r in rows]

def _to_date(x):
    if isinstance(x, datetime):
        return x.date()
    if isinstance(x, date):
        return x
    return datetime.strptime(str(x)[:10], "%Y-%m-%d").date()

def build_curva_s(series):
    """
    Recibe [(fecha, importe_diario)] y devuelve:
    fechas[], acumulado[], acumulado_pct[]
    """
    fechas = []
    acumulado = []
    total = 0.0
    for (t, y) in series:
        d = _to_date(t)
        total += y
        fechas.append(d)
        acumulado.append(total)
    if total == 0:
        pct = [0.0 for _ in acumulado]
    else:
        pct = [v / total * 100.0 for v in acumulado]
    return fechas, acumulado, pct, total

def plot_curva_s(fechas, acumulado, pct, titulo):
    """
    Muestra el gráfico de Curva S (acumulado) y % acumulado (2º eje).
    """
    fig, ax1 = plt.subplots(figsize=(10, 5))
    ax1.plot(fechas, acumulado, linewidth=2, label="Acumulado (€)")
    ax1.set_xlabel("Fecha")
    ax1.set_ylabel("Acumulado (€)")
    ax1.tick_params(axis='x', rotation=45)

    ax2 = ax1.twinx()
    ax2.plot(fechas, pct, linestyle="--", linewidth=1.5, label="% acumulado", color="orange")
    ax2.set_ylabel("% acumulado")

    fig.suptitle(titulo)
    fig.tight_layout()
    plt.show()

def main():
    parser = argparse.ArgumentParser(
        description="Curva S de Producción: avance económico acumulado (Tipos 0 y 1)."
    )
    parser.add_argument('--id', type=str, default='880', help='Nº Proyecto (por defecto: 880)')
    args = parser.parse_args()

    series = get_produccion_diaria(args.id)
    fechas, acumulado, pct, total = build_curva_s(series)

    print("📈 CURVA S DE PRODUCCIÓN (ECONÓMICA)")
    print("=" * 60)
    print(f"🏗️ Proyecto: {args.id}")
    print(f"💰 Total producido: {total:,.2f} €")
    if fechas:
        print(f"📅 Última fecha con producción: {fechas[-1].isoformat()}")

    if fechas:
        plot_curva_s(fechas, acumulado, pct, f"Curva S de Producción · Proyecto {args.id}")
    else:
        print("⚠️ No hay datos de producción para este proyecto.")

if __name__ == "__main__":
    main()