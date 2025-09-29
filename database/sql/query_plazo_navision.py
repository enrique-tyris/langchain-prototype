import os
import argparse
from datetime import date, datetime
from dotenv import load_dotenv
import pyodbc

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

# --- helpers de fechas ---
def _is_placeholder(d) -> bool:
    """True si es NULL o 1753-01-01 (placeholder típico de SQL Server)."""
    if d is None:
        return True
    if isinstance(d, datetime):
        return d.year == 1753 and d.month == 1 and d.day == 1
    if isinstance(d, date):
        return d.year == 1753 and d.month == 1 and d.day == 1
    # Si viene como string:
    s = str(d)[:10]
    return s == "1753-01-01"

def _fmt_date(d) -> str:
    """yyyy-mm-dd o 'NA' si placeholder/NULL."""
    return "NA" if _is_placeholder(d) else (d.date().isoformat() if isinstance(d, datetime) else (d.isoformat() if isinstance(d, date) else str(d)[:10]))

def _to_date(d):
    """Devuelve objeto date o None si placeholder/NULL."""
    if _is_placeholder(d):
        return None
    if isinstance(d, datetime):
        return d.date()
    if isinstance(d, date):
        return d
    # intento best-effort
    try:
        return datetime.strptime(str(d)[:10], "%Y-%m-%d").date()
    except Exception:
        return None

def _days_between(a, b):
    if a is None or b is None:
        return None
    return (b - a).days

def get_fechas_plazo(codigo_obra: str):
    """Devuelve las fechas relevantes del proyecto."""
    query = """
    SELECT
        o.[Fecha acta recep_ definitiva] AS recepcion,
        o.[Fecha adjudicación]           AS adjudicacion,
        o.[Fecha firma contrato]         AS firma_contrato,
        o.[Fecha acta de replanteo]      AS replanteo,
        o.[Fecha Fin Vigente]            AS fin_contrato
    FROM [obras ayu] o
    WHERE o.[No_] = ?
    """
    with get_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(query, (codigo_obra,))
            row = cur.fetchone()
            if not row:
                return dict(recepcion=None, adjudicacion=None, firma_contrato=None, replanteo=None, fin_contrato=None)
            recepcion, adjudicacion, firma_contrato, replanteo, fin_contrato = row
            return {
                "recepcion": recepcion,
                "adjudicacion": adjudicacion,
                "firma_contrato": firma_contrato,
                "replanteo": replanteo,
                "fin_contrato": fin_contrato,
            }

def main():
    parser = argparse.ArgumentParser(description="PLAZO: Fechas clave y duración total del proyecto")
    parser.add_argument('--id', type=str, default='880', help='Código de obra (No_) por defecto: 880')
    args = parser.parse_args()

    data = get_fechas_plazo(args.id)

    # Regla para calcular plazo total (usando fechas válidas únicamente)
    inicio_candidato = data.get("replanteo") or data.get("firma_contrato") or data.get("adjudicacion")
    fin_candidato    = data.get("recepcion") or data.get("fin_contrato")

    inicio = _to_date(inicio_candidato)
    fin    = _to_date(fin_candidato)
    dias   = _days_between(inicio, fin)

    print("🕒 PLAZO DEL PROYECTO")
    print("=" * 60)
    print(f"🏗️ Obra (No_): {args.id}")
    print(f"📅 Adjudicación:   {_fmt_date(data.get('adjudicacion'))}")
    print(f"📅 Firma contrato: {_fmt_date(data.get('firma_contrato'))}")
    print(f"📅 Replanteo:      {_fmt_date(data.get('replanteo'))}")
    print(f"📅 Fin contrato:   {_fmt_date(data.get('fin_contrato'))}")
    print(f"📅 Recepción:      {_fmt_date(data.get('recepcion'))}")

    # Plazo total (si no hay placeholders/NULL en inicio/fin)
    print("-" * 60)
    if dias is not None:
        meses_aprox = dias / 30.44
        print(f"⏳ Plazo total: {dias} días (~{meses_aprox:.1f} meses)")
        print(f"   Inicio (usado): {inicio.isoformat()}")
        print(f"   Fin (usado):    {fin.isoformat()}")
    else:
        motivo = []
        if inicio is None:
            motivo.append("inicio inválido (NULL/placeholder)")
        if fin is None:
            motivo.append("fin inválido (NULL/placeholder)")
        print("⛔ No es posible calcular el plazo total: " + ", ".join(motivo))

if __name__ == "__main__":
    main()
