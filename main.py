# main.py
from graph.chains.sql_retrieval_chain import run_nl_to_sql

if __name__ == "__main__":
    # Ejemplos
    print(">> Contactos por obra")
    res1 = run_nl_to_sql("Dame cargo, nombre y móvil de la obra 855")
    print(res1)

    print("\n>> Cronograma de hitos")
    res2 = run_nl_to_sql("Fechas clave (recepción, adjudicación, firma, replanteo, fin) de la obra 855")
    print(res2)

    print("\n>> Prueba negativa: cómo se hace una pizza")
    res3 = run_nl_to_sql("Dame la receta de una pizza")
    print(res3)
