import os
import time
from dotenv import load_dotenv
import vertexai
from langchain_google_vertexai import ChatVertexAI

# ⏱️ Medir tiempo de carga de variables
t0 = time.time()
load_dotenv()
t1 = time.time()
print(f"[LOG] Tiempo en cargar variables .env: {t1 - t0:.2f} s")

# ⏱️ Inicializar Vertex AI y el modelo
t2 = time.time()
vertexai.init(project=os.getenv("GOOGLE_CLOUD_PROJECT"), location="europe-west1")
chat = ChatVertexAI(
    model="gemini-2.5-flash-lite",
    max_output_tokens=200)
t3 = time.time()
print(f"[LOG] Tiempo en inicializar Vertex AI + modelo: {t3 - t2:.2f} s")

# 🔹 Llamada dummy para calentar (no medimos cold start aparte)
print("[LOG] Ejecutando llamada dummy para evitar cold start...")
td0 = time.time()
_ = chat.invoke("Hola, esta es una llamada de prueba. Ignórame.")
td1 = time.time()
print(f"[LOG] Tiempo en dummy call: {td1 - td0:.2f} s\n")

# 🔹 Inferencia real después de dummy
t4 = time.time()
output = chat.invoke("Probando conexión real con Vertex AI después de dummy call")
t5 = time.time()
print(f"[LOG] Tiempo en inferencia (post-dummy): {t5 - t4:.2f} s")

# Mostrar solo el texto limpio
print("\n")
print("==============================")
print("==============================")
print("\n=== Respuesta del modelo ===")
print(output.content)