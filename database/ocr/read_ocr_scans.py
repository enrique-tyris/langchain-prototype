import os
import json
import argparse
from typing import Dict, List, Optional, Tuple
from google.cloud import storage
from dotenv import load_dotenv

def load_env_vars() -> Dict[str, str]:
    """Carga las variables de entorno necesarias."""
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    env_path = os.path.join(project_root, '.env')
    load_dotenv(env_path)
    
    required_vars = {
        'GCS_BUCKET_NAME': os.getenv('GCS_BUCKET_NAME'),
        'GCS_OUTPUT_FOLDER': os.getenv('GCS_OUTPUT_FOLDER')
    }
    
    missing_vars = [var for var, value in required_vars.items() if not value]
    if missing_vars:
        raise ValueError(f"Faltan las siguientes variables de entorno: {', '.join(missing_vars)}")
    
    return required_vars

def download_json_from_gcs(bucket_name: str, json_path: str) -> Dict:
    """Descarga y lee un archivo JSON desde Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(json_path)
    
    json_content = blob.download_as_string()
    return json.loads(json_content)

def extract_text_from_json(json_data: Dict, page_number: Optional[int] = None) -> str:
    """
    Extrae texto del JSON de salida de Vision.
    - Si page_number es None: concatena todas las páginas del JSON.
    - Si page_number está definido (0-based por CLI): busca el response con context.pageNumber == page_number+1.
    """
    try:
        responses = json_data.get("responses", [])
        if not responses:
            return ""

        # Si no piden una página concreta: concatenar todo el texto de este JSON
        if page_number is None:
            textos = []
            for resp in responses:
                fta = resp.get("fullTextAnnotation", {})
                if "text" in fta:
                    textos.append(fta["text"])
            return "\n".join(textos).strip()

        # Si piden página concreta (CLI 0-based -> Vision 1-based)
        target = page_number + 1
        for resp in responses:
            ctx = resp.get("context", {}) or {}
            if ctx.get("pageNumber") == target:
                fta = resp.get("fullTextAnnotation", {})
                return fta.get("text", "").strip()

        # No encontrada en este JSON
        return ""

    except Exception as e:
        print(f"Error al extraer texto del JSON: {str(e)}")
        return ""

def list_ocr_results(bucket_name: str, output_folder: str, document_name: Optional[str] = None) -> List[str]:
    """
    Lista todos los archivos JSON de resultados OCR en el bucket.
    
    Args:
        bucket_name (str): Nombre del bucket
        output_folder (str): Carpeta base de resultados
        document_name (str, optional): Nombre del documento específico a buscar
        
    Returns:
        List[str]: Lista de rutas a archivos JSON
    """
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    
    # Si se especifica un documento, buscar en su subcarpeta específica
    if document_name:
        prefix = f"{output_folder}/{document_name}/ocr_"
    else:
        prefix = output_folder
    
    # Listar todos los blobs en la carpeta de salida
    blobs = bucket.list_blobs(prefix=prefix)
    return [blob.name for blob in blobs if blob.name.endswith('.json')]

def read_ocr_results(
    bucket_name: str,
    output_folder: str,
    document_name: Optional[str] = None,
    page_number: Optional[int] = None
) -> Dict[str, str]:
    """
    Lee y procesa los resultados del OCR desde Google Cloud Storage.
    
    Args:
        bucket_name (str): Nombre del bucket
        output_folder (str): Carpeta donde están los resultados
        document_name (str, optional): Nombre específico del documento a buscar
        page_number (int, optional): Número específico de página a extraer
        
    Returns:
        Dict[str, str]: Diccionario con {nombre_archivo: texto_extraído}
    """
    results = {}
    try:
        json_files = list_ocr_results(bucket_name, output_folder, document_name)
        if not json_files:
            print(f"No se encontraron archivos JSON en {output_folder}")
            return results

        # Ordena por nombre para leer en orden (1-10, 11-20, ...)
        json_files = sorted(json_files)

        for json_file in json_files:
            try:
                json_data = download_json_from_gcs(bucket_name, json_file)
                extracted_text = extract_text_from_json(json_data, page_number)

                if extracted_text:
                    results[json_file] = extracted_text
                    # Si pediste una página concreta, ya la encontraste: corta aquí
                    if page_number is not None:
                        break
                else:
                    # Solo informa si buscamos página concreta y no estaba en este JSON
                    if page_number is not None:
                        print(f"Página {page_number} no está en {json_file}")

            except Exception as e:
                print(f"Error procesando {json_file}: {str(e)}")
                continue

        # Si buscabas una página y no apareció en ningún JSON:
        if page_number is not None and not results:
            print(f"Error: La página {page_number} no existe en este documento")
        
    except Exception as e:
        print(f"Error al leer resultados OCR: {str(e)}")
    
    return results

def parse_args() -> argparse.Namespace:
    """Configura y parsea los argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(description='Lee resultados de OCR de documentos procesados.')
    
    parser.add_argument(
        'contrato',
        type=str,
        help='Nombre del contrato a leer (ej: contrato-854)'
    )
    
    parser.add_argument(
        '--pagina',
        type=int,
        help='Número de página específica a leer (1-based)',
        default=None
    )
    
    return parser.parse_args()

def main():
    """Función principal para ejecutar el script."""
    try:
        # Parsear argumentos
        args = parse_args()
        
        # Cargar variables de entorno
        env_vars = load_env_vars()
        bucket_name = env_vars['GCS_BUCKET_NAME']
        output_folder = env_vars['GCS_OUTPUT_FOLDER']
        
        # Asegurarse de que el nombre del contrato no incluya la extensión
        document_name = args.contrato
        if document_name.endswith('.pdf'):
            document_name = document_name[:-4]
        
        # Convertir página de 1-based (CLI) a 0-based (interno)
        page_arg = args.pagina - 1 if args.pagina is not None else None
        
        # Leer resultados OCR
        results = read_ocr_results(
            bucket_name=bucket_name,
            output_folder=output_folder,
            document_name=document_name,
            page_number=page_arg
        )
        
        if not results:
            print("No se encontraron resultados OCR.")
            return
        
        # Mostrar resultados
        print("\nResultados encontrados:")
        
        if args.pagina is not None:
            # Si se pidió una página específica, mostrar la información del archivo
            for file_name, text in results.items():
                print(f"\n{'='*50}")
                print(f"Archivo: {file_name}")
                print(f"Contrato: {args.contrato}")
                print(f"Página: {args.pagina}")
                print(f"{'='*50}\n")
                print(text)
                print(f"\nLongitud del texto: {len(text)} caracteres")
        else:
            # Si se pidió todo el documento, concatenar el texto de todos los archivos en orden
            print(f"\nContrato: {args.contrato}")
            print(f"{'='*50}\n")
            
            total_length = 0
            for file_name in sorted(results.keys()):
                text = results[file_name]
                print(text)
                print("\n" + "-"*50 + "\n")  # Separador entre partes del documento
                total_length += len(text)
            
            print(f"\nLongitud total del texto: {total_length} caracteres")

    except Exception as e:
        print(f"Error en la ejecución: {str(e)}")

if __name__ == "__main__":
    main()
