import os
import argparse
from pathlib import Path
from google.cloud import vision
from google.cloud import storage
from google.api_core import exceptions
from dotenv import load_dotenv

def get_output_prefix(input_path: str, output_folder: str) -> str:
    """
    Genera un prefijo de salida coherente con el nombre del archivo de entrada.
    
    Args:
        input_path (str): Ruta del archivo PDF de entrada
        output_folder (str): Carpeta base para los resultados
        
    Returns:
        str: Prefijo para los archivos JSON de salida
    """
    # Obtener el nombre base del archivo sin extensión
    base_name = Path(input_path).stem
    return f"{output_folder}/{base_name}/ocr_"

def process_pdf_with_vision(
    bucket_name: str,
    source_blob_name: str,
    destination_blob_prefix: str,
    timeout: int = 1800
) -> bool:
    """
    Procesa un archivo PDF usando Google Cloud Vision API.
    
    Args:
        bucket_name (str): Nombre del bucket de Google Cloud Storage
        source_blob_name (str): Ruta del archivo PDF en el bucket
        destination_blob_prefix (str): Prefijo para los archivos de salida
        timeout (int): Tiempo máximo de espera en segundos (default 1800)
    
    Returns:
        bool: True si el proceso fue exitoso, False en caso contrario
    """
    try:
        # Inicializar el cliente de Vision
        client = vision.ImageAnnotatorClient()

        # Construir las URIs
        gcs_source_uri = f"gs://{bucket_name}/{source_blob_name}"
        gcs_destination_uri = f"gs://{bucket_name}/{destination_blob_prefix}"

        # Configurar el tipo de archivo y características
        mime_type = "application/pdf"
        feature = vision.Feature(
            type_=vision.Feature.Type.DOCUMENT_TEXT_DETECTION
        )

        # Configurar el input
        input_config = vision.InputConfig(
            gcs_source=vision.GcsSource(uri=gcs_source_uri),
            mime_type=mime_type
        )

        # Configurar el output
        output_config = vision.OutputConfig(
            gcs_destination=vision.GcsDestination(uri=gcs_destination_uri),
            batch_size=10  # Número de páginas por archivo JSON
        )

        # Crear la solicitud asíncrona
        request = vision.AsyncAnnotateFileRequest(
            features=[feature],
            input_config=input_config,
            output_config=output_config
        )

        print(f"Iniciando procesamiento OCR del archivo: {source_blob_name}")
        print(f"Los resultados se guardarán en: {gcs_destination_uri}")

        # Ejecutar la operación asíncrona
        operation = client.async_batch_annotate_files(requests=[request])

        # Esperar y verificar el resultado
        print("Procesando... esto puede tomar varios minutos...")
        result = operation.result(timeout=timeout)

        # Verificar el estado de la operación
        if not result.responses:
            print("Error: No se recibió respuesta del servicio")
            return False
            
        # Verificar si hay errores en la respuesta
        for response in result.responses:
            if hasattr(response, 'error') and response.error:
                print(f"Error en el procesamiento: {response.error}")
                return False

        print("¡Procesamiento OCR completado exitosamente!")
        print(f"Los resultados están disponibles en: {gcs_destination_uri}")
        return True

    except exceptions.PermissionDenied:
        print("Error: No tienes permisos suficientes. Verifica tus credenciales de Google Cloud.")
        return False
    except exceptions.InvalidArgument as e:
        print(f"Error: Argumentos inválidos - {str(e)}")
        return False
    except exceptions.DeadlineExceeded:
        print(f"Error: Se superó el tiempo límite de {timeout} segundos.")
        return False
    except Exception as e:
        print(f"Error inesperado: {str(e)}")
        return False

def list_pdf_files(bucket_name: str, input_folder: str) -> list:
    """
    Lista todos los archivos PDF en el bucket y carpeta especificados.
    """
    print('hola')
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    print('adios')
    return [blob.name for blob in bucket.list_blobs(prefix=input_folder) 
            if blob.name.lower().endswith('.pdf')]

def parse_args() -> argparse.Namespace:
    """Configura y parsea los argumentos de línea de comandos."""
    parser = argparse.ArgumentParser(description='Procesa PDFs usando Google Cloud Vision OCR.')
    
    parser.add_argument(
        '--contrato',
        type=str,
        help='Nombre específico del contrato a procesar (sin extensión)'
    )
    
    parser.add_argument(
        '--all',
        action='store_true',
        help='Procesar todos los PDFs en la carpeta de entrada'
    )
    
    return parser.parse_args()

def main():
    try:
        # Configurar las variables de entorno
        project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        env_path = os.path.join(project_root, '.env')
            
        load_dotenv(env_path)
        
        # Parsear argumentos
        args = parse_args()

        # Obtener y verificar variables de entorno
        bucket_name = os.getenv('GCS_BUCKET_NAME')
        input_folder = os.getenv('GCS_PDF_FOLDER')
        output_folder = os.getenv('GCS_OUTPUT_FOLDER')
        
        if not bucket_name:
            print("\nError: GCS_BUCKET_NAME no está definido en el archivo .env")
            return
        if not input_folder:
            print("\nError: GCS_PDF_FOLDER no está definido en el archivo .env")
            return
        if not output_folder:
            print("\nError: GCS_OUTPUT_FOLDER no está definido en el archivo .env")
            return
        
        # Determinar qué archivos procesar
        if args.all:
            # Procesar todos los PDFs
            pdfs_to_process = list_pdf_files(bucket_name, input_folder)
            if not pdfs_to_process:
                print(f"No se encontraron archivos PDF en gs://{bucket_name}/{input_folder}")
                return
        elif args.contrato:
            # Procesar solo el contrato especificado
            pdf_name = f"{args.contrato}.pdf" if not args.contrato.endswith('.pdf') else args.contrato
            pdfs_to_process = [f"{input_folder}/{pdf_name}"]
        else:
            print("Error: Debes especificar --contrato NOMBRE o --all")
            return

        # Procesar cada PDF
        for pdf_path in pdfs_to_process:
            print(f"\nProcesando: {pdf_path}")
            
            # Generar prefijo de salida coherente con el nombre del archivo
            destination_prefix = get_output_prefix(pdf_path, output_folder)
            
            print(f"Procesando archivo desde gs://{bucket_name}/{pdf_path}")
            print(f"Los resultados se guardarán en gs://{bucket_name}/{destination_prefix}")
            
            success = process_pdf_with_vision(
                bucket_name=bucket_name,
                source_blob_name=pdf_path,
                destination_blob_prefix=destination_prefix
            )
            
            if not success:
                print(f"Error al procesar {pdf_path}")
            else:
                print(f"Procesamiento exitoso de {pdf_path}")

    except Exception as e:
        print(f"Error en la ejecución: {str(e)}")

if __name__ == "__main__":
    main()
