import os
from parser import ZipParser
from monitoring_analysis import monitoring_etl

def process_3dmark_files(raw_directory):
    """Procesa todos los archivos .3dmark-result en el directorio raw"""
    for filename in os.listdir(raw_directory):
        if filename.endswith(".3dmark-result"):
            file_path = os.path.join(raw_directory, filename)
            print(f"Procesando archivo: {filename}")

            # Inicializa el parser y extrae el archivo
            parser = ZipParser(file_path)
            temp_dir = parser.extract_to_temp()

            # Analiza el archivo Monitoring.csv
            monitoring_file = os.path.join(temp_dir, 'Monitoring.csv')
            if os.path.exists(monitoring_file):
                print(f"Analizando {monitoring_file}")
                output_file = os.path.join(temp_dir, 'Monitoring_cleaned.csv')
                stats = monitoring_etl(monitoring_file, output_file, threshold=0.3)  # Establecer un umbral del 30%
                if stats is not None:
                    print("Estadísticas del archivo Monitoring.csv:")
                    print(stats)
            
            # Limpieza del directorio temporal
            parser.clean_up()

if __name__ == "__main__":
    # Ruta al directorio donde se encuentran los archivos .3dmark-result
    raw_directory = "./data/raw/"
    process_3dmark_files(raw_directory)
