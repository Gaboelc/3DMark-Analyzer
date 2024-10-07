import os
from parser import ZipParser
from monitoring_analysis import monitoring_etl

def process_3dmark_files(raw_directory):
    for filename in os.listdir(raw_directory):
        if filename.endswith(".3dmark-result"):
            file_path = os.path.join(raw_directory, filename)
            print(f"Procesando archivo: {filename}")

            parser = ZipParser(file_path)
            temp_dir = parser.extract_to_temp()

            monitoring_file = os.path.join(temp_dir, 'Monitoring.csv')
            if os.path.exists(monitoring_file):
                print(f"Analizando {monitoring_file}")
                output_file = os.path.join(temp_dir, 'Monitoring_cleaned.csv')
                stats = monitoring_etl(monitoring_file, output_file, threshold=0.3)
                if stats is not None:
                    print("Estadísticas del archivo Monitoring.csv:")
                    print(stats)

            parser.clean_up()

if __name__ == "__main__":
    raw_directory = "./data/raw/"
    process_3dmark_files(raw_directory)
