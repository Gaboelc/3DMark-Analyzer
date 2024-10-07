import os
from parser import ZipParser
from monitoring_analysis import separate_and_order_columns, validate_cpu_data, validate_gpu_data, monitoring_etl

def process_3dmark_files(raw_directory):

    for filename in os.listdir(raw_directory):
        if filename.endswith(".3dmark-result"):
            file_path = os.path.join(raw_directory, filename)
            print(f"""Processing file: 
                  {filename}
                  """)

            parser = ZipParser(file_path)
            temp_dir = parser.extract_to_temp()

            monitoring_file = os.path.join(temp_dir, 'Monitoring.csv')
            if os.path.exists(monitoring_file):
                output_file = os.path.join(temp_dir, 'Monitoring_cleaned.csv')
                df = monitoring_etl(monitoring_file, output_file, threshold=0.3)
                
                if df is not None:
                    df_gpu, df_cpu = separate_and_order_columns(df)

                    cpu_validation_results = validate_cpu_data(df_cpu)
                    print("\n CPU validation results:")
                    print(cpu_validation_results)
                    
                    gpu_validation_results = validate_gpu_data(df_gpu)
                    print("\n CPU validation results:")
                    print(gpu_validation_results)

                    print("\n CPU Dataframe:")
                    print(df_cpu)
                    
                    print("\n GPU Dataframe:")
                    print(df_gpu)
            
            parser.clean_up()

if __name__ == "__main__":
    raw_directory = "./data/raw/"
    process_3dmark_files(raw_directory)
