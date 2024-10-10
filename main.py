import os
from parsers.parser import ZipParser
from parsers.monitoring_parser import separate_and_order_columns, validate_cpu_data, validate_gpu_data, monitoring_etl
from parsers.results_parser import extract_results_data
from parsers.arielle_parser import extract_arielle_data
import pandas as pd

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
            result_file = os.path.join(temp_dir, 'Result.xml')
            arielle_file = os.path.join(temp_dir, 'Arielle.xml')
            
            if os.path.exists(monitoring_file):
                output_file = os.path.join(temp_dir, 'Monitoring_cleaned.csv')
                df_monitoring = monitoring_etl(monitoring_file, output_file, threshold=0.3)
                
                if df_monitoring is not None:
                    df_gpu, df_cpu = separate_and_order_columns(df_monitoring)

                    cpu_validation_results = validate_cpu_data(df_cpu)
                    print("\n CPU validation results:")
                    print(cpu_validation_results)
                    
                    gpu_validation_results = validate_gpu_data(df_gpu)
                    print("\n GPU validation results:")
                    print(gpu_validation_results)

                    print("\n CPU Dataframe:")
                    print(df_cpu)
                    
                    print("\n GPU Dataframe:")
                    print(df_gpu)
                    
                else:
                    print(f"\n Warning: Could not process file {filename}.")
            else:
                print(f"\n The file Monitoring.csv was not found in {filename}")
            
            if os.path.exists(result_file):
                df_results = extract_results_data(result_file) 
                
                if df_results is not None:
                    results_target = df_results[df_results.columns[df_results.columns.str.contains('ForPass')]].dropna(axis=1, how='all').dropna(how='all')
                    
                    results = df_results[df_results.columns[~df_results.columns.str.contains("ForPass")]].dropna(axis=1, how='all').dropna(how='all')
                    
                    if 'benchmarkRunId' in results_target.columns:
                        results_target = results_target.drop(columns=['benchmarkRunId'])
                    if 'passIndex' in results_target.columns:
                        results_target = results_target.drop(columns=['passIndex'])
                        
                    if 'benchmarkRunId' in results.columns:
                        results = results.drop(columns=['benchmarkRunId'])
                    if 'passIndex' in results.columns:
                        results = results.drop(columns=['passIndex'])
                        results.iloc[0] = results.iloc[0].combine_first(results.iloc[1])
                        results = results.drop(index=1).reset_index(drop=True)
                    
                    print("\n Results Dataframe:")
                    print(results)
                    
                    print("\n Results_target Dataframe:")
                    print(results_target)
                    
                else:
                    print(f"\n Warning: Could not process file {filename}.")
            else:
                print(f"\n The file Result.xml was not found in {filename}")
                
            if os.path.exists(arielle_file): 
                
                df_app_info, df_hardware_info = extract_arielle_data(arielle_file)
                
                if df_app_info is not None and not df_app_info.empty:
                    print("\n Application Information:")
                    print(df_app_info)
                else:
                    print(f"\n Warning: Could not extract application info from {arielle_file}.")
                
                if df_hardware_info is not None and not df_hardware_info.empty:
                    print("\n Hardware information:")
                    print(df_hardware_info)
                else:
                    print(f"\n Warning: Could not extract hardware info from {arielle_file}.")
                    
            else:
                print(f"\n The file Arielle.xml was not found in {filename}")
                
            if zip(df_gpu, df_cpu, results_target, results) is not None:
                print(f"\n File {filename} processed successfully.")
            else:
                print(f"\n Warning: Could not process file {filename}.")
            
            parser.clean_up()

if __name__ == "__main__":
    raw_directory = "./data/raw/"
    process_3dmark_files(raw_directory)
