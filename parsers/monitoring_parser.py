import pandas as pd
import os

def extract_monitoring_data(file_path):
    try:
        data = pd.read_csv(file_path, delimiter=';')
        return data
    except Exception as e:
        print(f"Error al extraer datos del archivo Monitoring.csv: {e}")
        return None

def transform_monitoring_data(data, threshold=0.3):
    column_validity = data.notnull().mean() > threshold
    valid_columns = data.columns[column_validity]
    
    if len(valid_columns) == 0:
        print("Advertencia: No se encontraron columnas con datos significativos.")
        return pd.DataFrame()

    data_cleaned = data[valid_columns].dropna()

    data_cleaned = data_cleaned.apply(pd.to_numeric, errors='coerce').fillna(0)

    data_cleaned.columns = [col.replace("/", "_").replace(":", "") for col in data_cleaned.columns]
    
    return data_cleaned

def monitoring_etl(file_path, output_file, threshold=0.3):
    
    data = extract_monitoring_data(file_path)
    
    if data is not None:
        transformed_data = transform_monitoring_data(data, threshold=threshold)
        if transformed_data.empty:
            print(f"Warning: File {file_path} does not have columns with enough data.")
        else:
            transformed_data.to_csv(output_file, index=False)
            print(f"Transformed file saved in {output_file}")
            return transformed_data
    else:
        print(f"The file {file_path} could not be processed correctly.")
        return None

def separate_and_order_columns(df):
    common_columns = ['BM_RunTimeSecondsE', 'BM_FramesPerSecondE']
    gpu_columns = [col for col in df.columns if 'GPU' in col]
    cpu_columns = [col for col in df.columns if 'CPU' in col]

    common_columns = [col for col in common_columns if col in df.columns]

    df_gpu = df[common_columns + gpu_columns]
    df_cpu = df[common_columns + cpu_columns]

    cpu_columns_only = [col for col in df_cpu.columns if col not in common_columns]

    temp_columns = sorted([col for col in cpu_columns_only if 'Temperature' in col], key=lambda x: int(x.split('_')[-1]))
    freq_columns = sorted([col for col in cpu_columns_only if 'ClockFrequency' in col], key=lambda x: int(x.split('_')[-1]))

    ordered_cpu_columns = [val for pair in zip(temp_columns, freq_columns) for val in pair]
    ordered_cpu_columns = common_columns + ordered_cpu_columns

    df_cpu = df_cpu[ordered_cpu_columns]
    
    return df_gpu, df_cpu

def validate_cpu_data(df_cpu):
    missing_values = df_cpu.isnull().sum()

    invalid_temperature = df_cpu[[col for col in df_cpu.columns if 'Temperature' in col]].apply(lambda x: (x < 0) | (x > 100)).sum()

    invalid_frequency = df_cpu[[col for col in df_cpu.columns if 'ClockFrequency' in col]].apply(lambda x: (x < 0) | (x > 5000)).sum()

    duplicate_rows = df_cpu.duplicated().sum()

    data_types = df_cpu.dtypes

    validation_results_gpu = pd.DataFrame({
        'Missing values': missing_values,
        'Invalid temperature': invalid_temperature,
        'Invalid Frequency': invalid_frequency,
        'Duplicate Rows': [duplicate_rows] * len(missing_values),
        'Data Types': data_types
    })
    
    return validation_results_gpu

def validate_gpu_data(df_gpu):
    missing_values = df_gpu.isnull().sum()

    data_types = df_gpu.dtypes

    invalid_temperature = df_gpu[[col for col in df_gpu.columns if 'Temperature' in col]].apply(lambda x: (x < 0) | (x > 100)).sum()
    invalid_frequency = df_gpu[[col for col in df_gpu.columns if 'ClockFrequency' in col]].apply(lambda x: (x < 0) | (x > 5000)).sum()

    duplicate_rows = df_gpu.duplicated().sum()

    validation_results_gpu = pd.DataFrame({
        'Missing Values': missing_values,
        'Invalid Temperature': invalid_temperature,
        'Invalid Frequency': invalid_frequency,
        'Duplicate Rows': [duplicate_rows] * len(missing_values),
        'Data Types': data_types
    })

    return validation_results_gpu