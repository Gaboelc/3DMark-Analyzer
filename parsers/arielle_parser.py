import xml.etree.ElementTree as ET
import pandas as pd

def extract_arielle_data(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()

        app_info_data = []
        for info in root.find('application_info').findall('info'):
            app_info_data.append({
                'name': info.find('name').text,
                'value': info.find('value').text
            })
        df_app_info = pd.DataFrame(app_info_data)

        hardware_data = []
        for setting in root.find('settings').findall('setting'):
            hardware_data.append({
                'name': setting.find('name').text,
                'value': setting.find('value').text
            })
        df_hardware_info = pd.DataFrame(hardware_data)

        test_info_data = []
        test_info_root = root.find('test_info')
        if test_info_root is not None:
            benchmark_tests = test_info_root.find('benchmark_tests')
            if benchmark_tests is not None:
                for benchmark in benchmark_tests.findall('benchmark_test'):
                    test_info_data.append({
                        'type': 'benchmark_test',
                        'name': benchmark.get('name'),
                        'test_run_type': benchmark.get('test_run_type'),
                        'version': benchmark.get('version')
                    })

            workload_sets = test_info_root.find('workload_sets')
            if workload_sets is not None:
                for workload in workload_sets.findall('workload_set'):
                    test_info_data.append({
                        'type': 'workload_set',
                        'name': workload.get('name')
                    })

        df_test_info_full = pd.DataFrame(test_info_data)

        df_workload_sets = df_test_info_full[df_test_info_full['type'] == 'workload_set'].reset_index(drop=True)
        df_test_info = df_test_info_full[df_test_info_full['type'] != 'workload_set'].reset_index(drop=True)
        
        df_workload_sets = df_workload_sets.drop(columns=[col for col in ['test_run_type', 'version'] if col in df_workload_sets.columns], errors='ignore')

        return df_app_info, df_hardware_info, df_test_info, df_workload_sets

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None, None, None, None