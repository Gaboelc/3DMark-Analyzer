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

        return df_app_info, df_hardware_info

    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None, None