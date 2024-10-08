import xml.etree.ElementTree as ET
import pandas as pd

def extract_results_data(file_path):
    try:
        tree = ET.parse(file_path)
        root = tree.getroot()
        
        data = []
        for result in root.find('results').findall('result'):
            result_data = {}
            for element in result:
                result_data[element.tag] = element.text.strip() if element.text else None
            data.append(result_data)
            
            df = pd.DataFrame(data)

        return df
    
    except Exception as e:
        print(f"Error processing {file_path}: {e}")
        return None
