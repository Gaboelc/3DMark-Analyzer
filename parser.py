import zipfile
import os
import shutil
import tempfile

class ZipParser:
    def __init__(self, zip_path):
        self.zip_path = zip_path
        self.temp_dir = None

    def extract_to_temp(self):
        self.temp_dir = tempfile.mkdtemp()
        with zipfile.ZipFile(self.zip_path, 'r') as zip_ref:
            zip_ref.extractall(self.temp_dir)
        return self.temp_dir

    def clean_up(self):
        if self.temp_dir and os.path.exists(self.temp_dir):
            shutil.rmtree(self.temp_dir)
            self.temp_dir = None
