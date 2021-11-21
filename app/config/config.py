import json
import os
from pathlib import Path


class ConfigUtils:
    def __init__(self):
        # BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_root = Path(__file__).resolve().parent
        with open(os.path.join(config_root, 'app_config.json')) as secrets_file:
            self.secrets = json.load(secrets_file)

    def get_data(self, setting_key):
        """Get s≤ecret setting or fail with ImproperlyConfigured"""
        try:
            return self.secrets[setting_key]
        except KeyError:
            print("Set the {} setting".format(setting_key))
