import configparser 
import os

class ConfigReader:
    def __init__(self, config_file="config_airflow.conf"):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)

    def get_config(self, name_section, key):
        if name_section in self.config:
            value = self.config[name_section][key]
            if value:
                # Loại bỏ dấu `{}` khỏi chuỗi nhưng giữ lại `$`
                return value.replace("{", "").replace("}", "")
            else:
                print("Không lấy được giá trị")
        else:
            print("Không lấy được section")


