import configparser 
import os

class ConfigReader:

    def __init__(self,config_file = "config.conf"):
        self.config = configparser.ConfigParser()
        self.config.read(config_file)   

    def get_config(self,name_section, key):
        if name_section in self.config:
            value = self.config[name_section][key].strip("${}")
            if value:
                return os.getenv(value)
            else :
                print("Không lấy được giá trị")
        else:
            print("Không lấy được section ")
