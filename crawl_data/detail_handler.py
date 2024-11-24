from selenium.common.exceptions import WebDriverException
from data_extractor import DataExtractor
from server_mongodb import MongoDBClient
import logging
import datetime
import json
from cloud import *
from google.cloud import storage
import time
from bson import json_util  # Để hỗ trợ ObjectId
from server_mongodb import MongoDBClient
import os
from ConfigParser import *


class DetailHandler:
    def __init__(self, driver, page):
        config = ConfigReader()
        USER_NAME = config.get_config("MONGO","USER_NAME")
        PASSWORD = config.get_config("MONGO","PASSWORD")
        IP = config.get_config("MONGO","IP")

        self.driver = driver
        self.client_mongo = MongoDBClient(is_authen=False)
        self.page = page
        self.PATH = config.get_config("PATH","FOLDER_SAVE_DATA")

    def handle_detail(self):
        """Xử lý chi tiết dữ liệu từ trang web"""
        try:
            # Ghi log URL đang xử lý
            logging.info(f"Đang xử lý URL: {self.driver.current_url}")
            
            # Lấy dữ liệu từ trang web
            list_data = DataExtractor.get_data_from_page(self.driver,self.page)
            
            if not list_data:
                logging.warning(f"Không có dữ liệu được trích xuất từ trang: {self.driver.current_url}")
                return False
                
            logging.info(f"Đã trích xuất được {len(list_data)} bản ghi")

            # Luu du lieu vao mongo 
            try:
                self.client_mongo.insert_many(list_data)
                logging.info(f"Đã xử lý và lưu trữ dữ liệu thành công lên mongo {len(list_data)} ban ghi")
            except Exception as e :
                logging.warning(f"Loi :{e}")
            # Export data tu mongo
            self.client_mongo.export_data()

            # Tạm dừng 5 giây trước khi tiếp tục
            time.sleep(5)
            return True
            
        except WebDriverException as e:
            logging.error(f"Lỗi Selenium WebDriver: {str(e)}")
            return False
        except Exception as e:
            logging.error(f"Lỗi không mong đợi: {str(e)}")
            return False
        finally:
            # Đảm bảo đóng kết nối MongoDB trong mọi trường hợp
            if self.client_mongo:
                try:
                    self.client_mongo.quit()
                except Exception as e:
                    logging.error(f"Lỗi khi đóng kết nối MongoDB: {str(e)}")