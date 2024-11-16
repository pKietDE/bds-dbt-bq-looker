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
        self.driver = driver
        self.client_mongo = MongoDBClient()
        self.page = page
        config = ConfigReader()
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
            # Lưu vào file JSON

            try:
                json_success = self.save_to_json(list_data,self.PATH)
                if json_success:
                    logging.info(f"Đã xử lý và lưu trữ dữ liệu thành công vào file json {len(list_data)} ban ghi")
            except Exception as e :
                logging.warning(f"loi :{e}")
                
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

    def save_to_json(self, data, base_path):
        """
        Lưu dữ liệu vào file JSON mà không làm hỏng định dạng
        
        Args:
            data: Dữ liệu cần lưu (list các documents)
            base_path: Đường dẫn thư mục lưu file
            
        Returns:
            bool: True nếu lưu thành công, False nếu có lỗi
        """
        try:
            # Đường dẫn file JSON
            file_name = os.path.join(base_path, "data_bds.json")
            
            # Kiểm tra file JSON hiện có
            existing_data = []
            if os.path.exists(file_name):
                try:
                    with open(file_name, 'r', encoding='utf-8') as file:
                        content = file.read()
                        if content:  # Kiểm tra file không rỗng
                            existing_data = json.loads(content)
                except json.JSONDecodeError:
                    logging.warning(f"File {file_name} không đúng định dạng JSON. Tạo file mới.")
            
            # Chuyển đổi ObjectId trong dữ liệu mới
            new_data = json.loads(json_util.dumps(data))
            
            # Kết hợp dữ liệu
            combined_data = existing_data + new_data
            
            # Ghi lại danh sách hợp lệ vào file
            with open(file_name, 'w', encoding='utf-8') as file:
                json.dump(combined_data, file, ensure_ascii=False, indent=4)
            
            logging.info(f"Đã lưu dữ liệu thành công vào file: {file_name}")
            return True
        
        except Exception as e:
            logging.error(f"Lỗi khi lưu file JSON: {str(e)}")
            return False