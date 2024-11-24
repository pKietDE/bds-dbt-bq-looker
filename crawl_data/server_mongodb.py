from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError
from typing import List, Dict, Optional
import datetime
import logging
<<<<<<< HEAD
=======
import json
>>>>>>> cb91e62a99809a1dd85fc85ea296fa2c02d98410
from ConfigParser import *


class MongoDBClient:
<<<<<<< HEAD
    def __init__(self,name,password,ip,is_authen = False,host="mongodb://localhost:27017/", timeout=5000):
=======
    def __init__(self,name = None,password = None,ip = None,is_authen = False,host="mongodb://localhost:27017/", timeout=5000):
>>>>>>> cb91e62a99809a1dd85fc85ea296fa2c02d98410
        """
        Khởi tạo kết nối MongoDB với timeout và xử lý lỗi.
        
        Args:
            name (str): Tên đăng nhập MongoDB
            password (str): Mật khẩu đăng nhập MongoDB
            ip (str): Địa chỉ IP của MongoDB
            is_authen (bool): Cờ xác thực
            host (str): MongoDB connection string
            timeout (int): Thời gian timeout cho kết nối (ms)
        """
        try:
            # Tạo chuỗi kết nối MongoDB
            if not is_authen:
                # Kết nối không xác thực
                self.client = MongoClient(host, serverSelectionTimeoutMS=timeout)
            else:
                if not (name and password and ip):
                    logging.warning("Thiếu params cho việc đăng nhập bằng authenticate")
                    raise ValueError("Thiếu thông tin đăng nhập")
                # Kết nối với thông tin xác thực
                self.client = MongoClient(f"mongodb://{name}:{password}@{ip}:27017/", serverSelectionTimeoutMS=timeout)

            # Kiểm tra kết nối
            self.client.admin.command('ping')
            config_reader = ConfigReader()
            DATABASE = str(config_reader.get_config("MONGO","DATABASE"))
            COLLECTION = str(config_reader.get_config("MONGO","COLLECTION"))
<<<<<<< HEAD
            
            current_date = datetime.datetime.now().strftime("%Y-%m-%d")
=======

            
            current_date = datetime.datetime.now().strftime("%Y-%m-%d")
            self.OUTPUT_BATH = str(config_reader.get_config("PATH","FOLDER_SAVE_DATA"))+f"data_bds_{current_date}.json"
>>>>>>> cb91e62a99809a1dd85fc85ea296fa2c02d98410
            self.db = self.client[DATABASE]
            self.collection = self.db[f"{COLLECTION}{current_date}"]
            logging.info("Kết nối MongoDB thành công")
            
        except ConnectionFailure as e:
            logging.error(f"Không thể kết nối đến MongoDB: {str(e)}")
            raise
        except Exception as e:
            logging.error(f"Lỗi không xác định khi kết nối MongoDB: {str(e)}")
            raise

    def insert_one(self, doc: Dict) -> Optional[str]:
        """
        Insert một document vào MongoDB collection.
        
        Args:
            doc (Dict): Dictionary để insert
            
        Returns:
            Optional[str]: ID của document đã insert hoặc None nếu thất bại
        """
        try:
            if not doc:
                logging.warning("Document trống, bỏ qua insert_one")
                return None

            result = self.collection.insert_one(doc)
            logging.info(f"Đã insert thành công document với id: {result.inserted_id}")
            return str(result.inserted_id)
            
        except OperationFailure as e:
            logging.error(f"Lỗi operation khi insert_one: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"Lỗi không xác định khi insert_one: {str(e)}")
            return None

    def insert_many(self, docs: List[Dict], ordered: bool = True) -> Optional[List[str]]:
        """
        Insert nhiều documents vào MongoDB collection.
        
        Args:
            docs (List[Dict]): List các dictionary để insert
            ordered (bool): Nếu True, dừng insert khi gặp lỗi. 
                          Nếu False, tiếp tục insert các documents còn lại
        
        Returns:
            Optional[List[str]]: List các ID đã insert hoặc None nếu thất bại
        """
        try:
            if not docs:
                logging.warning("List documents trống, bỏ qua insert_many")
                return None

            # Lọc bỏ các document trống nếu có
            valid_docs = [doc for doc in docs if doc]
            if len(valid_docs) != len(docs):
                logging.warning(f"Đã lọc bỏ {len(docs) - len(valid_docs)} documents trống")

            if not valid_docs:
                logging.warning("Không có document hợp lệ để insert")
                return None

            result = self.collection.insert_many(valid_docs, ordered=ordered)
            inserted_count = len(result.inserted_ids)
            logging.info(f"Đã insert thành công {inserted_count}/{len(docs)} documents")
            
            return [str(id) for id in result.inserted_ids]
            
        except OperationFailure as e:
            logging.error(f"Lỗi operation khi insert_many: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"Lỗi không xác định khi insert_many: {str(e)}")
            return None

<<<<<<< HEAD
=======
    def export_data(self) -> bool:
        """
        Xuất dữ liệu từ MongoDB collection ra một tệp JSON và loại bỏ trường _id.

        Args:
            output_file (str): Đường dẫn tệp JSON xuất ra.

        Returns:
            bool: Trả về True nếu xuất dữ liệu thành công, False nếu có lỗi.
        """
        try:
            # Lấy tất cả dữ liệu từ collection
            data_cursor = self.collection.find()

            # Loại bỏ trường '_id' trong mỗi document
            data = []
            for doc in data_cursor:
                doc.pop('_id', None)  # Loại bỏ trường '_id'
                data.append(doc)

            # Mở tệp và ghi dữ liệu
            with open(self.OUTPUT_BATH, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=4)
            
            logging.info(f"Dữ liệu đã được xuất thành công vào {self.OUTPUT_BATH}")
            return True
        except Exception as e:
            logging.error(f"Lỗi khi xuất dữ liệu: {str(e)}")
            return False


>>>>>>> cb91e62a99809a1dd85fc85ea296fa2c02d98410
    def quit(self) -> None:
        """
        Đóng kết nối MongoDB an toàn.
        """
        try:
            if hasattr(self, 'client') and self.client:
                self.client.close()
                self.client = None
                logging.info("Đã đóng kết nối MongoDB")
        except Exception as e:
            logging.error(f"Lỗi khi đóng kết nối MongoDB: {str(e)}")

    def __enter__(self):
        """Hỗ trợ using context manager"""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Đảm bảo đóng kết nối khi sử dụng with statement"""
<<<<<<< HEAD
        self.quit()
=======
        self.quit()

if __name__ == "__main__":
    client_mongo = MongoDBClient(is_authen=False)
    is_export = client_mongo.export_data()
    if is_export:
        print("export thanh cong")
    else:
        print("export that bai")
>>>>>>> cb91e62a99809a1dd85fc85ea296fa2c02d98410
