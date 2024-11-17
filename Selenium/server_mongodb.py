from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError
from typing import List, Dict, Optional
import datetime
import logging
from ConfigParser import *

class MongoDBClient:
    def __init__(self, host="mongodb://localhost:27017/", timeout=5000):
        """
        Khởi tạo kết nối MongoDB với timeout và xử lý lỗi.
        
        Args:
            host (str): MongoDB connection string
            timeout (int): Thời gian timeout cho kết nối (ms)
        """
        try:
            config_parser = ConfigReader()
            DATABASE = str(config_parser.get_config("MONGO","DATABASE"))
            COLLECTION = str(config_parser.get_config("MONGO","COLLECTION"))

            self.client = MongoClient(host, serverSelectionTimeoutMS=timeout)
            # Kiểm tra kết nối
            self.client.admin.command('ping')
            
            current_date = datetime.datetime.now().strftime("%Y-%m-%d")
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
        self.quit()