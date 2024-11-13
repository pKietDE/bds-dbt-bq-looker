from pymongo import MongoClient
from typing import Union, List, Dict
import datetime

class MongoDBClient:
    def __init__(self, host="mongodb://localhost:27017/"):
        self.client = MongoClient(host)
        current_date = datetime.datetime.now().strftime("%Y-%m-%d")
        self.db = self.client["batdongsan"]
        self.collection = self.db[f"bds_{current_date}"]


    def insert_one(self, doc: Dict) -> None:
        """
        Insert một document vào MongoDB collection.
        :param doc: Dictionary để insert
        """
        try:
            result = self.collection.insert_one(doc)
            print(f"Đã insert thành công document với id: {result.inserted_id}")
        except Exception as e:
            print(f"Lỗi khi insert_one vào MongoDB: {str(e)}")

    def insert_many(self, docs: List[Dict]) -> None:
        """ 
        Insert nhiều documents vào MongoDB collection.
        :param docs: List các dictionary để insert
        """
        try:
            if docs:  # Kiểm tra list không rỗng
                result = self.collection.insert_many(docs)
                print(f"Đã insert thành công {len(result.inserted_ids)} documents")
            else:
                print("Không có dữ liệu để insert")
        except Exception as e:
            print(f"Lỗi khi insert_many vào MongoDB: {str(e)}")

    def quit(self):
        """Đóng kết nối MongoDB."""
        try:
            if self.client:
                self.client.close()
                self.client = None
                print("Đã đóng kết nối MongoDB")
        except Exception as e:
            print(f"Lỗi khi đóng kết nối MongoDB: {str(e)}")

