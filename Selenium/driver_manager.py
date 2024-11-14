from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support import expected_conditions as EC
from fake_useragent import UserAgent
import logging


# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DriverManager:
    
    @staticmethod
    def setup_driver():
        # Tạo đối tượng UserAgent để tạo User-Agent ngẫu nhiên
        ua = UserAgent()

        # Cấu hình các tùy chọn cho trình duyệt Chrome
        options = Options()
        options.add_argument(f'user-agent={ua.random}')  # Thêm user-agent ngẫu nhiên
        options.add_argument('--disable-web-security')  # Tắt bảo mật web
        options.add_argument('--disable-features=IsolateOrigins,site-per-process')  # Tắt cô lập các nguồn gốc
        options.add_argument('--disable-gpu')  # Tắt GPU
        options.add_argument('--no-sandbox')  # Tắt sandboxing (phải cho các hệ thống Linux)
        options.add_argument('--disable-dev-shm-usage')  # Tắt sử dụng shared memory trong Linux
        options.add_argument('--disable-blink-features=AutomationControlled')  # Tắt các tính năng nhận diện tự động hóa
        options.add_argument('--disable-notifications')  # Tắt các thông báo từ trình duyệt
        options.add_argument('--disable-site-isolation-trials')

        
        try:
            
            # Khởi tạo driver với các options trên và service
            driver = webdriver.Chrome(options=options)
            return driver
        except Exception as e:
            print(f"Lỗi khởi tạo driver: {e}")
            return None
