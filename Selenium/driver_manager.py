from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from fake_useragent import UserAgent

class DriverManager:
    @staticmethod
    def setup_driver():
        ua = UserAgent()
        options = Options()
        options.add_argument(f'user-agent={ua.random}')
        options.add_argument('--disable-web-security')
        #options.add_argument("--disable-gpu")
        #options.add_argument("--no-sandbox")
        #options.add_argument("--disable-dev-shm-usage")
        options.add_argument('--disable-blink-features=AutomationControlled')
        options.add_argument('--disable-notifications')

        # Chế độ headless mới
        #options.add_argument('--headless=new')
        #options.add_argument("window-size=1920,1080")
        
        try:
            driver = webdriver.Chrome(options=options)
            return driver
        except Exception as e:
            print(f"Lỗi khởi tạo driver: {e}")
            return None
