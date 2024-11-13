import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import json
from driver_manager import DriverManager
from detail_handler import DetailHandler
from server_mongodb import *
from cloud import *
from google.cloud import storage
from google.oauth2 import service_account

# Cấu hình logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


def process_single_page(page_url,page):
    """Process a single page and extract data"""
    try:
        driver_manager = DriverManager()
        driver = driver_manager.setup_driver()

        driver.get(page_url)
        time.sleep(6)
        logging.info(f"Crawl Page : {page_url} ")

        detailHandler = DetailHandler(driver,page)
        detailHandler.handle_detail()
        time.sleep(3)

        driver.quit()

    except Exception as e:
        logging.error(f"Error processing page {page_url}: {e}")
        return []

def main():
    PATH = "/home/kiet/Downloads/key.json"
    credentials = service_account.Credentials.from_service_account_file(PATH)
    storage_client = storage.Client(credentials=credentials)
    gcs = GCStorage(storage_client)
    base_url = "https://batdongsan.com.vn/nha-dat-ban"

    with ThreadPoolExecutor(max_workers= 15) as executor:
        
        for page in range(1, 25):
            page_url = f"{base_url}/p{page}" if page > 1 else base_url
            logging.info(f"Submitting page {page} to executor: {page_url}")
            executor.submit(process_single_page, page_url,page)


    # Sau khi tất cả các thread hoàn thành, upload file lên GCS
    current_date = datetime.datetime.now().strftime("%Y-%m-%d")
    file_name = f"/home/kiet/Documents/vsCode/Selenium/Data/data_bds_{current_date}.json"
    #gcs.upload_files_to_gcs(file_name)


if __name__ == "__main__":
    main()
