from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
from urllib.parse import urlparse
from Time_Convert import *
from server_mongodb import MongoDBClient
import time
from selenium.common.exceptions import *

class DataExtractor:
    @staticmethod
    def get_property_category(href):
        try:
            parse_url = urlparse(href)
            path = parse_url.path.split("/")[1]
            return path.replace("-"," ")
        except Exception as e:
            logging.warning(f"Error extracting property category: {str(e)}")
            return ""
    
    @staticmethod
    def get_data_from_page(driver,page):
        """Lấy tất cả dữ liệu của các bất động sản từ một trang"""
        all_properties = []
        invalid_count = 0
        retry_limit = 3  # Giới hạn số lần retry
        retry_count = 0  # Đếm số lần retry

        try:

            while retry_count < retry_limit:
                try:
                    # lấy danh sách property cards
                    property_cards = WebDriverWait(driver, 40).until(
                        EC.presence_of_all_elements_located((
                            By.CSS_SELECTOR, 
                            f"div[pgno='{page}']"
                        ))
                    )

                    if not property_cards:
                        logging.warning(f"Không tìm thấy property cards trên trang {page}. Thử lại lần {retry_count + 1}")
                        retry_count += 1
                        time.sleep(5)  # Chờ một chút trước khi thử lại
                        continue  # Quay lại vòng lặp và thử lại

                    logging.info(f"Tìm thấy {len(property_cards)} property cards.")
                    break  # Nếu tìm thấy thì thoát khỏi vòng lặp retry

                except Exception as e:
                    logging.error(f"Lỗi khi tìm kiếm property cards: {str(e)}")
                    retry_count += 1
                    time.sleep(5)  # Chờ một chút trước khi thử lại
                    continue  # Thử lại

            if retry_count == retry_limit:
                logging.warning(f"Đã thử {retry_limit} lần nhưng không thể tìm thấy property cards. Dừng lại.")
                return all_properties  # Nếu hết số lần retry, dừng lại và trả về dữ liệu đã có


            for card in property_cards:
                try:
                    data = {}
                    # Lấy địa chỉ
                    try:
                        address_element = card.find_element(By.CSS_SELECTOR, "[class*='js__card-title']")
                        data['address'] = address_element.text.strip()
                        logging.info(f"địa chỉ: {data['address']}")

                    except TimeoutException:
                        logging.warning(f"Không thể lấy địa chỉ: {driver.current_url}")
                        data['address'] = ""

                            

                    # Lấy quận/thành phố
                    try:
                        location_element = card.find_element(By.CSS_SELECTOR, "div.re__card-location")
                        location_parts = location_element.text.split(",")
                        data['district'] = location_parts[0].replace(".", "").replace("·", "").strip() if len(location_parts) > 0 else ""
                        data['city'] = location_parts[1].strip() if len(location_parts) > 1 else ""
                    except Exception as e:
                        logging.warning(f"Không thể lấy quận/thành phố")
                        data['district'] = ""
                        data['city'] = ""

                    # Lấy giá
                    try:
                        try:
                            price_element = card.find_element(By.CSS_SELECTOR, "span.re__card-config-price.js__card-config-item")
                            data['price'] = price_element.text.strip()
                            logging.info(f"Price : {data['price']}")
                        except:
                            logging.warning(f"Lỗi khi lấy giá ")
                            data['price'] = "Giá thỏa thuận"


                        try:
                            price_m2_element = card.find_element(By.CSS_SELECTOR, "span.re__card-config-price_per_m2.js__card-config-item")
                            # Lấy giá trị của price_m2 và loại bỏ các ký tự không mong muốn (dấu chấm, newline)
                            price_m2_text = price_m2_element.text.replace("·", "").replace("\n", "").strip()
                            price_m2_parts = price_m2_text.split(" ")
                            data['price_m2'] = (price_m2_parts[0] + " " + price_m2_parts[1]).replace(".", "")
                            logging.info(f"Price_m2 : {data['price_m2']}")
                        except Exception as e:
                            logging.warning(f"Lỗi khi lấy price_m2 ")
                            data['price_m2'] = ""

                    except Exception as e:
                        logging.warning(f"Lỗi khi lấy giá và m2 ")

                    # Lấy diện tích
                    try:
                        area_element = card.find_element(By.CSS_SELECTOR, "span.re__card-config-area.js__card-config-item")
                        data['area'] = area_element.text.strip()
                    except Exception as e:
                        logging.warning(f"Không thể lấy diện tích: {driver.current_url}")
                        data['area'] = ""

                    # Lấy thời gian cập nhật
                    try:
                        update_element = card.find_element(By.CSS_SELECTOR, "span.re__card-published-info-published-at")

                        if update_element:
                            data['update_time'] = str(TimeConverter.convert_update_time(update_element.text.strip()))
                            logging.info(f"update_time (span): {data['update_time']}")
                        else:
                            logging.warning(f"Không tìm thấy update_time (span)")

                    except Exception as e:
                        # Nếu không tìm thấy, tìm phần tử thứ hai (div)
                        try:
                            update_element = card.find_element(By.CSS_SELECTOR, "div.re__card-published-info div.card-user-info--date-time")
                            data['update_time'] = str(TimeConverter.convert_update_time(update_element.text.strip()))
                            logging.info(f"Update time (div): {data['update_time']}")
                        except Exception as e:
                            logging.error("Không tìm thấy phần tử update time.")
                            data['update_time'] = ""

                    # Lấy link và category trong cùng một lần tìm kiếm
                    try:
                        product_element = card.find_element(By.CSS_SELECTOR, "a.js__product-link-for-product-id")
                        href = product_element.get_attribute('href')
                        
                        if href:
                            data['link'] = href
                            data['category_home'] = DataExtractor.get_property_category(href)
                            logging.info(f"Đã lấy được link (a.js__product) : {href}")
                        else:
                            raise ValueError("Href attribute is empty")
                            
                    except:
                        try:
                            href_element = card.find_element(By.CSS_SELECTOR, "a.re__unreport")
                            href = href_element.get_attribute('href')
                            data['link'] = href
                            data['category_home'] = DataExtractor.get_property_category(href)
                            logging.info(f"Đã lấy được link (re__unreport): {href}")
                        except:
                            logging.error("Không tìm thấy phần tử  link.")
                            data['link'] = ""
                            data['category_home'] = ""

                    all_properties.append(data)
                        

                    # Kiểm tra và xử lý dữ liệu
                    # if data['update_time']:
                    #     time_Convert = TimeConverter()
                    #     result = time_Convert.convert_update_time(update_time=data['update_time'])

                    #     if isinstance(result, int) and result <= 7:
                    #         # Kiểm tra dữ liệu có đầy đủ các trường quan trọng
                    #             all_properties.append(data)
                    #             invalid_count = 0
                               
                    #     else:
                    #         invalid_count += 1
                    #         logging.info(f"Dữ liệu không hợp lệ, số lần: {invalid_count}")

                    #     if invalid_count >= 80:
                    #         logging.warning(f"Đã đạt giới hạn không có dữ liệu hợp lệ trong {invalid_count} lần liên tiếp")
                    #         return all_properties

                except Exception as e:
                    logging.error(f"Lỗi khi xử lý card: {driver.current_url}")
                    continue

        except Exception as e:
            logging.error(f"Lỗi khi lấy danh sách cards: {str(e)}")
            return []

        logging.info(f"Hoàn thành xử lý, tổng số bản ghi thành công: {len(all_properties)}")
        return all_properties