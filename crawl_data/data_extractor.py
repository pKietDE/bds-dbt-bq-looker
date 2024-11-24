from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import logging
from urllib.parse import urlparse
from Time_Convert import *
from server_mongodb import MongoDBClient
import time
from selenium.common.exceptions import *
from selenium.common.exceptions import *
from bs4 import BeautifulSoup
from lxml import *

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
                    
                    soup = BeautifulSoup(driver.page_source, 'lxml')
                    # Tìm tất cả các phần tử <div> có thuộc tính pgno
                    property_cards = soup.find_all("div", attrs={"pgno": page})
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
                    try:
                        address_element = card.find("span", class_="js__card-title")
                        if address_element:
                            data['address'] = address_element.text.strip()
                            logging.info(f"Địa chỉ: {data['address']}")
                        else:
                            logging.warning(f"Không thể lấy địa chỉ.")
                            data['address'] = ""
                    except Exception as e:
                        logging.warning(f"Lỗi khi lấy địa chỉ: {str(e)}")
                        data['address'] = ""
                    # Lấy quận/thành phố
                    try:
                        location_element = card.find("div", class_="re__card-location")
                        location_parts = location_element.text.split(",") if location_element else []
                        data['district'] = location_parts[0].replace(".", "").replace("·", "").strip() if len(location_parts) > 0 else ""
                        data['city'] = location_parts[1].strip() if len(location_parts) > 1 else ""
                    except Exception as e:
                        logging.warning(f"Không thể lấy quận/thành phố.")
                        data['district'] = ""
                        data['city'] = ""
                    # Lấy giá
                    try:
                        price_element = card.find("span", class_="re__card-config-price")
                        data['price'] = price_element.text.strip() if price_element else "Giá thỏa thuận"
                        logging.info(f"Price: {data['price']}")
                    except Exception as e:
                        logging.warning(f"Lỗi khi lấy giá.")
                        data['price'] = "Giá thỏa thuận"
                    # Lấy diện tích
                    try:
                        area_element = card.find("span", class_="re__card-config-area")
                        data['area'] = area_element.text.strip() if area_element else ""
                    except Exception as e:
                        logging.warning(f"Không thể lấy diện tích.")
                        data['area'] = ""
                    # Lấy thời gian cập nhật
                    try:
                        # Tìm phần tử <span> với class="re__card-published-info-published-at"
                        try: 
                            update_element = card.find("span", class_="re__card-published-info-published-at")

                            # Lấy giá trị thuộc tính aria-label nếu tồn tại
                            data['update_time'] = TimeConverter.convert_update_time(update_element.attrs.get('aria-label', ''))
                            logging.info(f"Update time (span): {data['update_time']}")
                        except:
                            logging.warning("Không tìm thấy update_time (span)")
                            try:
                                # Nếu không tìm thấy, thử tìm trong phần tử <div> với các class liên quan
                                update_element = card.find("div", class_=["card-user-info--date-time"])

                                # Lấy giá trị thời gian từ <div>
                                data['update_time'] = TimeConverter(update_element.text.strip())
                                logging.info(f"Update time (div): {data['update_time']}")
                            except:
                                logging.warning("Không tìm thấy update_time (div)")
                                data['update_time'] = ""
                    except Exception as e:
                        logging.warning(f"Lỗi khi lấy thời gian cập nhật: {str(e)}")
                        data['update_time'] = ""
                    # Lấy link và category
                    try:
                        # Cố gắng tìm thẻ <a> với class cụ thể
                        product_element = card.find("a", class_="js__product-link-for-product-id")
                        if product_element:
                            href = product_element.get("href")
                            if href:
                                data['link'] = href
                                data['category_home'] = DataExtractor.get_property_category(href)
                                logging.info(f"Đã lấy được link: {href}")
                                logging.info(f"Đã lấy được category_home: {data['category_home']}")
                            else:
                                logging.warning("Không tìm thấy thuộc tính href trong thẻ a.js__product-link-for-product-id.")
                        else:
                            logging.warning("Không tìm thấy thẻ a với class 'js__product-link-for-product-id'. Tiến hành thử thẻ <a> mặc định.")

                            # Nếu không tìm thấy, thử với thẻ <a> mặc định
                            product_element = card.find("a")
                            if product_element:
                                href = product_element.get("href")
                                if href:
                                    data['link'] = href
                                    data['category_home'] = DataExtractor.get_property_category(href)
                                    logging.info(f"Đã lấy được link: {href}")
                                    logging.info(f"Đã lấy được category_home: {data['category_home']}")
                                else:
                                    logging.warning("Không tìm thấy thuộc tính href trong thẻ a.")
                            else:
                                logging.error("Không tìm thấy thẻ <a> mặc định.")
                                data['link'] = ""
                                data['category_home'] = ""
                    except Exception as e:
                        logging.error(f"Đã gặp lỗi khi lấy link: {e}")
                        data['link'] = ""
                        data['category_home'] = ""

                    
                    try:
                        # Kiểm tra và xử lý dữ liệu
                        if  data['update_time']:
                            
                            if "Đăng 1 tuần trước" or "Đăng hôm nay" not in data['update_time']:
                                # Kiểm tra dữ liệu có đầy đủ các trường quan trọng
                                all_properties.append(data)
                                invalid_count = 0  # Đặt lại invalid_count nếu dữ liệu hợp lệ
                            else:
                                invalid_count += 1
                                logging.info(f"Dữ liệu không hợp lệ, số lần: {invalid_count}")
                            
                            # Nếu đạt giới hạn không hợp lệ liên tiếp, dừng vòng lặp
                            if invalid_count >= 20:
                                logging.warning(f"Đã đạt giới hạn không có dữ liệu hợp lệ trong {invalid_count} lần liên tiếp")
                                return all_properties
                    except:
                        logging.error(f"Lỗi khi xử lý convert thời gian")
                        
                        
                except Exception as e:
                    logging.error(f"Lỗi khi xử lý card: {driver.current_url}")
                    continue
        except Exception as e:
            logging.error(f"Lỗi khi lấy danh sách cards: {str(e)}")
            return []
        logging.info(f"Hoàn thành xử lý, tổng số bản ghi thành công: {len(all_properties)}")
        return all_properties