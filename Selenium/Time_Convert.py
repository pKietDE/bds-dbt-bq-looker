from datetime import datetime, timedelta
import re
import logging

class TimeConverter:
    @staticmethod
    def convert_update_time(update_time):
        try:
            # Kiểm tra trường hợp "Đăng hôm nay"
            if "Đăng hôm nay" in update_time:
                return datetime.now().date()
            
            # Kiểm tra trường hợp "Đăng hôm qua"
            elif "Đăng hôm qua" in update_time:
                return datetime.now().date() - timedelta(days=1)
            
            # Kiểm tra trường hợp "Đăng X ngày trước"
            match = re.match(r"Đăng (\d+)\s*ngày\s*trước", update_time)
            if match:
                day_count = int(match.group(1))
                return datetime.now().date() - timedelta(days=day_count)
            
            # Kiểm tra trường hợp ngày có định dạng "DD/MM/YYYY"
            pattern = r"Đăng (\b\d{2}/\d{2}/\d{4}\b)"
            match_gr_2 = re.search(pattern, update_time)
            if match_gr_2:
                date_str = match_gr_2.group(1)
                # Chuyển đổi từ định dạng "DD/MM/YYYY" sang datetime.date
                date_obj = datetime.strptime(date_str, "%d/%m/%Y").date()
                return date_obj

            # Nếu không khớp với bất kỳ mẫu nào
            return ""
        except Exception as e:
            logging.warning(f"Lỗi khi chuyển đổi thời gian: {str(e)}")
            return ""
    