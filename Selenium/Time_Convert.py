from datetime import datetime,timedelta
import re
class TimeConverter:
    @staticmethod
    def convert_update_time(update_time):
        try :

            # Kiểm tra trường hợp "Đăng hôm nay"
            if "Đăng hôm nay" in update_time:
                return datetime.now().date()
            
            # Kiểm tra trường hợp "Đăng hôm qua"
            elif "Đăng hôm qua" in update_time:
                return datetime.now().date() - timedelta(days=1)
            
            # Kiểm tra trường hợp "Đăng X ngày trước"
            match = re.match(r"Đăng (\d+)\s*ngày\s*trước", update_time)
            pattern = r"Đăng (\b\d{2}/\d{2}/\d{4}\b)"
            match_gr_2 = re.search(pattern,update_time)

            if match:
                day_count = int(match.group(1))
                return datetime.now().date() - timedelta(days=day_count)
            else:
                date_match = match_gr_2.group(1)
                if date_match:
                    return date_match
                else:
                    return None
        except:
            return update_time
        
    def calculate_date(update_time):
        try:
            date_conver = datetime.strptime(update_time, "%Y/%m/%d").date()
            date_now = datetime.now().date()  # Lấy ngày hiện tại dưới dạng đối tượng date
            q = (date_now - date_conver).days
            return int(q)
        except ValueError:
            return None  # Nếu không khớp định dạng thì trả về None