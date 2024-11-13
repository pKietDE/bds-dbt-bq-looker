import re

class TimeConverter:
    @staticmethod
    def convert_update_time(update_time):
        # Dùng regex để kiểm tra chuỗi
        match = re.match(r"Đăng (\d+)\s*ngày\s*trước", update_time)
        
        if match:
            # Lấy số ngày từ chuỗi
            day_count = int(match.group(1))
            if 1 <= day_count <= 6:  # Chỉ chấp nhận từ 1 đến 6 ngày
                return day_count + 1  # "Đăng X ngày trước" sẽ trả về X+1 (VD: "Đăng 2 ngày trước" trả về 3)
        
        # Các trường hợp đặc biệt khác
        if "Đăng hôm nay" in update_time:
            return 1
        elif "Đăng hôm qua" in update_time:
            return 2
        
        return None  # Trả về None nếu không khớp định dạng