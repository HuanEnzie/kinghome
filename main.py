import os
import pandas as pd
import requests
import json

# --- CẤU HÌNH HỆ THỐNG ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Từ điển từ khóa thông minh - Càng nhiều từ khóa, máy càng khôn
KEYWORD_MAP = {
    # Bổ sung các biến thể "địa chỉ tư vấn", "mã tòa" từ bảng AirStay, TVT, My Home
    "address": ["địa chỉ", "địa chỉ nhà", "địa chỉ tư vấn", "vị trí", "tòa nhà", "nhà", "tên nhà", "tên tòa", "mã tòa", "mã phòng địa chỉ", "định vị"],
    
    # Bổ sung tách riêng "quận" (vì nhiều bảng như AirStay, Taco Land để riêng cột Quận)
    "district": ["quận", "khu vực", "quận/huyện"],

    # Bổ sung "trục phòng", "ds phòng", "số phòng/tầng" từ bảng BHome, My Home, TingTong
    "room_num": ["phòng", "số p", "mã p", "tên phòng", "số phòng", "trục", "trục phòng", "tên p", "mã phòng", "ds phòng", "phòng trống", "tên phòng trống", "số phòng/tầng", "tên phòng ảnh"],
    
    # [MỚI] Các sheet thường dùng Loại, Dạng, Kiểu
    "room_type": ["loại phòng", "kiểu phòng", "dạng phòng", "loại", "loại p"],
    
    # Bổ sung "giá 6-9-12 tháng", "giá sau điều chỉnh" (TingTong), "giá cho thuê" (My Home)
    "price": ["giá", "giá phòng", "giá thuê", "giá cho thuê", "giá chốt", "giá sau điều chỉnh", "giá 6-9-12 tháng", "giá (vnd)", "giá ( triệu)", "triệu", "vnd", "price", "thuê"],
    
    # [MỚI] Diện tích rất hay được viết tắt là m2 hoặc S
    "area": ["diện tích", "diện tích (m2)", "diện tích m2", "diện tích phòng", "m2", "s", "dt"],
    
    # Thu gọn status chỉ để đánh giá Tình trạng phòng, tách ngày ra riêng
    "status": ["trạng thái", "tình trạng", "hiện trạng", "tình trạng phòng", "hiện trạng", "trống", "còn", "cọc", "đã cọc", "full"],
    
    # [MỚI] Tách riêng thời gian trống vì sale hay ghi "Ngày trống", "TG vào ở"
    "available_date": ["thời gian trống", "ngày trống", "thời gian vào ở", "vào ở", "ngày xem được phòng", "ngày trống / nhận phòng", "ngày ở", "thời gian ở", "tg xem phòng"],
    
    # [MỚI] Cột nội thất từ các bảng BHome, TingTong...
    "amenities": ["nội thất", "đồ đạc", "tài sản trong phòng", "thông tin nội thất", "trang thiết bị", "thông tin phòng", "mô tả", "thông tin"],
    
    # [MỚI] Chi phí dịch vụ rất lộn xộn, cần bắt các từ khóa sau
    "services": ["dịch vụ", "phí dịch vụ", "dvc", "dịch vụ chung", "tiền dịch vụ", "dịch vụ bao gồm", "điện", "nước", "net", "mạng", "internet", "phí dịch vụ chung"],
    
    # Bổ sung "ql", "sđt bạn dẫn" từ TQ Housing, BHome...
    "contact": ["quản lý", "ql", "sđt", "liên hệ", "người dẫn", "dẫn khách", "sđt người dẫn", "sđt bạn dẫn", "sđt quản lý", "tên quản lý", "phone", "zalo", "dẫn"],
    
    # [MỚI] Lấy link ảnh từ các cột (nhiều sheet gộp Ảnh+Video)
    "images": ["ảnh", "video", "link ảnh", "link ảnh, videos", "hình ảnh", "ảnh+video", "link hình ảnh", "link thông tin", "ảnh + vid", "link ảnh+ video"],
    
    # [MỚI] Bắt các cột ghi chú, giới hạn người, xe, pet, hoa hồng
    "note": ["ghi chú", "chú ý", "lưu ý", "đặc điểm", "thanh toán", "hình thức tt", "cọc", "đóng 1:1", "hoa hồng", "xe điện", "pet", "pet / xe điện", "số người ở max", "số lượng xe máy", "gửi xe", "cầu thang", "thang"]
}


def clean_price(val):
    if pd.isna(val) or val == "": return 0
    s = str(val).lower().replace("tr", "000000").replace(".", "").replace(",", "")
    nums = "".join(filter(str.isdigit, s))
    return int(nums) if nums else 0

def smart_find_columns(columns):
    mapping = {}
    cols_clean = [str(c).lower().strip() for c in columns]
    for concept, keywords in KEYWORD_MAP.items():
        for i, col_name in enumerate(cols_clean):
            if any(kw in col_name for kw in keywords):
                mapping[concept] = i
                break
    return mapping

def run():
    print("🚀 Sếp Huân lệnh, máy bắt đầu càn quét toàn bộ bảng hàng...")
    
    with open("configuration.json", "r", encoding="utf-8") as f:
        config = json.load(f)

    all_data_to_push = []

    for src in config.get("sources", []):
        name = src.get("name", "Nguồn lạ")
        sheet_id = src.get("sheet_id")
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        
        try:
            # Tìm dòng header chuẩn
            df_raw = pd.read_csv(url, header=None, nrows=15)
            header_idx = 0
            for i, row in df_raw.iterrows():
                row_str = " ".join(row.astype(str)).lower()
                if any(kw in row_str for kws in KEYWORD_MAP.values() for kw in kws):
                    header_idx = i
                    break
            
            df = pd.read_csv(url, skiprows=header_idx).fillna("")
            col_map = smart_find_columns(df.columns)
            
            print(f"📊 Nguồn [{name}]: Đã tìm thấy các cột: {list(col_map.keys())}")

            # Dò địa chỉ dự phòng
            if "address" not in col_map:
                for i in range(len(df.columns)):
                    if len(str(df.iloc[0, i])) > 10:
                        col_map["address"] = i
                        break

            if "address" not in col_map:
                print(f"❌ Chịu thua nguồn {name} vì không thấy cột Địa chỉ!")
                continue

            for idx, row in df.iterrows():
                addr_val = str(row.iloc[col_map["address"]]).strip()
                
                # Lọc bỏ các dòng tiêu đề lặp lại hoặc dòng trống
                if addr_val and len(addr_val) > 5 and addr_val.lower() not in KEYWORD_MAP["address"]:
                    # Tạo room_number duy nhất nhất có thể
                    room_val = str(row.iloc[col_map["room_number"]]).strip() if "room_number" in col_map else f"P{idx+1}"
                    if room_val == "" or room_val.lower() == "nan": room_val = f"P{idx+1}"

                    record = {
                        "source_name": name[:255],
                        "address": addr_val[:500],
                        "room_number": room_val[:100],
                        "price": clean_price(row.iloc[col_map["price"]]) if "price" in col_map else 0,
                        "status": str(row.iloc[col_map["status"]])[:50] if "status" in col_map else "AVAILABLE",
                        "contact": str(row.iloc[col_map["contact"]])[:255] if "contact" in col_map else ""
                    }
                    all_data_to_push.append(record)
                    
        except Exception as e:
            print(f"❌ Lỗi tại nguồn {name}: {e}")

    if all_data_to_push:
        headers = {
            "apikey": SUPABASE_KEY, 
            "Authorization": f"Bearer {SUPABASE_KEY}", 
            "Content-Type": "application/json", 
            "Prefer": "resolution=merge-duplicates"
        }
        
        # Chia nhỏ data để đẩy (batch) nếu quá lớn
        batch_size = 100
        for i in range(0, len(all_data_to_push), batch_size):
            batch = all_data_to_push[i:i + batch_size]
            res = requests.post(SUPABASE_URL, headers=headers, json=batch)
            print(f"📡 Đang đẩy đợt {i//batch_size + 1}... Status: {res.status_code}")

        print(f"✅ ĐÃ XONG! Tổng cộng {len(all_data_to_push)} phòng đã được đưa vào kho dữ liệu.")
    else:
        print("ℹ️ Không tìm thấy dữ liệu nào để đẩy lên.")

if __name__ == "__main__":
    run()
