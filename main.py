import os
import pandas as pd
import requests
import json

# --- CẤU HÌNH HỆ THỐNG ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Từ điển từ khóa thông minh - Càng nhiều từ khóa, máy càng khôn
KEYWORD_MAP = {
    "address": ["địa chỉ", "vị trí", "tòa nhà", "nhà", "khu vực", "tên tòa", "tên nhà", "địa chỉ nhà"],
    "price": ["giá", "triệu", "vnd", "price", "giá chốt", "giá thuê", "thuê", "giá phòng"],
    "status": ["trạng thái", "tình trạng", "trống", "cọc", "còn", "hiện trạng", "vào ở", "thời gian trống"],
    "room_number": ["phòng", "số p", "mã p", "tên phòng", "số phòng", "trục", "tên p", "mã phòng"],
    "contact": ["quản lý", "sđt", "liên hệ", "người dẫn", "dẫn khách", "phone", "zalo", "sđt quản lý"]
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
