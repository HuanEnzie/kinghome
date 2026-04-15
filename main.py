import os
import pandas as pd
import requests
import json

# --- CẤU HÌNH HỆ THỐNG ---
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Từ điển từ khóa thông minh để máy tự "đọc hiểu" tiêu đề cột
KEYWORD_MAP = {
    "address": ["địa chỉ", "vị trí", "tòa nhà", "nhà", "khu vực", "tên tòa", "tên nhà"],
    "price": ["giá", "triệu", "vnd", "price", "giá chốt", "giá thuê", "thuê"],
    "status": ["trạng thái", "tình trạng", "trống", "cọc", "còn", "hiện trạng", "vào ở"],
    "room_number": ["phòng", "số p", "mã p", "tên phòng", "số phòng", "trục", "tên p"],
    "contact": ["quản lý", "sđt", "liên hệ", "người dẫn", "dẫn khách", "phone", "zalo"]
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
    print("🚀 Bắt đầu quá trình đồng bộ dữ liệu...")
    
    if not os.path.exists("configuration.json"):
        print("❌ LỖI: Không tìm thấy file configuration.json")
        return

    with open("configuration.json", "r", encoding="utf-8") as f:
        config = json.load(f)

    all_raw_data = []

    for src in config.get("sources", []):
        name = src.get("name", "Unknown")
        sheet_id = src.get("sheet_id")
        print(f"🔍 Đang quét nguồn: {name}...")
        
        url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
        
        try:
            # Bước 1: Tìm dòng Header chuẩn trong 20 dòng đầu
            df_header_check = pd.read_csv(url, header=None, nrows=20)
            header_idx = 0
            for i, row in df_header_check.iterrows():
                row_str = " ".join(row.astype(str)).lower()
                if any(kw in row_str for kws in KEYWORD_MAP.values() for kw in kws):
                    header_idx = i
                    break
            
            # Bước 2: Đọc dữ liệu từ dòng header đã tìm thấy
            df = pd.read_csv(url, skiprows=header_idx).fillna("")
            col_map = smart_find_columns(df.columns)
            
            # Chế độ dò địa chỉ dự phòng nếu không thấy cột tên "Địa chỉ"
            if "address" not in col_map:
                for i in range(len(df.columns)):
                    sample = str(df.iloc[0, i]) if len(df) > 0 else ""
                    if len(sample) > 10: 
                        col_map["address"] = i
                        break

            if "address" not in col_map:
                print(f"⚠️ Bỏ qua {name}: Không tìm thấy cột Địa chỉ.")
                continue

            # Bước 3: Trích xuất và làm sạch từng dòng
            for _, row in df.iterrows():
                addr_val = str(row.iloc[col_map["address"]]).strip()
                
                # Chỉ lấy dòng có địa chỉ thật (độ dài > 5) và không phải dòng tiêu đề lặp lại
                if addr_val and len(addr_val) > 5 and addr_val.lower() not in KEYWORD_MAP["address"]:
                    room_val = str(row.iloc[col_map["room_number"]])[:100] if "room_number" in col_map else "Chưa rõ"
                    
                    record = {
                        "source_name": name[:255],
                        "address": addr_val[:500],
                        "room_number": room_val,
                        "price": clean_price(row.iloc[col_map["price"]]) if "price" in col_map else 0,
                        "status": str(row.iloc[col_map["status"]])[:50] if "status" in col_map else "AVAILABLE",
                        "contact": str(row.iloc[col_map["contact"]])[:255] if "contact" in col_map else ""
                    }
                    all_raw_data.append(record)
                    
        except Exception as e:
            print(f"❌ Lỗi khi xử lý nguồn {name}: {e}")

    # Bước 4: Xử lý trùng lặp và đẩy lên Supabase
    if all_raw_data:
        # Loại bỏ trùng lặp ngay trong Python dựa trên bộ khóa (Nguồn, Địa chỉ, Số phòng)
        df_final = pd.DataFrame(all_raw_data)
        count_before = len(df_final)
        df_final = df_final.drop_duplicates(subset=['source_name', 'address', 'room_number'], keep='first')
        count_after = len(df_final)
        
        if count_before != count_after:
            print(f"🧹 Đã lọc bỏ {count_before - count_after} dòng trùng lặp trong dữ liệu thô.")

        clean_data = df_final.to_dict(orient='records')

        headers = {
            "apikey": SUPABASE_KEY, 
            "Authorization": f"Bearer {SUPABASE_KEY}", 
            "Content-Type": "application/json", 
            "Prefer": "resolution=merge-duplicates" # Cơ chế UPSERT của Supabase
        }
        
        try:
            res = requests.post(SUPABASE_URL, headers=headers, json=clean_data)
            if res.status_code in [200, 201, 204]:
                print(f"✅ THÀNH CÔNG: Đã đồng bộ {len(clean_data)} phòng lên hệ thống!")
            else:
                print(f"❌ Lỗi đẩy dữ liệu (Status {res.status_code}): {res.text}")
        except Exception as e:
            print(f"❌ Lỗi kết nối Supabase: {e}")
    else:
        print("ℹ️ Không tìm thấy dữ liệu hợp lệ để cập nhật.")

if __name__ == "__main__":
    run()
