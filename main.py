import json
import requests
import pandas as pd
import os

# Gọi mã bảo mật từ GitHub Secrets
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
# Từ điển từ khóa thông minh
CONCEPT_MAP = {
    "address": ["địa chỉ", "vị trí", "khu vực", "tòa nhà", "address", "nơi ở"],
    "price": ["giá", "giá thuê", "giá phòng", "triệu", "price", "giá chốt"],
    "status": ["trạng thái", "tình trạng", "hiện trạng", "còn/hết", "status"],
    "room_number": ["phòng", "số phòng", "mã phòng", "tên phòng"],
    "amenities": ["nội thất", "đồ đạc", "trang thiết bị"],
    "contact": ["quản lý", "sđt", "liên hệ", "người dẫn", "contact"]
}

def find_column_indices(df_columns):
    mapping = {}
    for concept, keywords in CONCEPT_MAP.items():
        for i, col_name in enumerate(df_columns):
            col_name_clean = str(col_name).lower().strip()
            if any(kw in col_name_clean for kw in keywords):
                mapping[concept] = i
                break # Tìm thấy rồi thì dừng để tránh nhầm cột khác
    return mapping

def process_sheet(sheet_id):
    url = f"https://docs.google.com/spreadsheets/d/{sheet_id}/export?format=csv"
    df_raw = pd.read_csv(url, header=None) # Đọc thô hoàn toàn
    
    # Bước 1: Tìm dòng Header
    header_row_index = 0
    max_matches = 0
    for i in range(min(10, len(df_raw))): # Quét 10 dòng đầu
        row_values = df_raw.iloc[i].astype(str).str.lower().tolist()
        matches = sum(1 for val in row_values if any(kw in val for kws in CONCEPT_MAP.values() for kw in kws))
        if matches > max_matches:
            max_matches = matches
            header_row_index = i
            
    # Bước 2: Thiết lập DataFrame với Header chuẩn
    df = pd.read_csv(url, skiprows=header_row_index).fillna("")
    col_map = find_column_indices(df.columns)
    
    # Bước 3: Trích xuất dữ liệu dựa trên map vừa tìm được
    standardized_data = []
    for _, row in df.iterrows():
        # Chỉ lấy nếu tìm được cột Địa chỉ và cột đó không trống
        addr_idx = col_map.get("address")
        if addr_idx is not None and str(row.iloc[addr_idx]).strip():
            record = {
                "address": str(row.iloc[addr_idx])[:500],
                "price": clean_price(row.iloc[col_map.get("price")]) if "price" in col_map else 0,
                "status": get_status(row.iloc[col_map.get("status")]) if "status" in col_map else "UNKNOWN",
                # ... các trường khác tương tự
            }
            standardized_data.append(record)
    return standardized_data
    
def clean_price(val):
    if pd.isna(val): return 0
    s = str(val).lower().replace("tr", "000000").replace(".", "").replace(",", "")
    nums = "".join(filter(str.isdigit, s))
    return int(nums) if nums else 0

def get_status(val):
    if pd.isna(val): return "UNKNOWN"
    s = str(val).lower()
    if any(k in s for k in ["trống", "sẵn", "ở luôn"]): return "AVAILABLE"
    if any(k in s for k in ["full", "kín", "cọc"]): return "RENTED"
    return "UPCOMING"

def run_pipeline():
    # Đọc cấu hình từ file mapping
    with open("configuration.json", "r", encoding="utf-8") as f:
        config = json.load(f)

    all_data = []
    for src in config["sources"]:
        # Chuyển link view sang link tải CSV trực tiếp
        url = f"https://docs.google.com/spreadsheets/d/{src['sheet_id']}/export?format=csv"
        try:
            df = pd.read_csv(url).fillna("")
            cols = src["mapping"]
            
            for _, row in df.iterrows():
                # Cắt chuỗi để tránh lỗi Supabase Column Length (Max 100-255)
                data = {
                    "source_name": src["name"][:255],
                    "address": str(row.iloc[cols["address"]])[:500],
                    "room_number": str(row.iloc[cols["room_number"]])[:100],
                    "room_type": str(row.iloc[cols["room_type"]])[:100],
                    "price": clean_price(row.iloc[cols["price"]]),
                    "status": get_status(row.iloc[cols["status"]]),
                    "contact": str(row.iloc[cols["contact"]])[:255],
                    "amenities": str(row.iloc[cols["amenities"]])[:2000],
                    "services": str(row.iloc[cols["services"]])[:2000]
                }
                all_data.append(data)
        except Exception as e:
            print(f"Lỗi tại {src['name']}: {e}")

    # Đẩy dữ liệu lên Supabase
    if all_data:
        headers = {
            "apikey": SUPABASE_KEY,
            "Authorization": f"Bearer {SUPABASE_KEY}",
            "Content-Type": "application/json",
            "Prefer": "resolution=merge-duplicates"
        }
        res = requests.post(SUPABASE_URL, headers=headers, json=all_data)
        print(f"Đã đẩy {len(all_data)} phòng. Status: {res.status_code}")

if __name__ == "__main__":
    run_pipeline()