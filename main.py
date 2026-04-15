import os
import pandas as pd
import requests
import json

# Lấy cấu hình từ Secret
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Từ điển từ khóa để máy tự hiểu cột
KEYWORD_MAP = {
    "address": ["địa chỉ", "vị trí", "tòa nhà", "nhà"],
    "price": ["giá", "giá thuê", "giá chốt", "triệu"],
    "status": ["trạng thái", "tình trạng", "hiện trạng", "còn/hết"],
    "room_number": ["phòng", "số phòng", "mã phòng"],
    "contact": ["quản lý", "sđt", "liên hệ", "người dẫn"]
}

def clean_price(val):
    if pd.isna(val) or val == "": return 0
    s = str(val).lower().replace("tr", "000000").replace(".", "").replace(",", "")
    nums = "".join(filter(str.isdigit, s))
    return int(nums) if nums else 0

def smart_find_columns(columns):
    mapping = {}
    for concept, keywords in KEYWORD_MAP.items():
        for i, col in enumerate(columns):
            if any(kw in str(col).lower() for kw in keywords):
                mapping[concept] = i
                break
    return mapping

def run():
    with open("configuration.json", "r", encoding="utf-8") as f:
        config = json.load(f)

    all_data = []
    for src in config["sources"]:
        url = f"https://docs.google.com/spreadsheets/d/{src['sheet_id']}/export?format=csv"
        try:
            # Đọc 5 dòng đầu để tìm dòng tiêu đề chuẩn
            df_check = pd.read_csv(url, header=None, nrows=5)
            header_idx = 0
            for i, row in df_check.iterrows():
                if any(kw in str(val).lower() for val in row for kws in KEYWORD_MAP.values() for kw in kws):
                    header_idx = i
                    break
            
            df = pd.read_csv(url, skiprows=header_idx).fillna("")
            col_map = smart_find_columns(df.columns)
            
            for _, row in df.iterrows():
                addr_idx = col_map.get("address")
                if addr_idx is not None and str(row.iloc[addr_idx]).strip():
                    data = {
                        "source_name": src["name"],
                        "address": str(row.iloc[addr_idx])[:500],
                        "room_number": str(row.iloc[col_map.get("room_number")])[:100] if "room_number" in col_map else "",
                        "price": clean_price(row.iloc[col_map.get("price")]) if "price" in col_map else 0,
                        "status": str(row.iloc[col_map.get("status")])[:50] if "status" in col_map else "UNKNOWN",
                        "contact": str(row.iloc[col_map.get("contact")])[:255] if "contact" in col_map else ""
                    }
                    all_data.append(data)
        except Exception as e:
            print(f"Bỏ qua {src['name']} do lỗi: {e}")

    if all_data:
        headers = {"apikey": SUPABASE_KEY, "Authorization": f"Bearer {SUPABASE_KEY}", "Content-Type": "application/json", "Prefer": "resolution=merge-duplicates"}
        requests.post(SUPABASE_URL, headers=headers, json=all_data)
        print(f"✅ Đã đẩy thành công {len(all_data)} phòng lên Supabase!")

if __name__ == "__main__":
    run()
