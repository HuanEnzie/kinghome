import os
import pandas as pd
import requests
import json

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

# Từ điển từ khóa linh hoạt hơn cho sếp
KEYWORD_MAP = {
    "address": ["địa chỉ", "vị trí", "tòa nhà", "nhà", "khu vực"],
    "price": ["giá", "triệu", "vnd", "price"],
    "status": ["trạng thái", "tình trạng", "trống", "còn", "hiện trạng"],
    "room_number": ["phòng", "số p", "mã p"],
    "contact": ["quản lý", "sđt", "liên hệ", "người dẫn", "dẫn khách"]
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
    if not os.path.exists("configuration.json"):
        print("❌ Không tìm thấy file configuration.json")
        return

    with open("configuration.json", "r", encoding="utf-8") as f:
        config = json.load(f)

    all_data = []
    for src in config.get("sources", []):
        print(f"🔍 Đang quét nguồn: {src['name']}")
        url = f"https://docs.google.com/spreadsheets/d/{src['sheet_id']}/export?format=csv"
        try:
            # Đọc thô để tìm header
            df_raw = pd.read_csv(url, header=None, nrows=10)
            header_idx = 0
            for i, row in df_raw.iterrows():
                row_str = " ".join(row.astype(str).lower())
                if any(kw in row_str for kws in KEYWORD_MAP.values() for kw in kws):
                    header_idx = i
                    break
            
            df = pd.read_csv(url, skiprows=header_idx).fillna("")
            col_map = smart_find_columns(df.columns)
            
            if "address" not in col_map:
                print(f"⚠️ Không tìm thấy cột Địa chỉ tại {src['name']}")
                continue

            for _, row in df.iterrows():
                addr_val = str(row.iloc[col_map["address"]]).strip()
                if addr_val and len(addr_val) > 5: # Tránh lấy các dòng trống hoặc rác
                    data = {
                        "source_name": src["name"][:255],
                        "address": addr_val[:500],
                        "room_number": str(row.iloc[col_map["room_number"]])[:100] if "room_number" in col_map else "Chưa rõ",
                        "price": clean_price(row.iloc[col_map["price"]]) if "price" in col_map else 0,
                        "status": str(row.iloc[col_map["status"]])[:50] if "status" in col_map else "UNKNOWN",
                        "contact": str(row.iloc[col_map["contact"]])[:255] if "contact" in col_map else ""
                    }
                    all_data.append(data)
        except Exception as e:
            print(f"❌ Lỗi tại {src['name']}: {e}")

    if all_data:
        headers = {
            "apikey": SUPABASE_KEY, 
            "Authorization": f"Bearer {SUPABASE_KEY}", 
            "Content-Type": "application/json", 
            "Prefer": "resolution=merge-duplicates"
        }
        res = requests.post(SUPABASE_URL, headers=headers, json=all_data)
        print(f"✅ Đã đẩy {len(all_data)} phòng lên Supabase. Status: {res.status_code}")
    else:
        print("ℹ️ Không có dữ liệu mới để cập nhật.")

if __name__ == "__main__":
    run()
