import json
import requests
import pandas as pd
import os

# Gọi mã bảo mật từ GitHub Secrets
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")

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