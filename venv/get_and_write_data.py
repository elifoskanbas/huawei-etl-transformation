# get_and_write_data.py
import pandas as pd
import requests
from time import sleep, time
from datetime import datetime, timedelta

def get_api_data():
    user_data = []
    start_time = time()
    duration = 10 * 60  # 10 dakika = 600 saniye

    print("Veri çekme başladı...")

    while (time() - start_time) < duration:
        try:
            response = requests.get("https://randomuser.me/api/")
            if response.status_code == 200:
                json_data = response.json()
                user_data.append(json_data['results'][0])  # Sadece 'results' içindeki ilk kullanıcı
            else:
                print("API hatası:", response.status_code)
        except Exception as e:
            print("İstek sırasında hata:", e)
        
        sleep(2)  # 2 saniye bekle

    print("Veri çekme tamamlandı.")
    return user_data

def create_parquet_file(data, filename="user_data.parquet"):
    df = pd.json_normalize(data)
    
    # Posta kodu karışıklığına karşı tüm 'location.postcode' sütununu string yap
    if 'location.postcode' in df.columns:
        df['location.postcode'] = df['location.postcode'].astype(str)

    df.to_parquet(filename, index=False)
    print(f"{filename} başarıyla oluşturuldu.")


if __name__ == "__main__":  
    data = get_api_data()
    create_parquet_file(data)
