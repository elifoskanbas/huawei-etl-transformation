import pandas as pd # data isleme ve tabloya cevirme islemleri icin
import requests  # API'dan data cekmek icin
from time import sleep, time  # kodun belli sure beklemesi icin
from datetime import datetime  # zaman  islemleri icin

def get_api_data(): # API'den data ceken fonksiyon
    users = [] # userlari saklayacagimiz bos liste

    for i in range(300):  # 10 dakika boyunca her 2 saniyede bir API'den data cekmek icin 600 / 2 = 300 kez 
        try:
            response = requests.get("https://randomuser.me/api") # randomuser.me API'sine request gonderilir

            if response.status_code == 200:  # eger request basariliysa (200 OK)
                data = response.json()['results'][0] # gelen JSON datasini alir ve ilk useri seceriz

                
                user_info = { # user bilgilerini bir dictionary olarak toplariz
                    'gender': data['gender'],
                    'title': data['name']['title'],
                    'first_name': data['name']['first'],
                    'last_name': data['name']['last'],
                    'email': data['email'],
                    'dob': data['dob']['date'],
                    'registered': data['registered']['date'],
                    'country': data['location']['country'],
                    'city': data['location']['city'],
                    'latitude': data['location']['coordinates']['latitude'],
                    'longitude': data['location']['coordinates']['longitude'],
                    'username': data['login']['username'],
                    'phone': data['phone'],
                    'cell': data['cell'],
                    'nat': data['nat'],
                    'picture': data['picture']['large']
                }
                users.append(user_info) # user infosunu listeye ekleriz

            else:
                print(f"Hata! Durum kodu: {response.status_code}") # basarisiz requestlerde hata mesaji yazdirilir
        except Exception as e:
            print(f"İstek sirasinda hata olustu: {e}")  # request sirasinda beklenmeyen bir hata olursa yazdirilir
            
        sleep(2)  # 2 saniye bekle

    return users # tum user datalarini dondurur

def create_parquet_file(data): # toplanan datalari Parquet fileina kaydeden fonksiyon
    df = pd.DataFrame(data) #listeyi pandas DataFrame (tablo) haline getiririz
    df.to_parquet("users.parquet", engine='pyarrow', index=False) # Parquet fileina yazariz. (engine='pyarrow' kullanarak)
    print("dataler users.parquet dosyasina yazildi.") # file yazma islemi tamamlandiginda mesaj verir

if __name__ == "__main__": # main program blogu - bu file dogrudan calistirildiginda devreye girer
    print("data cekme islemi basladi...")
    data = get_api_data()  # API'den user datalarini al
    print("data cekme tamamlandi. Parquet dosyasi olusturuluyor...")
    create_parquet_file(data) # datayi Parquet fileina kaydet
