import requests
import pandas as pd
from datetime import datetime
import time

cryptos = ["bitcoin", "ethereum", "tether","ripple", "litecoin", "solana","cardano","dogecoin", "chainlink", "polkadot", "dai"]

url = "https://api.coingecko.com/api/v3/coins/markets"

params = {
    'vs_currency': 'usd',  
    'ids': ','.join(cryptos)
}

def get_data():
    response = requests.get(url, params=params)
    if response.status_code == 200:
        return response.json()
    else:
        print("Error al obtener datos. Reintentando en 10 minutos")
        time.sleep(10 * 60)
        return process_data()


def process_data():
    crypto_data = get_data()
    current_datetime = datetime.now().strftime("%Y-%m-%d %H:%M")
    crypto_list = []
    for crypto in crypto_data:
        crypto_info = {
            'ID': crypto.get('id'),
            'Symbol': crypto.get('symbol'),
            'Name': crypto.get('name'),
            'Current Price': crypto.get('current_price'),
            'Market Cap': crypto.get('market_cap'),
            'Total Volume': crypto.get('total_volume'),
            'High 24h': crypto.get('high_24h'),
            'Low 24h': crypto.get('low_24h'),
            'Price Change 24h': crypto.get('price_change_24h'),
            'Price Change Percentage 24h': crypto.get('price_change_percentage_24h'),
            'Market Cap Change 24h': crypto.get('market_cap_change_24h'),
            'Market Cap Change Percentage 24h': crypto.get('market_cap_change_percentage_24h'),
            'Circulating Supply': crypto.get('circulating_supply'),
            'Total Supply': crypto.get('total_supply'),
            'Ath': crypto.get('ath'),
            'Ath Change Percentage': crypto.get('ath_change_percentage'),
            'DateTime': current_datetime
        }
        crypto_list.append(crypto_info)
    data = pd.DataFrame(crypto_list)
    print(data)


#esto hace que sea un bucle infinito que cosnulta informacion cada 3 horas, si no es lo que se busca se puede eliminar el while true y corre una vez
while True:
      process_data()
      time.sleep(3 * 60 * 60) 
