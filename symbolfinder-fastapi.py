from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from pydantic import BaseModel
from typing import Optional, List
import multiprocessing as mp
import pandas as pd
import requests
import difflib
import time

app = FastAPI()

class SymbolFinder:
    def extract_crypto_exchange(self, input_file='unique_symbols.csv', output_parquet: str = 'output.parquet'):
        data_list = pd.read_csv(input_file)#[:2]
        result = self.parallel_process(data_list)
        result.to_parquet(output_parquet, index=False)
        return {"status": "Exchange data extracted and saved to parquet."}

    def extract_tv_url(self, tv_symbols_file='tv_symbols.csv', BASE: str = 'BTC', QUOTE: Optional[str] = None):
        df = pd.read_csv(tv_symbols_file)
        df_cleaned = df.dropna(subset=['currency_code'])
        df_cleaned['Base0'] = df_cleaned['symbol'].apply(lambda x: x.rstrip('.P') if x.endswith('.P') else x)
        df_cleaned['Base'] = df_cleaned.apply(lambda x: x['Base0'].replace(x['currency_code'], ''), axis=1)
        return self.determine_exchange(df_cleaned, BASE, QUOTE)

    def get_market_data_with_proxy(self, coin, quote, offset, proxy, retries=3):
        retry_count = 0
        while retry_count < retries:
            proxies = {
                'http': proxy,
                'https': proxy
            }
            try:
                response = requests.get(
                    'https://http-api.livecoinwatch.com/markets',
                    params={'currency': quote, 'limit': 30, 'offset': offset, 'sort': 'depth', 'order': 'descending', 'coin': coin},
                    proxies=proxies,
                    timeout=10
                )
                if response.status_code == 200:
                    return response.json().get('data', [])
                elif response.status_code == 503:
                    retry_count += 1
                    time.sleep(2 ** retry_count)
            except requests.exceptions.RequestException:
                retry_count += 1
                time.sleep(2 ** retry_count)
        return None

    def fetch_data(self, params):
        coin, quote, n, proxy = params
        result = pd.DataFrame()
        coin1 = '_' * n + coin
        offset = 0
        while True:
            data = self.get_market_data_with_proxy(coin1, quote, offset, proxy)
            if not data:
                break
            df = pd.DataFrame(data)
            for _, row in df.iterrows():
                if row['base'] == coin and row['quote'] == quote:
                    row_df = pd.DataFrame([row], columns=df.columns)
                    result = pd.concat([result, row_df], ignore_index=True)
            offset += 30
        return result

    def parallel_process(self, data_list, proxy='http://speyinarxb:81ZxuK_Rgj4Fc2tidi@gate.smartproxy.com:7000'):
        tasks = [(row['0'].split('/')[0], row['0'].split('/')[1], n, proxy) for _, row in data_list.iterrows() for n in range(20)]
        with mp.Pool(mp.cpu_count()) as pool:
            results = pool.map(self.fetch_data, tasks)
        return pd.concat(results, ignore_index=True)

    def create_url(self, trading_pair, exchange):
        s = f"symbol={exchange.upper()}:{trading_pair.upper()}"
        return f"https://s.tradingview.com/widgetembed/?frameElementId=tradingview_abc&{s}&interval=60&theme=dark"

    def generate_trading_urls(self, base_currency, quote_currency, df_filtered):
        df_filtered2 = df_filtered[df_filtered['currency_code'] == quote_currency]
        return [
            {
                'base_currency': base_currency,
                'quote_currency': quote_currency,
                'exchange': row['exchange'],
                'url': self.create_url(row['symbol'], row['source_id'])
            }
            for _, row in df_filtered2.iterrows()
        ]

    def determine_exchange(self, df_cleaned, BASE='BTC', QUOTE=None):
        BASE = BASE.upper()
        if BASE not in df_cleaned['Base'].values:
            return {'message': 'No matches for the base currency!'}
        df_filtered = df_cleaned[df_cleaned['Base'] == BASE]
        if QUOTE is None:
            return self.determine_exchange(df_cleaned, BASE, QUOTE='USD')
        if QUOTE in df_filtered['currency_code'].values:
            return self.generate_trading_urls(BASE, QUOTE, df_filtered)
        else:
            most_similar_QUOTE = max(df_filtered['currency_code'].values, key=lambda x: difflib.SequenceMatcher(None, QUOTE, x).ratio())
            return self.determine_exchange(df_cleaned, BASE, most_similar_QUOTE)

symbol_finder = SymbolFinder()

@app.get("/extract_tv_url/")
def extract_tv_url(BASE: str = 'BTC', QUOTE: Optional[str] = None):
    try:
        return symbol_finder.extract_tv_url(BASE=BASE, QUOTE=QUOTE)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/extract_crypto_exchange/")
def extract_crypto_exchange():
    try:
        file_path = symbol_finder.extract_crypto_exchange()
        return FileResponse(path='unique_symbols.csv', filename="output.parquet", media_type="application/octet-stream")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "symbolfinder-fastapi:app", host="0.0.0.0", port=8018, log_level="info", workers=4
    )
