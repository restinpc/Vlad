#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import os
import sys
import argparse
import datetime
import pandas as pd
import yfinance as yf
import mysql.connector
from sqlalchemy import create_engine, text
from curl_cffi import requests as crequests
import traceback
from dotenv import load_dotenv

load_dotenv()

TRACE_URL = "https://server.brain-project.online/trace.php"
NODE_NAME = os.getenv("NODE_NAME", "tradingview_loader")
EMAIL = os.getenv("ALERT_EMAIL", "vladyurjevitch@yandex.ru")


def send_error_trace(exc: Exception, script_name: str = "TradingView.py"):
    logs = f"Node: {NODE_NAME}\nScript: {script_name}\nException: {repr(exc)}\n\nTraceback:\n{traceback.format_exc()}"
    payload = {"url": "cli_script", "node": NODE_NAME, "email": EMAIL, "logs": logs}
    try:
        import requests
        requests.post(TRACE_URL, data=payload, timeout=10)
    except:
        pass


parser = argparse.ArgumentParser(description="TradingView Data Collector → MySQL (ТОЛЬКО одна таблица)")
parser.add_argument("table_name", help="Имя целевой таблицы в БД")
parser.add_argument("host", nargs="?", default=os.getenv("DB_HOST"))
parser.add_argument("port", nargs="?", default=os.getenv("DB_PORT", "3306"))
parser.add_argument("user", nargs="?", default=os.getenv("DB_USER"))
parser.add_argument("password", nargs="?", default=os.getenv("DB_PASSWORD"))
parser.add_argument("database", nargs="?", default=os.getenv("DB_NAME"))
args = parser.parse_args()

if not all([args.host, args.user, args.password, args.database]):
    print("❌ Ошибка: не указаны параметры подключения к БД")
    sys.exit(1)

SQLALCHEMY_URL = f"mysql+mysqlconnector://{args.user}:{args.password}@{args.host}:{args.port}/{args.database}"

# 🔒 СТРОГО ОДНА таблица — никаких циклов по всем активам!
ASSETS_CONFIG = {
    'tradingview_market_data': {
        'assets': {
            'EURUSD': 'EURUSD=X',
            'BTC': 'BTC-USD',
            'ETH': 'ETH-USD',
            'DXY': 'DX-Y.NYB',
            'SP500': '^GSPC',
            'Nasdaq': '^IXIC',
            'VIX': '^VIX',
            'Oil': 'CL=F',
            'Gold': 'GC=F',
            'US10Y': '^TNX',
        },
        'description': 'Почасовые рыночные данные с Yahoo Finance'
    }
}


class TradingViewCollector:
    def __init__(self, table_name: str):
        self.table_name = table_name
        self.engine = create_engine(SQLALCHEMY_URL, pool_recycle=3600)
        if table_name not in ASSETS_CONFIG:
            print(f"❌ Ошибка: неизвестная таблица '{table_name}'")
            sys.exit(1)
        self.config = ASSETS_CONFIG[table_name]

    def get_last_datetime(self) -> datetime.datetime | None:
        try:
            with self.engine.connect() as conn:
                result = conn.execute(text(f"SELECT MAX(`datetime`) FROM `{self.table_name}`"))
                row = result.fetchone()
                return row[0] if row and row[0] else None
        except:
            return None

    def get_market_data(self, last_dt: datetime.datetime | None) -> pd.DataFrame | None:
        print("[*] Скачивание рыночных данных (Yahoo Finance)...")
        tickers = list(self.config['assets'].values())
        try:
            if last_dt:
                start_date = last_dt - datetime.timedelta(days=1)
                data = yf.download(tickers, start=start_date.strftime('%Y-%m-%d'), interval="1h", group_by='ticker',
                                   progress=False)
            else:
                data = yf.download(tickers, period="2y", interval="1h", group_by='ticker', progress=False)
        except Exception as e:
            print(f"   -> Ошибка Yahoo: {e}")
            return None

        dfs = {}
        for name, ticker in self.config['assets'].items():
            try:
                if isinstance(data.columns, pd.MultiIndex):
                    if ticker in data.columns.levels[0]:
                        df = data[ticker].copy()
                    else:
                        continue
                else:
                    df = data.copy()
                cols_map = {}
                if 'Close' in df.columns: cols_map['Close'] = f'{name}_Close'
                if 'Volume' in df.columns: cols_map['Volume'] = f'{name}_Volume'
                if not cols_map:
                    continue
                df = df.rename(columns=cols_map)[list(cols_map.values())]
                df.index = pd.to_datetime(df.index).tz_localize(None)
                dfs[name] = df
            except:
                continue
        if not dfs:
            return None
        full_df = pd.concat(dfs.values(), axis=1)
        full_df.sort_index(inplace=True)
        full_df.dropna(how='all', inplace=True)
        if last_dt:
            full_df = full_df[full_df.index > last_dt]
        return full_df if not full_df.empty else None

    def get_crypto_metrics(self) -> pd.DataFrame | None:
        print("[*] Скачивание On-Chain метрик...")
        metrics = {}
        try:
            url = "https://api.blockchain.info/charts/hash-rate?timespan=2years&format=json"
            r = crequests.get(url)
            df = pd.DataFrame(r.json()['values'])
            df['x'] = pd.to_datetime(df['x'], unit='s')
            df.set_index('x', inplace=True)
            df.columns = ['BTC_Hashrate']
            metrics['Hashrate'] = df.resample('1h').ffill()
        except:
            pass
        return pd.concat(metrics.values(), axis=1) if metrics else pd.DataFrame()

    def save_market_data_incremental(self, df_matrix: pd.DataFrame):
        if df_matrix.empty:
            return
        try:
            df_matrix.to_sql(
                name=self.table_name,
                con=self.engine,
                if_exists='append',
                index=True,
                index_label='datetime',
                chunksize=1000,
                method='multi'
            )
            print(f"✅ Добавлено {len(df_matrix)} строк в '{self.table_name}'")
        except Exception as e:
            print(f"❌ Ошибка записи: {e}")


def main():
    # 🔒 ГАРАНТИЯ: только одна таблица
    if args.table_name not in ASSETS_CONFIG:
        print(f"❌ Ошибка: неизвестная таблица '{args.table_name}'. Допустимые:")
        for name in ASSETS_CONFIG.keys():
            print(f"  - {name}")
        sys.exit(1)

    print(f"🚀 TRADINGVIEW COLLECTOR (ТОЛЬКО: {args.table_name})")
    print(f"База: {args.host}:{args.port}/{args.database}")
    print(f"🎯 ЦЕЛЕВАЯ ТАБЛИЦА: {args.table_name}")
    print("=" * 60)

    collector = TradingViewCollector(args.table_name)
    last_dt = collector.get_last_datetime()
    df_market = collector.get_market_data(last_dt)
    df_onchain = collector.get_crypto_metrics()

    if df_market is not None:
        final_df = df_market.join(df_onchain, how='left').ffill() if not df_onchain.empty else df_market
        collector.save_market_data_incremental(final_df)
        print("=" * 60)
        print("🏁 ЗАГРУЗКА ЗАВЕРШЕНА (только одна таблица обработана)")


if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        raise
    except KeyboardInterrupt:
        print("\n🛑 Прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e!r}")
        send_error_trace(e)
        sys.exit(1)