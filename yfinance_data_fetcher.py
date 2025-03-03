import yfinance as yf
import pandas as pd
import yaml
import os
from typing import Dict, Any
from google_sheet_api import GoogleSheetsUploader

class YahooFinanceDataFetcher:
    """A class to fetch, save, and upload historical stock/index data from Yahoo Finance."""
    
    def __init__(self, config_file: str):
        """Initialize the fetcher with a configuration file."""
        with open(config_file, "r") as file:
            self.config: Dict[str, Any] = yaml.safe_load(file)
        
        self.symbol_map: Dict[str, str] = self.config.get("symbols_yfinance", {})
        self.minute_period: str = self.config.get("minute_period", "7d")
        self.minute_interval: str = self.config.get("minute_interval", "1m")
        self.daily_period: str = self.config.get("daily_period", "10y")
        self.daily_interval: str = self.config.get("daily_interval", "1d")
        self.output_dir: str = self.config.get("output_directory", ".")
        
        os.makedirs(self.output_dir, exist_ok=True)

    def fetch_data(self, ticker: str, period: str, interval: str) -> pd.DataFrame:
        """Fetch historical data for a given ticker, period, and interval."""
        try:
            data = yf.download(ticker, period=period, interval=interval)
            if data.empty:
                print(f"No data available for {ticker}.")
            return data
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
            return pd.DataFrame()

    def clean_data(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """Clean and preprocess the downloaded data."""
        if data.empty:
            return data
        
        data = data.reset_index()
        
        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.droplevel(1)

        if "Date" in data.columns:
            data.rename(columns={"Date": "datetime"}, inplace=True)
        elif "Datetime" in data.columns:
            data.rename(columns={"Datetime": "datetime"}, inplace=True)
        
        data["datetime"] = pd.to_datetime(data["datetime"], errors="coerce", utc=True)

        numeric_cols = ["Close", "High", "Low", "Open", "Volume"]
        for col in numeric_cols:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')

        data["Symbol"] = symbol
        column_order = ["Symbol", "datetime"] + [col for col in data.columns if col not in ["Symbol", "datetime"]]
        return data[column_order]

    def process_all_symbols(self) -> None:
        """Fetch, clean, merge, and upload both minute and daily data for all symbols."""
        all_minute_data = []
        all_daily_data = []

        for symbol, ticker in self.symbol_map.items():
            print(f"Fetching data for {symbol} ({ticker})...")

            minute_data = self.fetch_data(ticker, period=self.minute_period, interval=self.minute_interval)
            daily_data = self.fetch_data(ticker, period=self.daily_period, interval=self.daily_interval)

            if not minute_data.empty:
                all_minute_data.append(self.clean_data(minute_data, symbol))
            
            if not daily_data.empty:
                all_daily_data.append(self.clean_data(daily_data, symbol))
        
        if all_minute_data:
            merged_minute_data = pd.concat(all_minute_data, ignore_index=True)
            minute_file = os.path.join(self.output_dir, "yfinance_price_minutes.csv")
            merged_minute_data.to_csv(minute_file, index=False)
            print(f"Saved {minute_file}")
            
            uploader = GoogleSheetsUploader(credentials_file="credential_google_sheets.json", spreadsheet_name="Stock Market Dashboard")
            uploader.upload_to_sheets(minute_file, name_sheet="yfinance_price_minutes")
            print("Uploaded yfinance_price_minutes.csv to Google Sheets")
        
        if all_daily_data:
            merged_daily_data = pd.concat(all_daily_data, ignore_index=True)
            daily_file = os.path.join(self.output_dir, "yfinance_price_daily.csv")
            merged_daily_data.to_csv(daily_file, index=False)
            print(f"Saved {daily_file}")
            
            uploader.upload_to_sheets(daily_file, name_sheet="yfinance_price_daily")
            print("Uploaded yfinance_price_daily.csv to Google Sheets")

if __name__ == "__main__":
    config_file: str = "config.yaml"
    fetcher = YahooFinanceDataFetcher(config_file)
    fetcher.process_all_symbols()