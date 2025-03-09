import os
import yaml
from datetime import datetime
import pandas as pd
import yfinance as yf
from typing import Dict, Any
from mysql_api import MySQLDataConnector


class YahooFinanceDataFetcher:
    """
    A class to fetch, clean, and process historical stock/index data from Yahoo Finance.
    """

    def __init__(self, config_file: str) -> None:
        """
        Initialize the fetcher with a configuration file.
        
        Args:
            config_file (str): Path to the configuration YAML file.
        """
        with open(config_file, "r") as file:
            self.config: Dict[str, Any] = yaml.safe_load(file)

        self.symbol_map: Dict[str, str] = self.config.get("symbols_yfinance", {})
        self.minute_period: str = self.config.get("minute_period", "7d")
        self.minute_interval: str = self.config.get("minute_interval", "1m")
        self.daily_period: str = self.config.get("daily_period", "10y")
        self.daily_interval: str = self.config.get("daily_interval", "1d")

    def fetch_data(self, ticker: str, period: str, interval: str) -> pd.DataFrame:
        """
        Fetch historical data for a given ticker, period, and interval.
        
        Args:
            ticker (str): The stock/index ticker.
            period (str): The data period to fetch.
            interval (str): The time interval between data points.
        
        Returns:
            pd.DataFrame: The fetched historical data.
        """
        try:
            data = yf.download(ticker, period=period, interval=interval)
            if data.empty:
                print(f"❌ Warning: No data available for {ticker}.")
            return data
        except Exception as e:
            print(f"❌ Error fetching data for {ticker}: {e}")
            return pd.DataFrame()

    def clean_data(self, data: pd.DataFrame, symbol: str) -> pd.DataFrame:
        """
        Clean and preprocess the downloaded data.
        
        Args:
            data (pd.DataFrame): Raw data from Yahoo Finance.
            symbol (str): Symbol corresponding to the data.
        
        Returns:
            pd.DataFrame: Cleaned and structured data.
        """
        if data.empty:
            return data

        data = data.reset_index()

        if isinstance(data.columns, pd.MultiIndex):
            data.columns = data.columns.droplevel(1)

        data.rename(columns={"Date": "Datetime", "datetime": "Datetime"}, inplace=True)
        data["Datetime"] = pd.to_datetime(data["Datetime"], errors="coerce", utc=True)

        # Convert Datetime to MySQL-compatible format
        data["Datetime"] = data["Datetime"].dt.strftime("%Y-%m-%d %H:%M:%S")

        numeric_cols = ["Open", "High", "Low", "Close", "Volume"]
        for col in numeric_cols:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')

        data["Symbol"] = symbol

        return data[["Symbol", "Datetime"] + [col for col in numeric_cols if col in data.columns]]

    def process_all_symbols(self) -> Dict[str, pd.DataFrame]:
        """
        Fetch, clean, and merge both minute and daily data for all symbols.
        
        Returns:
            Dict[str, pd.DataFrame]: Dictionary containing minute and daily data.
        """
        all_minute_data, all_daily_data = [], []

        for symbol, ticker in self.symbol_map.items():
            print(f"✅ Fetching data for {symbol} ({ticker})...")
            
            minute_data = self.fetch_data(ticker, self.minute_period, self.minute_interval)
            daily_data = self.fetch_data(ticker, self.daily_period, self.daily_interval)

            if not minute_data.empty:
                all_minute_data.append(self.clean_data(minute_data, symbol))
            if not daily_data.empty:
                all_daily_data.append(self.clean_data(daily_data, symbol))

        return {
            "minute": pd.concat(all_minute_data, ignore_index=True) if all_minute_data else pd.DataFrame(),
            "daily": pd.concat(all_daily_data, ignore_index=True) if all_daily_data else pd.DataFrame()
        }

if __name__ == "__main__":
    config_file = "config.yaml"
    data_fetcher = YahooFinanceDataFetcher(config_file)
    dict_df = data_fetcher.process_all_symbols()

    connector_minute = MySQLDataConnector(
        credentials_file="credential_mysql.json", 
        table_name="yfinance_minutes", 
        primary_keys=["Symbol", "Datetime"],
        max_row_key=24*60, 
        sort_col="Datetime",

    )
    connector_daily = MySQLDataConnector(
        credentials_file="credential_mysql.json", 
        table_name="yfinance_daily", 
        primary_keys=["Symbol", "Datetime"],
        max_row_key=365*10, 
        sort_col="Datetime",
    )

    if not dict_df["minute"].empty:
        connector_minute.insert_or_update(dict_df["minute"])
    if not dict_df["daily"].empty:
        connector_daily.insert_or_update(dict_df["daily"])

    connector_minute.close_connection()
    connector_daily.close_connection()

    print("✅ Data inserted successfully.")