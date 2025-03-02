import yfinance as yf
import pandas as pd
import yaml
import os
from typing import Dict, Any
from google_sheet_api import GoogleSheetsUploader

class YahooFinanceDataFetcher:
    """A class to fetch, save, and upload historical stock/index data from Yahoo Finance."""
    
    def __init__(self, config_file: str):
        """Initialize the fetcher with a configuration file.
        
        Args:
            config_file (str): Path to the YAML configuration file containing all parameters.
        """
        with open(config_file, "r") as file:
            self.config: Dict[str, Any] = yaml.safe_load(file)
        
        # Extract configuration parameters
        self.symbol_map: Dict[str, str] = self.config.get("symbols_yfinance", {})  # Dictionary of symbols and tickers
        self.minute_period: str = self.config.get("minute_period", "7d")  # Period for minute data
        self.minute_interval: str = self.config.get("minute_interval", "1m")  # Interval for minute data
        self.daily_period: str = self.config.get("daily_period", "10y")  # Period for daily data
        self.daily_interval: str = self.config.get("daily_interval", "1d")  # Interval for daily data
        self.output_dir: str = self.config.get("output_directory", ".")  # Directory to save CSV files
        
        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

    def fetch_data(self, ticker: str, period: str, interval: str) -> pd.DataFrame:
        """Fetch historical data for a given ticker, period, and interval.
        
        Args:
            ticker (str): Yahoo Finance ticker symbol.
            period (str): Duration of historical data to fetch (e.g., "7d", "10y").
            interval (str): Time interval for data (e.g., "1m", "1d").
        
        Returns:
            pd.DataFrame: Fetched historical data.
        """
        try:
            data = yf.download(ticker, period=period, interval=interval)
            if data.empty:
                print(f"No data available for {ticker}.")
            return data
        except Exception as e:
            print(f"Error fetching data for {ticker}: {e}")
            return pd.DataFrame()

    def clean_data(self, data: pd.DataFrame) -> pd.DataFrame:
        """Clean and preprocess the downloaded data.
        
        Args:
            data (pd.DataFrame): Raw data fetched from Yahoo Finance.
        
        Returns:
            pd.DataFrame: Cleaned and formatted data.
        """
        if data.empty:
            return data
        
        # Ensure 'Datetime' or 'Date' is a column instead of an index
        data = data.reset_index()

        # Drop the second level of MultiIndex (Ticker), keeping only relevant column names
        if isinstance(data.columns, pd.MultiIndex):  # Ensure it's a MultiIndex before dropping
            data.columns = data.columns.droplevel(1)

        # Convert 'Datetime' column to proper format if available
        if "Datetime" in data.columns:
            data["Datetime"] = pd.to_datetime(data["Datetime"], errors="coerce", utc=True)

        # Convert numerical columns to float type
        numeric_cols = ["Close", "High", "Low", "Open", "Volume"]
        for col in numeric_cols:
            if col in data.columns:
                data[col] = pd.to_numeric(data[col], errors='coerce')

        return data

    def save_to_csv(self, data: pd.DataFrame, filename: str) -> str:
        """Save a DataFrame to a CSV file and return the file path.
        
        Args:
            data (pd.DataFrame): DataFrame containing historical data.
            filename (str): Name of the output CSV file.
        
        Returns:
            str: File path of the saved CSV file.
        """
        if not data.empty:
            filepath = os.path.join(self.output_dir, filename)
            data.to_csv(filepath, index=False)
            print(f"Saved {filepath}")
            return filepath
        else:
            print(f"No data to save for {filename}")
            return None  # Return None if the file is empty

    def upload_to_google_sheets(self, file_path: str, sheet_name: str) -> None:
        """Upload a CSV file to Google Sheets.
        
        Args:
            file_path (str): Path to the CSV file to upload.
            sheet_name (str): Name of the Google Sheets worksheet.
        """
        if file_path and os.path.exists(file_path):
            uploader = GoogleSheetsUploader(
                credentials_file="credential_google_sheets.json",
                spreadsheet_name="Stock Market Dashboard"
            )
            uploader.upload_to_sheets(file_path, name_sheet=sheet_name)
            print(f"Uploaded {file_path} to Google Sheets as {sheet_name}")
        else:
            print(f"File {file_path} does not exist. Skipping upload.")

    def process_all_symbols(self) -> None:
        """Fetch, clean, save, and upload both minute and daily data for all symbols in the configuration."""
        for symbol, ticker in self.symbol_map.items():
            print(f"Fetching data for {symbol} ({ticker})...")

            # Fetch minute-level and daily-level data
            minute_data = self.fetch_data(ticker, period=self.minute_period, interval=self.minute_interval)
            daily_data = self.fetch_data(ticker, period=self.daily_period, interval=self.daily_interval)

            # Clean data before saving
            if not minute_data.empty:
                minute_data = self.clean_data(minute_data)
                minute_file = self.save_to_csv(minute_data, f"{symbol}_minutes.csv")
                self.upload_to_google_sheets(minute_file, f"{symbol}_minutes")

            if not daily_data.empty:
                daily_data = self.clean_data(daily_data)
                daily_file = self.save_to_csv(daily_data, f"{symbol}_daily.csv")
                self.upload_to_google_sheets(daily_file, f"{symbol}_daily")

if __name__ == "__main__":
    config_file: str = "config.yaml"  # Path to configuration file
    fetcher = YahooFinanceDataFetcher(config_file)  # Instantiate the fetcher
    fetcher.process_all_symbols()  # Start the data fetching process