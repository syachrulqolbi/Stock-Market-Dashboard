import yfinance as yf
import yaml
import os
import pandas as pd
from typing import Dict, List, Any
from google_sheet_api import GoogleSheetsUploader

class YahooFinanceNewsFetcher:
    """Class to fetch and save the latest news for stock indices from Yahoo Finance based on a YAML configuration file."""

    def __init__(self, config_file: str):
        """
        Initializes the YahooFinanceNewsFetcher by loading symbols and output directory from YAML config file.

        Args:
            config_file (str): Path to the YAML configuration file.
        """
        self.config_file = config_file
        self.config = self.load_config()

        # Extract Yahoo Finance symbols
        self.symbols = self.config.get("symbols_yfinance", {})
        if not self.symbols:
            raise ValueError("No symbols found in config.yaml under 'symbols_yfinance'.")

        # Extract output directory and create it if not exists
        self.output_dir = self.config.get("output_directory", ".")
        os.makedirs(self.output_dir, exist_ok=True)

    def load_config(self) -> Dict[str, Any]:
        """
        Loads the YAML configuration file.

        Returns:
            Dict[str, Any]: Parsed YAML configuration.
        """
        try:
            with open(self.config_file, "r") as file:
                return yaml.safe_load(file)
        except Exception as e:
            raise FileNotFoundError(f"Error loading config file: {e}")

    def fetch_news(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Fetches the latest news for a given stock index symbol.

        Args:
            symbol (str): Yahoo Finance ticker symbol.

        Returns:
            List[Dict[str, Any]]: List of news articles.
        """
        try:
            stock = yf.Ticker(symbol)
            news = stock.news  # Fetch news articles from Yahoo Finance

            if not news:
                print(f"No news available for {symbol}.")
                return []

            # Filter news to ensure valid structure
            formatted_news = []
            for article in news[:10]:  # Only take the top 5 latest articles
                formatted_news.append({
                    "Title": article["content"].get("title", ""),
                    "Summary": article["content"].get("summary", ""),
                    "URL": article["content"]["clickThroughUrl"].get("url", ""),
                    "Published": article["content"].get("pubDate", "")
                })

            return formatted_news
        except Exception as e:
            print(f"Error fetching news for {symbol}: {e}")
            return []

    def save_news_to_csv(self, symbol: str, news: List[Dict[str, Any]]) -> str:
        """
        Saves the fetched news to a CSV file in the specified output directory.

        Args:
            symbol (str): Yahoo Finance ticker symbol.
            news (List[Dict[str, Any]]): List of news articles.

        Returns:
            str: The saved file path.
        """
        if not news:
            print(f"No news to save for {symbol}.")
            return ""

        # Convert to DataFrame
        df = pd.DataFrame(news)

        # Define file path
        filename = f"{symbol}_news.csv"
        filepath = os.path.join(self.output_dir, filename)

        # Save to CSV
        df.to_csv(filepath, index=False)
        print(f"✅ Saved news for {symbol} to {filepath}")

        return filepath
    
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

    def fetch_all_news(self) -> List[str]:
        """
        Fetches, displays, and saves news for all symbols listed in the YAML configuration.

        Returns:
            List[str]: List of saved CSV file paths.
        """
        for name, symbol in self.symbols.items():
            print(f"\n📩 Fetching news for {name} ({symbol})...")
            news = self.fetch_news(symbol)
            csv_path = self.save_news_to_csv(name, news)
            print(csv_path)
            self.upload_to_google_sheets(csv_path, f"{name}_news")


if __name__ == "__main__":
    config_file = "config.yaml"  # Path to the configuration file
    news_fetcher = YahooFinanceNewsFetcher(config_file)
    news_fetcher.fetch_all_news()  # Fetch, display, and save news for all symbols