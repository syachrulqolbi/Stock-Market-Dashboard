import yfinance as yf
import yaml
import pandas as pd
from typing import Dict, List, Any, Optional
from pathlib import Path
from google_sheet_api import GoogleSheetsUploader


class YahooFinanceNewsFetcher:
    """
    Fetch and save the latest news for stock indices from Yahoo Finance based on a YAML configuration file.
    """

    def __init__(self, config_file: str):
        """
        Initialize the news fetcher by loading stock symbols and output directory from the configuration file.
        :param config_file: Path to the YAML configuration file.
        """
        self.config = self._load_config(config_file)
        self.symbols = self.config.get("symbols_yfinance", {})
        
        if not self.symbols:
            raise ValueError("No symbols found in config.yaml under 'symbols_yfinance'.")
        
        self.output_dir = Path(self.config.get("output_directory", "output"))
        self.output_dir.mkdir(parents=True, exist_ok=True)

    @staticmethod
    def _load_config(config_file: str) -> Dict[str, Any]:
        """
        Load and parse the YAML configuration file.
        :param config_file: Path to the YAML configuration file.
        :return: Parsed configuration dictionary.
        """
        try:
            with open(config_file, "r") as file:
                return yaml.safe_load(file)
        except (FileNotFoundError, yaml.YAMLError) as e:
            raise ValueError(f"Error loading config file {config_file}: {e}")

    @staticmethod
    def fetch_news(symbol: str) -> List[Dict[str, Any]]:
        """
        Fetch the latest news for a given stock symbol from Yahoo Finance.
        :param symbol: Stock symbol to fetch news for.
        :return: List of dictionaries containing news articles.
        """
        try:
            stock = yf.Ticker(symbol)
            news = stock.news or []  # Fetch news articles, defaulting to empty list if none found

            return [
                {
                    "Symbol": symbol,
                    "Title": article.get("title", ""),
                    "Summary": article.get("summary", ""),
                    "URL": article.get("link", ""),
                    "Published": article.get("providerPublishTime", "")
                }
                for article in news[:10]  # Limit to the top 10 articles
            ]
        except Exception as e:
            print(f"⚠️ Error fetching news for {symbol}: {e}")
            return [{"Symbol": symbol, "Title": "", "Summary": "", "URL": "", "Published": ""}]

    def fetch_all_news(self) -> pd.DataFrame:
        """
        Fetch news for all configured stock symbols.
        :return: DataFrame containing news articles.
        """
        all_news = [news for symbol in self.symbols.values() for news in self.fetch_news(symbol)]
        return pd.DataFrame(all_news)

    def save_news_to_csv(self, news_df: pd.DataFrame) -> Optional[Path]:
        """
        Save the fetched news DataFrame to a CSV file.
        :param news_df: DataFrame containing news data.
        :return: Path to the saved CSV file or None if no data is available.
        """
        if news_df.empty:
            print("⚠️ No news data to save.")
            return None

        # Reverse symbol mapping for user-friendly representation
        reverse_symbols = {v: k for k, v in self.symbols.items()}
        news_df["Symbol"] = news_df["Symbol"].map(lambda x: reverse_symbols.get(x, x))

        filepath = self.output_dir / "yfinance_news.csv"
        news_df.drop_duplicates().to_csv(filepath, index=False)
        print(f"✅ News saved to {filepath}")
        return filepath

    def upload_to_google_sheets(self, file_path: Optional[Path], sheet_name: str):
        """
        Upload the saved CSV file to Google Sheets.
        :param file_path: Path to the CSV file.
        :param sheet_name: Target Google Sheet name.
        """
        if file_path and file_path.exists():
            uploader = GoogleSheetsUploader(
                credentials_file="credential_google_sheets.json",
                spreadsheet_name="Stock Market Dashboard"
            )
            uploader.upload_to_sheets(str(file_path), name_sheet=sheet_name)
            print(f"✅ Uploaded {file_path} to Google Sheets as {sheet_name}")
        else:
            print(f"⚠️ File {file_path} does not exist. Skipping upload.")


if __name__ == "__main__":
    config_file = "config.yaml"
    news_fetcher = YahooFinanceNewsFetcher(config_file)
    news_df = news_fetcher.fetch_all_news()

    uploader = GoogleSheetsUploader(
        credentials_file="credential_google_sheets.json",
        spreadsheet_name="Stock Market Dashboard"
    )

    try:
        old_news_df = uploader.get_sheet_as_dataframe("yfinance_news")
    except Exception as e:
        print(f"⚠️ Could not fetch existing data from Google Sheets: {e}")
        old_news_df = pd.DataFrame()

    # Merge new and old data, handling duplicates
    if not old_news_df.empty:
        merged_news_df = pd.concat([old_news_df, news_df], ignore_index=True)
        merged_news_df["Published"] = pd.to_datetime(merged_news_df["Published"], errors="coerce")
        merged_news_df = merged_news_df.drop_duplicates().sort_values(by=["Symbol", "Published"], ascending=[True, False])
        
        # Keep only the latest 10 news items per symbol
        merged_news_df = merged_news_df.groupby("Symbol").head(10).reset_index(drop=True)
    else:
        merged_news_df = news_df

    # Save and upload if news data exists
    if not merged_news_df.empty:
        merged_news_df = merged_news_df[merged_news_df["Title"] != ""]
        file_path = news_fetcher.save_news_to_csv(merged_news_df)
        if file_path:
            news_fetcher.upload_to_google_sheets(file_path, "yfinance_news")