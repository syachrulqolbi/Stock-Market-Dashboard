import yfinance as yf
import yaml
import pandas as pd
from typing import Dict, List, Any
from mysql_api import MySQLDataConnector


class YahooFinanceNewsFetcher:
    """
    Fetches the latest news for stock indices from Yahoo Finance based on a YAML configuration file.
    """

    def __init__(self, config_file: str):
        """
        Initializes the news fetcher by loading stock symbols from the configuration file.
        
        :param config_file: Path to the YAML configuration file.
        """
        self.config = self._load_config(config_file)
        self.symbols = self.config.get("symbols_yfinance", {})
        self.symbol_lookup = {v: k for k, v in self.symbols.items()}

        if not self.symbols:
            raise ValueError("❌ No symbols found in config.yaml under 'symbols_yfinance'.")

    @staticmethod
    def _load_config(config_file: str) -> Dict[str, Any]:
        """
        Loads and parses the YAML configuration file.
        
        :param config_file: Path to the YAML configuration file.
        :return: Parsed configuration dictionary.
        """
        try:
            with open(config_file, "r") as file:
                return yaml.safe_load(file)
        except (FileNotFoundError, yaml.YAMLError) as e:
            raise ValueError(f"❌ Error loading config file {config_file}: {e}")

    def fetch_news(self, symbol: str) -> List[Dict[str, Any]]:
        """
        Fetches the latest news for a given stock symbol from Yahoo Finance.
        
        :param symbol: Stock symbol to fetch news for.
        :return: List of dictionaries containing news articles.
        """
        try:
            stock = yf.Ticker(symbol)
            news = stock.news or []

            return [
                {
                    "Symbol": self.symbol_lookup.get(symbol, symbol),
                    "Title": article["content"].get("title", ""),
                    "Summary": article["content"].get("summary", ""),
                    "URL": article["content"]["clickThroughUrl"].get("url", ""),
                    "Datetime": article["content"].get("pubDate", "")
                }
                for article in news
            ]
        except Exception as e:
            print(f"❌ Error fetching news for {symbol}: {e}")
            return [{"Symbol": self.symbol_lookup.get(symbol, symbol), "Title": "", "Summary": "", "URL": "", "Datetime": ""}]

    def fetch_all_news(self) -> pd.DataFrame:
        """
        Fetches news for all configured stock symbols and compiles them into a DataFrame.
        
        :return: DataFrame containing news articles.
        """
        all_news = [news for symbol in self.symbols.values() for news in self.fetch_news(symbol)]
        df = pd.DataFrame(all_news)
        df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")

        return df


if __name__ == "__main__":
    config_file = "config.yaml"
    news_fetcher = YahooFinanceNewsFetcher(config_file)
    news_df = news_fetcher.fetch_all_news()

    connector = MySQLDataConnector(
        credentials_file="credential_mysql.json",
        table_name="yfinance_news",
        primary_keys=["Symbol", "URL"],
        max_row_key=10,
        sort_col="Datetime",
    )

    connector.insert_or_update(news_df)
    connector.close_connection()
    
    print("✅ Data insertion completed.")