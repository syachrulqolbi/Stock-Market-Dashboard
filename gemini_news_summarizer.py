import yaml
import json
import logging
import google.generativeai as genai
import pandas as pd
from google_sheet_api import GoogleSheetsUploader

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class NewsSummarizer:
    """
    A class to summarize stock market news from Google Sheets using Google Gemini AI.
    """
    def __init__(self, config_path: str, gemini_credentials: str, google_sheets_credentials: str, spreadsheet_name: str):
        """
        Initializes the NewsSummarizer class.
        """
        self.config_path = config_path
        self.api_key = self._load_gemini_api_key(gemini_credentials)
        self.credentials_file = google_sheets_credentials
        self.spreadsheet_name = spreadsheet_name
        self.symbols = self._load_symbols()
        self.uploader = GoogleSheetsUploader(credentials_file=self.credentials_file, spreadsheet_name=self.spreadsheet_name)

        # Configure Gemini AI
        genai.configure(api_key=self.api_key)

    def _load_symbols(self) -> list:
        """
        Loads symbols from the YAML configuration file.
        """
        try:
            with open(self.config_path, "r") as file:
                config = yaml.safe_load(file)
            return list(config.get("symbols_tradingview", {}).keys())
        except Exception as e:
            logging.error(f"Error loading config file: {e}")
            return []

    def _load_gemini_api_key(self, gemini_credentials: str) -> str:
        """
        Loads the Google Gemini API key from a JSON file.
        """
        try:
            with open(gemini_credentials, "r") as file:
                credentials = json.load(file)
            return credentials.get("api_key", "")
        except Exception as e:
            logging.error(f"Error loading Gemini API key: {e}")
            return ""

    def summarize_news(self) -> pd.DataFrame:
        """
        Fetches news from Google Sheets and summarizes it using Gemini AI.
        """
        results = []

        for symbol in self.symbols:
            try:
                df = self.uploader.get_sheet_as_dataframe(f"{symbol}_news")

                if df.empty or "Summary" not in df.columns:
                    results.append({"Symbol": symbol, "Summary": ""})
                    logging.warning(f"Skipping {symbol}: No data available.")
                    continue

                news_summaries = "\n".join(df["Summary"].dropna().tolist()).strip()
                if not news_summaries:
                    logging.warning(f"Skipping {symbol}: No valid news summaries.")
                    continue

                # Generate a summarized version of all news
                model = genai.GenerativeModel("gemini-2.0-flash")
                response = model.generate_content(
                    f"Summarize the following news articles concisely in a short paragraph:\n{news_summaries}"
                )

                summary_text = response.text.strip() if response.text else "No summary available"
                results.append({"Symbol": symbol, "Summary": summary_text})
                logging.info(f"Successfully summarized news for {symbol}")

            except Exception as e:
                logging.error(f"Error processing {symbol}: {e}")

        return pd.DataFrame(results)

if __name__ == "__main__":
    # Initialize NewsSummarizer
    summarizer = NewsSummarizer(
        config_path="config.yaml",
        gemini_credentials="credential_gemini.json",
        google_sheets_credentials="credential_google_sheets.json",
        spreadsheet_name="Stock Market Dashboard"
    )

    # Summarize news and export results
    df = summarizer.summarize_news()
    output_file = "news_summarize.csv"
    df.to_csv(output_file, index=False, header=True)

    # Upload summarized news to Google Sheets
    uploader = GoogleSheetsUploader(credentials_file="credential_google_sheets.json", spreadsheet_name="Stock Market Dashboard")
    uploader.upload_to_sheets(output_file, name_sheet="news_summarize")

    logging.info("News summarization and upload completed.")