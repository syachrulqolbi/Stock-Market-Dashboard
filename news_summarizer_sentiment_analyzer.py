import yaml
import json
import logging
import pandas as pd
import google.generativeai as genai
from transformers import BertTokenizer, BertForSequenceClassification, pipeline
from google_sheet_api import GoogleSheetsUploader

# Configure logging with timestamp and log level
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class NewsProcessor:
    def __init__(self, config_path: str, gemini_credentials: str, google_sheets_credentials: str, spreadsheet_name: str):
        """
        Initialize the NewsProcessor class.
        Loads configuration, API keys, and necessary models.
        """
        self.config_path = config_path
        self.api_key = self._load_gemini_api_key(gemini_credentials)
        self.credentials_file = google_sheets_credentials
        self.spreadsheet_name = spreadsheet_name
        self.symbols = self._load_symbols()
        self.uploader = GoogleSheetsUploader(credentials_file=self.credentials_file, spreadsheet_name=self.spreadsheet_name)
        self.tokenizer, self.model = self._load_finbert()
        genai.configure(api_key=self.api_key)

    def _load_symbols(self) -> list:
        """Load trading symbols from the YAML configuration file."""
        try:
            with open(self.config_path, "r") as file:
                config = yaml.safe_load(file)
            return list(config.get("symbols_tradingview", {}).keys())
        except Exception as e:
            logging.error(f"Error loading config file: {e}")
            return []

    def _load_gemini_api_key(self, gemini_credentials: str) -> str:
        """Load the Gemini API key from the credentials JSON file."""
        try:
            with open(gemini_credentials, "r") as file:
                credentials = json.load(file)
            return credentials.get("api_key", "")
        except Exception as e:
            logging.error(f"Error loading Gemini API key: {e}")
            return ""

    def _load_finbert(self):
        """Load the FinBERT model and tokenizer for sentiment analysis."""
        model_name = "ProsusAI/finbert"
        tokenizer = BertTokenizer.from_pretrained(model_name)
        model = BertForSequenceClassification.from_pretrained(model_name)
        return tokenizer, model

    def summarize_news(self) -> pd.DataFrame:
        """
        Summarize financial news articles for each stock symbol using Gemini AI.
        Returns a DataFrame with summarized news and last updated timestamps.
        """
        df = self.uploader.get_sheet_as_dataframe("yfinance_news")
        latest_dates = df.groupby("Symbol")["Published"].max().reset_index().rename(columns={"Published": "Last Updated"})

        try:
            df_existing = self.uploader.get_sheet_as_dataframe("news_summary_sentiment_analysis")
        except Exception as e:
            logging.warning(f"Could not load existing summary sheet: {e}")
            df_existing = pd.DataFrame()
        
        results = []
        for symbol in df["Symbol"].unique():
            latest_published = latest_dates.loc[latest_dates["Symbol"] == symbol, "Last Updated"].values[0]
            
            if not df_existing.empty and symbol in df_existing["Symbol"].values:
                last_update_existing = df_existing.loc[df_existing["Symbol"] == symbol, "Last Updated"].values[0]
                if last_update_existing == latest_published:
                    logging.info(f"Skipping {symbol}: No new updates.")
                    continue
            
            symbol_df = df[df["Symbol"] == symbol]
            news_summaries = "\n".join(symbol_df["Summary"].dropna().tolist()).strip()
            
            if not news_summaries:
                logging.warning(f"Skipping {symbol}: No valid news summaries.")
                results.append({"Symbol": symbol, "Summary": "", "Last Updated": latest_published})
                continue
            
            try:
                model = genai.GenerativeModel("gemini-2.0-flash")
                response = model.generate_content(f"Generate a concise 1-paragraph summary of the following news articles:\n{news_summaries}")
                
                summary_text = response.text.strip() if response.text else "No summary available"
                results.append({"Symbol": symbol, "Summary": summary_text, "Last Updated": latest_published})
                logging.info(f"Successfully summarized news for {symbol}")
            except Exception as e:
                logging.error(f"Error summarizing news for {symbol}: {e}")
                results.append({"Symbol": symbol, "Summary": "", "Last Updated": latest_published})
        
        return pd.DataFrame(results)

    def analyze_sentiment(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Perform sentiment analysis on the summarized news using FinBERT.
        Adds sentiment label and confidence score to the DataFrame.
        """
        df["Sentiment"] = ""
        df["Confidence"] = ""
        sentiment_pipeline = pipeline("text-classification", model=self.model, tokenizer=self.tokenizer)
        
        for index, row in df.iterrows():
            if pd.notna(row["Summary"]):
                result = sentiment_pipeline(row["Summary"], truncation=True, max_length=512)[0]
                df.at[index, "Sentiment"] = result["label"]
                df.at[index, "Confidence"] = result["score"]
        
        return df

if __name__ == "__main__":
    processor = NewsProcessor(
        config_path="config.yaml",
        gemini_credentials="credential_gemini.json",
        google_sheets_credentials="credential_google_sheets.json",
        spreadsheet_name="Stock Market Dashboard"
    )
    
    df_summary = processor.summarize_news()
    
    if not df_summary.empty:
        df_combined = processor.analyze_sentiment(df_summary)
        output_file = "news_summary_sentiment_analysis.csv"
        df_combined.to_csv(output_file, index=False)
        processor.uploader.upload_to_sheets(output_file, name_sheet="news_summary_sentiment_analysis")
        logging.info("News summarization and sentiment analysis completed and uploaded.")
    else:
        logging.info("No new updates for any symbols. No processing required.")