from google_sheet_api import GoogleSheetsUploader
from transformers import BertTokenizer, BertForSequenceClassification
from transformers import pipeline
import pandas as pd

class NewsSentimentAnalyzer:
    """
    A class to fetch summarized news from Google Sheets and perform sentiment analysis using FinBERT.
    """
    def __init__(self, google_sheets_credentials: str, spreadsheet_name: str):
        """
        Initializes the NewsSentimentAnalyzer class.
        
        :param google_sheets_credentials: Path to Google Sheets API credentials file.
        :param spreadsheet_name: Name of the Google Spreadsheet.
        """
        self.uploader = GoogleSheetsUploader(credentials_file=google_sheets_credentials, spreadsheet_name=spreadsheet_name)
        self.df = self._fetch_news_summaries()
        self.tokenizer, self.model = self._load_finbert()
    
    def _fetch_news_summaries(self) -> pd.DataFrame:
        """
        Fetches summarized news from the 'news_summarize' sheet.
        
        :return: DataFrame containing summarized news.
        """
        df = self.uploader.get_sheet_as_dataframe("news_summarize")
        df["Sentiment"] = ""
        df["Confidence"] = ""
        return df
    
    def _load_finbert(self):
        """
        Loads the FinBERT model and tokenizer.
        
        :return: Tokenizer and model for FinBERT.
        """
        model_name = "ProsusAI/finbert"
        tokenizer = BertTokenizer.from_pretrained(model_name)
        model = BertForSequenceClassification.from_pretrained(model_name)
        return tokenizer, model
    
    def analyze_sentiment(self) -> pd.DataFrame:
        """
        Performs sentiment analysis on the summarized news.
        
        :return: DataFrame with sentiment labels and confidence scores.
        """
        sentiment_pipeline = pipeline("text-classification", model=self.model, tokenizer=self.tokenizer)
        
        for index, row in self.df.iterrows():
            if pd.notna(row["Summary"]):
                result = sentiment_pipeline(row["Summary"], truncation=True, max_length=512)[0]
                self.df.at[index, "Sentiment"] = result["label"]
                self.df.at[index, "Confidence"] = result["score"]
        
        return self.df

if __name__ == "__main__":
    # Initialize analyzer
    analyzer = NewsSentimentAnalyzer(
        google_sheets_credentials="credential_google_sheets.json",
        spreadsheet_name="Stock Market Dashboard"
    )
    
    # Perform sentiment analysis and upload results
    df_results = analyzer.analyze_sentiment()
    df_results.to_csv("news_sentiment.csv", index = False, header = True)
    
    # Upload summarized news to Google Sheets
    uploader = GoogleSheetsUploader(credentials_file="credential_google_sheets.json", spreadsheet_name="Stock Market Dashboard")
    uploader.upload_to_sheets("news_sentiment.csv", name_sheet="news_sentiment")
