import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe, get_as_dataframe
from gspread.exceptions import SpreadsheetNotFound


class GoogleSheetsUploader:
    def __init__(self, credentials_file, spreadsheet_name):
        self.credentials_file = credentials_file
        self.spreadsheet_name = spreadsheet_name
        self.scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        self.client = self.authenticate()

    def authenticate(self):
        creds = Credentials.from_service_account_file(self.credentials_file).with_scopes(self.scopes)
        return gspread.authorize(creds)

    def read_csv(self, csv_file):
        try:
            return pd.read_csv(csv_file)
        except FileNotFoundError:
            raise FileNotFoundError(f"CSV file '{csv_file}' not found.")
        except pd.errors.EmptyDataError:
            raise ValueError(f"CSV file '{csv_file}' is empty.")
        except pd.errors.ParserError:
            raise ValueError(f"CSV file '{csv_file}' contains parsing errors.")

    def get_sheet(self, name_sheet):
        try:
            spreadsheet = self.client.open(self.spreadsheet_name)
            if name_sheet not in [ws.title for ws in spreadsheet.worksheets()]:
                return spreadsheet.add_worksheet(title=name_sheet, rows="100", cols="20")  # Create a new sheet if not found
            else:
                return spreadsheet.worksheet(name_sheet)  # Use dynamic worksheet selection
        except SpreadsheetNotFound:
            raise FileNotFoundError(f"Spreadsheet '{self.spreadsheet_name}' not found. Please check the name or ID.")
        except gspread.exceptions.WorksheetNotFound:
            raise FileNotFoundError(f"Worksheet '{name_sheet}' not found in the spreadsheet.")

    def clear_sheet(self, sheet):
        """Clears all existing data from the sheet."""
        sheet.clear()
        print(f"✅ Cleared all data from sheet: {sheet.title}")

    def upload_to_sheets(self, csv_file, name_sheet):
        df = self.read_csv(csv_file)
        sheet = self.get_sheet(name_sheet)  # Dynamically get the correct sheet
        self.clear_sheet(sheet)  # Clear existing data before uploading new data
        set_with_dataframe(sheet, df)
        print(f"✅ DataFrame successfully uploaded to Google Sheets: {name_sheet}!")

    def get_sheet_as_dataframe(self, name_sheet):
        """
        Retrieve Google Sheets data as a Pandas DataFrame.
        """
        try:
            sheet = self.get_sheet(name_sheet)
            df = get_as_dataframe(sheet, evaluate_formulas=True)  # Convert Google Sheets to DataFrame
            print(f"✅ Successfully retrieved data from '{name_sheet}' as a DataFrame.")
            return df
        except Exception as e:
            raise RuntimeError(f"Failed to retrieve data from Google Sheets: {e}")