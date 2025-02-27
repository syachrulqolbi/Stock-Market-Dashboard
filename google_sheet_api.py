import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
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

    def upload_to_sheets(self, csv_file, name_sheet):
        df = self.read_csv(csv_file)
        sheet = self.get_sheet(name_sheet)  # Dynamically get the correct sheet
        set_with_dataframe(sheet, df)
        print(f"✅ DataFrame successfully uploaded to Google Sheets: {name_sheet}!")
