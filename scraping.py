from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe

class TradingViewScraper:
    def __init__(self, headless: bool = True):
        """
        Initializes the WebDriver with optimized Chrome options.
        :param headless: Run Chrome in headless mode (default: True)
        """
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")  # Ensures compatibility with latest Chrome versions

        # Optimize browser settings
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-infobars")

        # Initialize WebDriver
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)

        # Symbol mapping
        self.symbol_map = {
            "AUS200": "ASX-XJO", "ESP35": "BME-IBC", "EUSTX50": "TVC-SX5E",
            "FRA40": "EURONEXT-PX1", "GER40": "XETR-DAX", "JPN225": "INDEX-NKY",
            "NAS100": "NASDAQ-NDX", "SPX500": "SPX", "UK100": "FTSE-UKX",
            "US30": "DJ-DJI"
        }

        # Standardized column headers (added 'Last Updated')
        self.headers = ["Pair", "Symbol", "Market cap", "Price", "Change %", "Volume", "Rel Volume",
                        "P/E", "EPS dil", "EPS dil growth", "Div yield %", "Sector", "Analyst Rating", "Last Updated"]

    def close_browser(self):
        """Closes the WebDriver."""
        self.driver.quit()

    def scrape_symbol_component(self, symbol: str):
        """
        Scrapes TradingView for the component data of the given symbol.
        :param symbol: The index symbol to search for.
        :return: Pandas DataFrame with scraped data.
        """
        tradingview_symbol = self.symbol_map.get(symbol, symbol)
        url = f"https://www.tradingview.com/symbols/{tradingview_symbol}/components/"
        self.driver.get(url)
        wait = WebDriverWait(self.driver, 10)

        try:
            button_xpath = '//*[@id="js-category-content"]/div[2]/div/div[2]/div[3]/button'
            
            # Keep clicking until no new rows are added
            while True:
                initial_row_count = len(self.driver.find_elements(By.XPATH, '//*[@id="js-category-content"]//table//tr'))
                
                try:
                    # Wait until the button is clickable
                    load_more_button = wait.until(EC.element_to_be_clickable((By.XPATH, button_xpath)))
                    load_more_button.click()

                    # Wait for new rows to load
                    wait.until(lambda driver: len(driver.find_elements(By.XPATH, '//*[@id="js-category-content"]//table//tr')) > initial_row_count)
                except Exception:
                    break  # No button found or no new rows loaded

            # Extract table data
            rows = self.driver.find_elements(By.XPATH, '//*[@id="js-category-content"]//table//tr')
            table_data = [[cell.text.strip() for cell in row.find_elements(By.TAG_NAME, "td")] for row in rows]
            
            if not table_data or not any(table_data):
                return pd.DataFrame(columns=self.headers)

            # Auto-detect column count and assign matching headers
            max_cols = max(len(row) for row in table_data)
            df_headers = self.headers[1:max_cols+1]  # Exclude "Pair" since it is added separately

            # Construct DataFrame
            df = pd.DataFrame(table_data, columns=df_headers)
            df.insert(0, "Pair", symbol)  # Add the "Pair" column

            # Add last updated timestamp (UTC format)
            df["Last Updated"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            # Reorder & fill missing columns
            df = df.reindex(columns=self.headers, fill_value="N/A")

            return df

        except Exception as e:
            print(f"Error scraping {symbol}: {e}")
            return pd.DataFrame(columns=self.headers)

    def scrape_all(self, csv_file="tradingview_indices_components.csv"):
        """Scrapes all symbols and consolidates results into a CSV file."""
        all_data = [self.scrape_symbol_component(symbol) for symbol in self.symbol_map]

        if all_data:
            df_final = pd.concat(all_data, ignore_index=True)
            df_final = df_final.loc[~(df_final["Symbol"].isnull())]
            df_final.to_csv(csv_file, index=False)
            print(f"✅ Data saved to {csv_file}")

        self.close_browser()
        return csv_file


class GoogleSheetsUploader:
    def __init__(self, credentials_file, spreadsheet_name):
        self.credentials_file = credentials_file
        self.spreadsheet_name = spreadsheet_name
        self.scopes = [
            "https://spreadsheets.google.com/feeds",
            "https://www.googleapis.com/auth/drive"
        ]
        self.client = self.authenticate()
        self.sheet = self.get_sheet()
    
    def authenticate(self):
        creds = Credentials.from_service_account_file(self.credentials_file).with_scopes(self.scopes)
        return gspread.authorize(creds)
    
    def get_sheet(self):
        try:
            return self.client.open(self.spreadsheet_name).sheet1  # Select the first worksheet
        except gspread.exceptions.SpreadsheetNotFound:
            raise FileNotFoundError(f"Spreadsheet '{self.spreadsheet_name}' not found. Please check the name or ID.")
    
    def read_csv(self, csv_file):
        try:
            return pd.read_csv(csv_file)
        except FileNotFoundError:
            raise FileNotFoundError(f"CSV file '{csv_file}' not found.")
        except pd.errors.EmptyDataError:
            raise ValueError(f"CSV file '{csv_file}' is empty.")
        except pd.errors.ParserError:
            raise ValueError(f"CSV file '{csv_file}' contains parsing errors.")
    
    def upload_to_sheets(self, csv_file):
        df = self.read_csv(csv_file)
        set_with_dataframe(self.sheet, df)
        print("✅ DataFrame successfully uploaded to Google Sheets!")


if __name__ == "__main__":
    # Scrape data from TradingView
    scraper = TradingViewScraper()
    csv_file = scraper.scrape_all()

    # Upload data to Google Sheets
    uploader = GoogleSheetsUploader(
        credentials_file="credential_google_sheets.json",
        spreadsheet_name="Stock Market Dashboard"
    )
    uploader.upload_to_sheets(csv_file)