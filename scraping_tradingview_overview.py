from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import numpy as np
from datetime import datetime
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from gspread.exceptions import SpreadsheetNotFound
from google_sheet_api import GoogleSheetsUploader

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
        self.overview_headers = ["Pair", "Symbol", "Market cap", "Price", "Change %", "Volume", "Rel Volume",
                        "P/E", "EPS dil", "EPS dil growth", "Div yield %", "Sector", "Analyst Rating", "Last Updated"]

    def close_browser(self):
        """Closes the WebDriver."""
        self.driver.quit()

    def scrape_tradingview_overview(self, csv_file="tradingview_indices_components.csv"):
        """
        Scrapes TradingView for component data of all symbols in the symbol map.
        
        Args:
            csv_file (str): The filename where the scraped data will be saved.
        
        Returns:
            str: The name of the saved CSV file.
        """
        all_data = []
        
        for symbol in self.symbol_map:
            tradingview_symbol = self.symbol_map.get(symbol, symbol)
            url = f"https://www.tradingview.com/symbols/{tradingview_symbol}/components/"
            self.driver.get(url)
            wait = WebDriverWait(self.driver, 5)

            try:
                button_xpath = '//*[@id="js-category-content"]/div[2]/div/div[2]/div[3]/button'
                while True:
                    initial_row_count = len(self.driver.find_elements(By.XPATH, '//*[@id="js-category-content"]//table//tr'))
                    try:
                        load_more_button = wait.until(EC.element_to_be_clickable((By.XPATH, button_xpath)))
                        load_more_button.click()
                        wait.until(lambda driver: len(driver.find_elements(By.XPATH, '//*[@id="js-category-content"]//table//tr')) > initial_row_count)
                    except Exception:
                        break  # No button found or no new rows loaded

                rows = self.driver.find_elements(By.XPATH, '//*[@id="js-category-content"]//table//tr')
                table_data = [[cell.text.strip() for cell in row.find_elements(By.TAG_NAME, "td")] for row in rows]
                
                if not table_data or not any(table_data):
                    continue

                max_cols = max(len(row) for row in table_data)
                df_headers = self.overview_headers[1:max_cols+1]
                df = pd.DataFrame(table_data, columns=df_headers)
                df.insert(0, "Pair", symbol)
                df["Last Updated"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                df = df.reindex(columns=self.overview_headers, fill_value="")
                all_data.append(df)
            
            except Exception as e:
                print(f"Error scraping {symbol}: {e}")
                continue

        if all_data:
            df_final = pd.concat(all_data, ignore_index=True)
            df_final.dropna(subset=["Symbol"], inplace=True)
            df_final.replace("—", "", inplace=True)
            
            columns_to_clean = ["Market cap", "Price", "EPS dil"]
            for column in columns_to_clean:
                df_final[column] = df_final[column].str.replace(r"[^\d.\sKMB]", "", regex=True)
            
            for col in ["EPS dil growth", "Change %"]:
                df_final[col] = df_final[col].str.replace("+", "", regex=False)
            
            def convert_volume(value):
                if isinstance(value, str):
                    value = value.replace(",", "")
                    if "K" in value:
                        return float(value.replace("K", "")) * 1000
                    elif "M" in value:
                        return float(value.replace("M", "")) * 1000000
                    elif "B" in value:
                        return float(value.replace("B", "")) * 1000000000
                    return float(value)
                return np.nan
            
            df_final["Volume"] = df_final["Volume"].apply(convert_volume)
            df_final["Market cap"] = df_final["Market cap"].apply(convert_volume)
            df_final[['Symbol', 'Description']] = df_final["Symbol"].str.replace(r'\n(D|REIT|P|DR)$', '', regex=True).str.split("\n", n=1, expand=True)
            column_order = ["Symbol", "Description"] + [col for col in df_final.columns if col not in ["Symbol", "Description"]]
            df_final = df_final[column_order]


            df_final.to_csv(csv_file, index=False)
            print(f"✅ Data saved to {csv_file}")

        return csv_file
    
if __name__ == "__main__":
    scraper = TradingViewScraper()
    csv_file = scraper.scrape_tradingview_overview(csv_file="tradingview_indices_components.csv")

    # Upload data to Google Sheets
    uploader = GoogleSheetsUploader(
        credentials_file="credential_google_sheets.json",
        spreadsheet_name="Stock Market Dashboard"
    )

    uploader.upload_to_sheets(csv_file, name_sheet="tradingview_overview")