from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from datetime import datetime
from google_sheet_api import GoogleSheetsUploader
import time


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
            "S&P/ASX 200": "AUS200",
            "IBEX 35": "ESP35",
            "Euro Stoxx 50": "EUSTX50",
            "CAC 40": "FRA40",
            "DAX": "GER40",
            "Nikkei 225": "JPN225",
            "Nasdaq": "NAS100",
            "S&P 500": "SPX500",
            "FTSE 100": "UK100",
            "Dow Jones": "US30"
            }

        # Standardized column headers (added 'Last Updated')
        self.technical_headers = ["", "Name", "Hourly", "Daily", "Weekly", "Monthly"]

    def close_browser(self):
        """Closes the WebDriver."""
        self.driver.quit()

    def scrape_investing_technical(self, csv_file="investing_indices_technical.csv"):
        """
        Scrapes Investing for technical data of all symbols in the web.
        
        Args:
            csv_file (str): The filename where the scraped data will be saved.
        
        Returns:
            str: The name of the saved CSV file.
        """
        
        url = "https://www.investing.com/indices/major-indices"
        self.driver.get(url)
        wait = WebDriverWait(self.driver, 10)

        try:
            technical_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/div[2]/div[1]/div[4]/div[1]/div[1]/button[3]')))
            technical_button.click()
            # Wait for the table to load using XPath
            wait.until(EC.presence_of_element_located((By.XPATH, '//table[contains(@class, "datatable-v2_table__93S4Y")]')))

            # Select the table rows
            rows = self.driver.find_elements(By.XPATH, '//table[contains(@class, "datatable-v2_table__93S4Y")]/tbody/tr')

            table_data = []
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                if len(cells) >= len(self.technical_headers):  # Ensure all required columns exist
                    table_data.append([cell.text.strip() for cell in cells[:len(self.technical_headers)]])

            # Ensure data is available before proceeding
            if not table_data:
                print("No data found.")
                return None

            df = pd.DataFrame(table_data, columns=self.technical_headers)

            # Ensure correct column ordering
            df = df.reindex(columns=self.technical_headers, fill_value="")
            df = df[self.technical_headers[1:]]
            df["Last Updated"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")

            # Apply mapping to create Symbol column
            df["Symbol"] = df["Name"].map(self.symbol_map).fillna("")

            column_order = ["Symbol", "Name"] + [col for col in df.columns if col not in ["Symbol", "Name"]]
            df = df[column_order]
            
            df.to_csv(csv_file, index=False)
            print(f"✅ Data saved to {csv_file}")

            return csv_file

        except Exception as e:
            print(f"Error scraping Investing.com: {e}")
            return None


if __name__ == "__main__":
    scraper = TradingViewScraper()
    csv_file = scraper.scrape_investing_technical(csv_file="investing_indices_technical.csv")

    if csv_file:
        # Upload data to Google Sheets
        uploader = GoogleSheetsUploader(
            credentials_file="credential_google_sheets.json",
            spreadsheet_name="Stock Market Dashboard"
        )
        uploader.upload_to_sheets(csv_file, name_sheet="investing_technical")

    # Close browser session
    scraper.close_browser()