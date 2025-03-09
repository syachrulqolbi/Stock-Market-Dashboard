from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
from datetime import datetime
import yaml
from typing import Dict, Any
from mysql_api import MySQLDataConnector

class TradingViewScraper:
    def __init__(self, headless: bool = True, config_path="config.yaml"):
        self.config_file = config_path 
        self.config = self.load_config()
        self.symbol_map = self.config.get("symbols_investing", {})

        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")

        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-infobars")

        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)

        self.technical_headers = ["", "Name", "Hourly", "Daily", "Weekly", "Monthly", "Last Updated"]

    def load_config(self) -> Dict[str, Any]:
        try:
            with open(self.config_file, "r") as file:
                return yaml.safe_load(file)
        except Exception as e:
            raise FileNotFoundError(f"❌ Error loading config file: {e}")

    def close_browser(self):
        self.driver.quit()

    def scrape_investing_technical(self) -> pd.DataFrame:
        url = "https://www.investing.com/indices/major-indices"
        self.driver.get(url)
        wait = WebDriverWait(self.driver, 10)

        try:
            technical_button = wait.until(EC.element_to_be_clickable((By.XPATH, '//*[@id="__next"]/div[2]/div[2]/div[2]/div[1]/div[4]/div[1]/div[1]/button[3]')))
            technical_button.click()
            
            wait.until(EC.presence_of_element_located((By.XPATH, '//table[contains(@class, "datatable-v2_table__93S4Y")]')))
            rows = self.driver.find_elements(By.XPATH, '//table[contains(@class, "datatable-v2_table__93S4Y")]/tbody/tr')

            table_data = []
            for row in rows:
                cells = row.find_elements(By.TAG_NAME, "td")
                row_data = [cell.text.strip() for cell in cells]
                
                if len(row_data) >= len(self.technical_headers) - 1:
                    row_data = row_data[:len(self.technical_headers) - 1]
                    row_data.append(datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S"))
                    table_data.append(row_data)

            if not table_data:
                print("❌ No data found.")
                return pd.DataFrame()

            df = pd.DataFrame(table_data, columns=self.technical_headers)
            df["Symbol"] = df["Name"].map(self.symbol_map).fillna("")
            column_order = ["Symbol", "Name"] + [col for col in df.columns if col not in ["Symbol", "Name"]]
            df = df[column_order]
            del df[""]

            return df

        except Exception as e:
            print(f"❌ Error scraping Investing.com: {e}")
            return pd.DataFrame()

if __name__ == "__main__":
    scraper = TradingViewScraper(config_path="config.yaml")
    df = scraper.scrape_investing_technical()

    if not df.empty:
        connector = MySQLDataConnector(credentials_file='credential_mysql.json', 
                                    table_name='investing_technical', 
                                    primary_keys=['Name'])
        
        connector.insert_or_update(df)
        
        connector.close_connection()
        print("✅ Data inserted successfully.")

    # Close browser session
    scraper.close_browser()
