import yaml
import os
import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, Any
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from mysql_api import MySQLDataConnector

class TradingViewScraper:
    """
    Scrapes stock market data from TradingView and saves it in a structured format.
    """
    def __init__(self, headless: bool = True, config_path: str = "config.yaml"):
        self.config_file = config_path
        self.config = self.load_config()
        self.symbol_map = self.config.get("symbols_tradingview", {})
        
        # Configure Chrome options
        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_argument("--window-size=1920x1080")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        
        # Initialize Selenium WebDriver
        service = Service(ChromeDriverManager().install())
        self.driver = webdriver.Chrome(service=service, options=chrome_options)
        self.wait = WebDriverWait(self.driver, 10)
        
        # Define expected table headers
        self.overview_headers = [
            "Pair", "Symbol", "Market cap", "Price", "Change %", "Volume", "Rel Volume",
            "P/E", "EPS dil", "EPS dil growth", "Div yield %", "Sector", "Analyst Rating", "Last Updated"
        ]

    def load_config(self) -> Dict[str, Any]:
        """Load configuration file containing symbols to scrape."""
        try:
            with open(self.config_file, "r") as file:
                return yaml.safe_load(file)
        except Exception as e:
            raise FileNotFoundError(f"❌ Error loading config file: {e}")

    def close_browser(self):
        """Closes the Selenium WebDriver instance."""
        self.driver.quit()
    
    def scrape_tradingview_overview(self) -> pd.DataFrame:
        """
        Scrapes TradingView overview table for specified symbols and returns structured data.
        """
        all_data = []
        for symbol, tradingview_symbol in self.symbol_map.items():
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
                        break  # No more "Load More" button to click

                # Extract table data
                rows = self.driver.find_elements(By.XPATH, '//*[@id="js-category-content"]//table//tr')
                table_data = [[cell.text.strip() for cell in row.find_elements(By.TAG_NAME, "td")] for row in rows]
                
                if not table_data or not any(table_data):
                    continue
                
                # Structure DataFrame
                max_cols = max(len(row) for row in table_data)
                df_headers = self.overview_headers[1:max_cols+1]
                df = pd.DataFrame(table_data, columns=df_headers)
                df.insert(0, "Pair", symbol)
                df["Last Updated"] = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
                df = df.reindex(columns=self.overview_headers, fill_value="")
                all_data.append(df)
            
            except Exception as e:
                print(f"❌ Error scraping {symbol}: {e}")
                continue

        if all_data:
            df_final = pd.concat(all_data, ignore_index=True)
            df_final.dropna(subset=["Symbol"], inplace=True)
            df_final.replace("—", "", inplace=True)
            
            # Convert numerical values
            num_columns = ["Market cap", "Price", "EPS dil"]
            df_final[num_columns] = df_final[num_columns].replace(r"[^\d.\sKMB]", "", regex=True).apply(pd.to_numeric, errors='coerce')
            
            for col in ["EPS dil growth", "Change %", "Div yield %"]:
                df_final[col] = df_final[col].str.replace("+", "", regex=False).replace("%", "", regex=False)
            
            # Convert volume notation (K, M, B)
            def convert_volume(value):
                if isinstance(value, str) and value:
                    value = value.replace(",", "")
                    multiplier = {"K": 1e3, "M": 1e6, "B": 1e9}
                    for key, factor in multiplier.items():
                        if key in value:
                            return float(value.replace(key, "")) * factor
                    return float(value)
                return np.nan
            
            df_final["Volume"] = df_final["Volume"].apply(convert_volume)
            df_final["Market cap"] = df_final["Market cap"].apply(convert_volume)
            
            # Clean up Symbol and Description columns
            df_final[['Symbol', 'Description']] = df_final["Symbol"].str.replace(r'\n(D|REIT|P|DR)$', '', regex=True).str.split("\n", n=1, expand=True)
            
            # Reorder columns
            column_order = ["Symbol", "Description"] + [col for col in df_final.columns if col not in ["Symbol", "Description"]]
            df_final = df_final[column_order]
            
            return df_final
        
        return pd.DataFrame()

if __name__ == "__main__":
    scraper = TradingViewScraper(config_path="config.yaml")
    df = scraper.scrape_tradingview_overview()
    
    if not df.empty:
        connector = MySQLDataConnector(credentials_file='credential_mysql.json', 
                                       table_name='tradingview_overview', 
                                       primary_keys=['Symbol', 'Pair'])
        connector.insert_or_update(df)
        connector.close_connection()
        print("✅ Data inserted successfully.")
    else:
        print("❌ No data scraped, skipping data upload.")