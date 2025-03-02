import yaml
import os
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
from google_sheet_api import GoogleSheetsUploader
from typing import Dict, Any

class TradingViewScraper:
    def __init__(self, headless: bool = True, config_path="config.yaml"):
        """
        Initializes the WebDriver with optimized Chrome options.
        :param headless: Run Chrome in headless mode (default: True)
        :param config_path: Path to the YAML configuration file
        """
        self.config_file = config_path  # Ensure correct reference
        
        # Load configuration
        self.config = self.load_config()
        
        self.symbol_map = self.config.get("symbols_tradingview", {})
        self.output_dir = self.config.get("output_directory", ".")

        # Ensure output directory exists
        os.makedirs(self.output_dir, exist_ok=True)

        chrome_options = Options()
        if headless:
            chrome_options.add_argument("--headless=new")

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

        # Standardized column headers
        self.overview_headers = [
            "Pair", "Symbol", "Market cap", "Price", "Change %", "Volume", "Rel Volume",
            "P/E", "EPS dil", "EPS dil growth", "Div yield %", "Sector", "Analyst Rating", "Last Updated"
        ]
        
    def load_config(self) -> Dict[str, Any]:
        """
        Loads the YAML configuration file.

        Returns:
            Dict[str, Any]: Parsed YAML configuration.
        """
        try:
            with open(self.config_file, "r") as file:
                return yaml.safe_load(file)
        except Exception as e:
            raise FileNotFoundError(f"Error loading config file: {e}")

    def close_browser(self):
        """Closes the WebDriver."""
        self.driver.quit()

    def scrape_tradingview_overview(self, filename="tradingview_indices_components.csv"):
        """
        Scrapes TradingView for component data of all symbols in the symbol map.

        Args:
            filename (str): The filename where the scraped data will be saved.
        
        Returns:
            str: The path of the saved CSV file.
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
            
            # Convert numeric columns
            for column in ["Market cap", "Price", "EPS dil"]:
                df_final[column] = df_final[column].str.replace(r"[^\d.\sKMB]", "", regex=True).apply(pd.to_numeric, errors='coerce')

            for col in ["EPS dil growth", "Change %"]:
                df_final[col] = df_final[col].str.replace("+", "", regex=False)

            def convert_volume(value):
                if isinstance(value, str) and value:
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

            # Save CSV using the output directory from YAML
            output_path = os.path.join(self.output_dir, filename)
            df_final.to_csv(output_path, index=False)
            print(f"✅ Data saved to {output_path}")
            return output_path
        
        return None

if __name__ == "__main__":
    scraper = TradingViewScraper(config_path="config.yaml")
    csv_path = scraper.scrape_tradingview_overview()

    if csv_path:
        # Upload data to Google Sheets
        uploader = GoogleSheetsUploader(
            credentials_file="credential_google_sheets.json",
            spreadsheet_name="Stock Market Dashboard"
        )
        uploader.upload_to_sheets(csv_path, name_sheet="tradingview_overview")
    else:
        print("❌ No data scraped, skipping Google Sheets upload.")