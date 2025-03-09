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
from typing import Dict, Any, List
from mysql_api import MySQLDataConnector

class InvestingNewsScraper:
    def __init__(self, headless: bool = True, config_path="config.yaml"):
        self.config_file = config_path
        self.config = self.load_config()
        self.symbols = self.config.get("symbols_news_investing", {})
        self.symbol_lookup = {v: k for k, v in self.symbols.items()}

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

    def load_config(self) -> Dict[str, Any]:
        """Load YAML configuration file."""
        try:
            with open(self.config_file, "r") as file:
                return yaml.safe_load(file)
        except Exception as e:
            raise FileNotFoundError(f"❌ Error loading config file: {e}")

    def close_browser(self):
        """Safely close the browser."""
        if self.driver:
            self.driver.quit()

    def scrape_investing_news(self) -> pd.DataFrame:
        """Scrapes news data from Investing.com for each symbol in the config."""
        news_data = []

        for symbol in self.symbols.values():
            url = f"https://www.investing.com/indices/{symbol}-news"
            self.driver.get(url)

            try:
                self.wait.until(EC.presence_of_element_located((By.XPATH, '//ul[@data-test="news-list"]//article')))
                articles = self.driver.find_elements(By.XPATH, '//ul[@data-test="news-list"]//article')

                for article in articles:
                    try:
                        title_element = article.find_element(By.XPATH, './/a[@data-test="article-title-link"]')
                        title = title_element.get_attribute("textContent").strip()
                        article_url = title_element.get_attribute("href")

                        try:
                            summary_element = article.find_element(By.XPATH, './/p[@data-test="article-description"]')
                            summary = summary_element.get_attribute("textContent").strip() if summary_element else ""
                        except:
                            summary = ""

                        datetime_element = article.find_element(By.XPATH, './/time[@data-test="article-publish-date"]')
                        date_str = datetime_element.get_attribute("datetime")

                        news_data.append({
                            "Symbol": self.symbol_lookup.get(symbol, symbol),
                            "Title": title,
                            "Summary": summary,
                            "URL": article_url,
                            "Datetime": date_str,
                        })

                    except Exception as e:
                        print(f"❌ Error extracting article: {e}")
                        continue

            except Exception as e:
                print(f"❌ Error scraping {symbol}: {e}")
                continue

        if not news_data:
            print("❌ No news data found.")
            return pd.DataFrame()

        df = pd.DataFrame(news_data)
        df["Datetime"] = pd.to_datetime(df["Datetime"], errors="coerce").dt.strftime("%Y-%m-%d %H:%M:%S")
        return df


if __name__ == "__main__":
    scraper = InvestingNewsScraper(config_path="config.yaml")
    df = scraper.scrape_investing_news()

    if not df.empty:
        connector = MySQLDataConnector(
            credentials_file='credential_mysql.json',
            table_name='investing_news',
            primary_keys=['Title', 'URL'],
            max_row_key=10,
            sort_col="Datetime",
        )

        connector.insert_or_update(df)
        connector.close_connection()
        print("✅ Data inserted successfully.")

    scraper.close_browser()
