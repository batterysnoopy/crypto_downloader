import concurrent.futures
import re
import requests
import pandas as pd
import zipfile
from io import BytesIO
from tqdm import tqdm
import logging
import time
import os
# Selenium and BeautifulSoup imports
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from bs4 import BeautifulSoup

# Set up logging
LOGGER = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

class KuCoinDataFetcher:
    def __init__(self, chrome_driver_path='path/to/chromedriver'):
        self._base_url = 'https://historical-data.kucoin.com'
        self.chrome_driver_path = chrome_driver_path

    def get_tickers(self):
        """
        Retrieve the list of available tickers using Selenium and BeautifulSoup.
        """
        # Add chromedriver.exe to the PATH within the Python script
        os.environ['PATH'] += os.pathsep + self.chrome_driver_path

        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')  # Optional, useful on Windows
        chrome_options.add_argument('--no-sandbox')   # Necessary if running as root in some environments
        chrome_options.add_argument('--disable-dev-shm-usage')  # Overcome limited resource problems in some environments

        # Initialize the Chrome WebDriver with options
        driver = webdriver.Chrome(options=chrome_options)

        try:
            # Open the specified URL
            url = f"{self._base_url}/?prefix=data/spot/daily/klines"
            driver.get(url)

            # Wait for a few seconds to ensure the page is fully loaded
            time.sleep(1)

            # Fetch the page content (HTML)
            page_content = driver.page_source

            # Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(page_content, 'html.parser')

            # Find all <tr> elements within the <tbody> with id "listing"
            table_rows = soup.find('tbody', id='listing').find_all('tr')

            # Initialize a list to hold the ticker names
            tickers = []

            # Loop through each row to extract the ticker names
            for row in table_rows:
                # Find the <a> tag within the row
                a_tag = row.find('a')
                if a_tag:
                    # Extract the ticker name from the href attribute
                    href = a_tag['href']
                    # The href looks like '?prefix=data/spot/daily/klines/BTCUSDT/'
                    ticker_match = re.search(r'/klines/(.*?)/$', href)
                    if ticker_match:
                        ticker = ticker_match.group(1)
                        tickers.append(ticker)

            LOGGER.info(f"Retrieved {len(tickers)} tickers.")
            return tickers

        finally:
            # Close the browser after you're done
            driver.quit()

    def get_available_dates(self, ticker, frequency='1d'):
        """
        Retrieve the list of available dates for a given ticker and frequency using Selenium and BeautifulSoup.
        """
        # Add chromedriver.exe to the PATH within the Python script
        os.environ['PATH'] += os.pathsep + self.chrome_driver_path

        chrome_options = Options()
        chrome_options.add_argument('--headless')
        chrome_options.add_argument('--disable-gpu')  # Optional, useful on Windows
        chrome_options.add_argument('--no-sandbox')   # Necessary if running as root in some environments
        chrome_options.add_argument('--disable-dev-shm-usage')  # Overcome limited resource problems in some environments

        # Initialize the Chrome WebDriver with options
        driver = webdriver.Chrome(options=chrome_options)

        try:
            # Construct the URL
            url = f"{self._base_url}/?prefix=data/spot/daily/klines/{ticker}/{frequency}/"
            driver.get(url)

            # Wait for a few seconds to ensure the page is fully loaded
            time.sleep(1)

            # Fetch the page content (HTML)
            page_content = driver.page_source

            # Parse the HTML content using BeautifulSoup
            soup = BeautifulSoup(page_content, 'html.parser')

            # Find all <tr> elements within the <tbody> with id "listing"
            table_rows = soup.find('tbody', id='listing').find_all('tr')

            # Initialize a list to hold the dates
            dates = []

            # Loop through each row to extract the dates
            for row in table_rows:
                # Find the <a> tag within the row
                a_tag = row.find('a')
                if a_tag:
                    # Extract the filename from the href attribute
                    href = a_tag['href']
                    # The href looks like '?prefix=data/spot/daily/klines/BTCUSDT/1d/BTCUSDT-1d-2024-07-02.zip'
                    filename_match = re.search(r'/(.+?\.zip)$', href)
                    if filename_match:
                        filename = filename_match.group(1)
                        # Use regex to extract date
                        match = re.match(r'^.*-(\d{4}-\d{2}-\d{2})\.zip$', filename)
                        if match:
                            date_str = match.group(1)
                            dates.append(date_str)

            LOGGER.info(f"Ticker {ticker} has {len(dates)} available dates for frequency {frequency}.")
            return dates

        finally:
            # Close the browser after you're done
            driver.quit()

    def get_combined_data(self, ticker, frequency='1d', dates=None, save_to_disk=False, output_dir='kucoin_data'):
        """
        Read CSV for each day file for a chosen frequency and combine all the CSV data together for a chosen ticker.

        Args:
            ticker (str): The ticker symbol.
            frequency (str): The frequency (e.g., '1d', '8h', etc.).
            dates (list[str], optional): Specific dates to download. If None, all available dates will be downloaded.
            save_to_disk (bool): If True, saves individual CSV files to disk.
            output_dir (str): Directory to save the CSV files if save_to_disk is True.

        Returns:
            pandas.DataFrame: Combined data.
        """
        if dates is None:
            dates = self.get_available_dates(ticker, frequency)
        else:
            available_dates = self.get_available_dates(ticker, frequency)
            dates = [date for date in dates if date in available_dates]

        if not dates:
            LOGGER.warning(f"No dates available for ticker {ticker} and frequency {frequency}.")
            return pd.DataFrame()

        all_data = []

        def download_and_process(date):
            filename = f"{ticker}-{frequency}-{date}.zip"
            # Construct the download URL
            url = f"{self._base_url}/data/spot/daily/klines/{ticker}/{frequency}/{filename}"
            response = requests.get(url)
            if response.status_code == 200:
                # Read the zip file from bytes
                zip_file = zipfile.ZipFile(BytesIO(response.content))
                # Assuming there is only one file in the zip
                csv_filename = zip_file.namelist()[0]
                with zip_file.open(csv_filename) as csv_file:
                    df = pd.read_csv(csv_file)  # Read headers from the first line
                    if save_to_disk:
                        # Save the CSV file to disk
                        output_path = os.path.join(output_dir, ticker, frequency)
                        os.makedirs(output_path, exist_ok=True)
                        df.to_csv(os.path.join(output_path, f"{ticker}-{frequency}-{date}.csv"), index=False)
                    return df
            else:
                LOGGER.error(f"Failed to download data for date {date}. Status code: {response.status_code}")
                return None

        # Use ThreadPoolExecutor to parallelize downloads
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Submit all tasks
            future_to_date = {executor.submit(download_and_process, date): date for date in dates}
            for future in tqdm(concurrent.futures.as_completed(future_to_date), total=len(future_to_date),
                               desc=f"Downloading data for {ticker} at frequency {frequency}"):
                date = future_to_date[future]
                try:
                    df = future.result()
                    if df is not None:
                        all_data.append(df)
                except Exception as e:
                    LOGGER.error(f"Exception occurred while processing date {date}: {e}")

        if all_data:
            # Combine all data
            combined_df = pd.concat(all_data, ignore_index=True)
            LOGGER.info(f"Combined data contains {len(combined_df)} rows.")
            return combined_df
        else:
            LOGGER.warning(f"No data was downloaded for ticker {ticker}.")
            return pd.DataFrame()


if __name__ == '__main__':
    # Specify the path to your chromedriver executable
    chrome_driver_path = r'path/to/chromedriver'  # Replace with your actual path

    # Initialize the data fetcher
    kucoin_fetcher = KuCoinDataFetcher(chrome_driver_path=chrome_driver_path)

    # Retrieve available tickers
    tickers = kucoin_fetcher.get_tickers()
    print(f"Available tickers: {tickers[:5]}")  # Print first 5 tickers

    # Get available dates for a specific ticker
    ticker = 'BTCUSDT'
    dates = kucoin_fetcher.get_available_dates(ticker, frequency='1d')
    print(f"Available dates for {ticker}: {dates}")

    # Get combined data for the ticker
    combined_data = kucoin_fetcher.get_combined_data(ticker, frequency='1d', save_to_disk=False)
    print(combined_data.head())
