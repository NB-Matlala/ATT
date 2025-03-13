from bs4 import BeautifulSoup
from requests_html import HTMLSession
import re
import math
import threading
import csv
from queue import Queue
from datetime import datetime, timedelta
from azure.storage.blob import BlobClient
import os

base_url = os.getenv("BASE_URL")
con_str = os.getenv("CON_STR")
# con_str = os.getenv("CON_STR")

# Global session with headers
session = HTMLSession()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
})

def main():
    page_url = f"{base_url}/trucks-for-sale"
    response = session.get(page_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    print(soup)

if __name__ == "__main__":
    print("Running. . .")
    main()
