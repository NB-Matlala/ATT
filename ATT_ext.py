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

base_url = "https://www.autotrader.co.za" 
# os.getenv("BASE_URL")
con_str = os.getenv("CON_STR")
# Global session with headers
session = HTMLSession()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36",
    "Accept-Language": "en-US,en;q=0.9",
    "Referer": "https://www.google.com",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
})


# Queue for multithreading
queue = Queue()

# Lists to store extracted data
stored_links = []
thread_data = []

# Lock to prevent race conditions when writing to lists
lock = threading.Lock()


def get_last_page(soup):
    """Extracts the last page number from pagination"""
    total_listings_element = soup.find('span', class_='e-results-total__inlwbpm7AH4ufxfg')
    
    if total_listings_element:
        total_listings = int(total_listings_element.text.replace(' ', ''))
        last_page = math.ceil(total_listings / 20) + 10  # Adding buffer pages
        return last_page
    return 1


def process_page(category, page_url):
    """Fetch and extract data from a given page URL"""
    response = session.get(page_url)
    soup = BeautifulSoup(response.content, 'html.parser')

    cars_containers = soup.find_all('div', attrs={'class': re.compile(r'b-result-tile__NSAT4E2EbrD8w7kA .*')})

    temp_stored_links = []
    temp_thread_data = []

    for each_div in cars_containers:
        link = each_div.find('a', href=True)
        if not link or '?' not in link['href']:
            continue
        
        found_link = base_url + link['href']
        car_id_match = re.search(r'/(\d+)\?', found_link)
        if not car_id_match:
            continue
        
        car_id = car_id_match.group(1)
        title = each_div.find('span', class_='e-title__PWADYWpQJlv5U7Pv')
        price_span = each_div.find('span', class_='e-price__fz79voUOfPnB65Lt')

        price = None
        if price_span:
            inner_span = price_span.find('span')
            price = inner_span.text.strip() if inner_span else price_span.text.strip()
        
        dealer = each_div.find('span', class_='e-dealer__ijJoWpcBtgDSTAs2')
        location = each_div.find('span', class_='e-suburb__UJOR_1tLqFkXpFjd')
        suburb, region = location.text.split(',') if location else ("Unknown", "Unknown")

        # Timestamp
        current_datetime = (datetime.now() + timedelta(hours=2)).strftime('%Y-%m-%d')

        car_data = {
            'Car_ID': car_id,
            'Title': title.text.strip() if title else None,
            'Category': category,
            'Date': current_datetime,
            'Dealer': dealer.text.strip() if dealer else None,
            'Suburb': suburb.strip(),
            'Region': region.strip(),
            'Price': re.sub(r'[^\d.,]+', '', price) if price else None,
            'Link': found_link
        }

        temp_thread_data.append(car_data)
        temp_stored_links.append({'Link': found_link})

    # Use lock to safely update global lists
    with lock:
        thread_data.extend(temp_thread_data)
        stored_links.extend(temp_stored_links)


def worker():
    """Thread worker that processes pages from the queue"""
    while not queue.empty():
        category, page_url = queue.get()
        try:
            process_page(category, page_url)
        finally:
            queue.task_done()


def main():
    commercial_types = {
        "trucks-for-sale": "Trucks",
        "tractors-for-sale": "Tractors",
        "trailers-for-sale": "Trailers",
        "attachments-for-sale": "Attachments",
        "buses-for-sale": "Buses",
        "cranes-for-sale": "Cranes",
        "dozers-for-sale": "Dozers",
        "excavators-for-sale": "Excavators",
        "forklifts-for-sale": "Forklifts",
        "graders-for-sale": "Graders",
        "loaders-for-sale": "Loaders",
        "rollers-for-sale": "Rollers",
    }

    # Get last page numbers for each category
    for category_url, category in commercial_types.items():
        type_url = f"{base_url}/{category_url}?pagenumber=1&sortorder=Newest"
        response = session.get(type_url)
        soup = BeautifulSoup(response.content, 'html.parser')
        last_page = get_last_page(soup)

        # Add pages to the queue
        for pg in range(1, last_page + 1):
            page_url = f"{base_url}/{category_url}?pagenumber={pg}&sortorder=Newest"
            queue.put((category, page_url))

    # Start threads
    threads = []
    for _ in range(5):  # Adjust number of threads as needed
        t = threading.Thread(target=worker)
        t.start()
        threads.append(t)

    # Wait for all threads to finish
    queue.join()

    # Save results to CSV
    save_to_csv()

def save_to_csv():
    """Saves extracted data to separate CSV files"""
    
    # Save the main extracted vehicle data
    with open("thread_data.csv", "w", newline="", encoding="utf-8") as data_file:
        writer = csv.DictWriter(data_file, fieldnames=["Car_ID", "Title", "Category", "Date", "Dealer", "Suburb", "Region", "Price", "Link"])
        writer.writeheader()
        writer.writerows(thread_data)

    # Split stored_links into two batches
    mid_index = len(stored_links) // 2
    stored_links_batch1 = stored_links[:mid_index]
    stored_links_batch2 = stored_links[mid_index:]

    # Save first batch
    with open("stored_links_batch1.csv", "w", newline="", encoding="utf-8") as batch1_file:
        writer = csv.DictWriter(batch1_file, fieldnames=["Link"])
        writer.writeheader()
        writer.writerows(stored_links_batch1)

    # Save second batch
    with open("stored_links_batch2.csv", "w", newline="", encoding="utf-8") as batch2_file:
        writer = csv.DictWriter(batch2_file, fieldnames=["Link"])
        writer.writeheader()
        writer.writerows(stored_links_batch2)

    
    filename1, filename2, filename3 = "thread_data.csv","stored_links_batch1.csv","stored_links_batch2.csv"
    # Upload to Blob 
    connection_string = f"{con_str}"
    container_name = "privateprop"
    blob_name1 = filename1

    blob_client = BlobClient.from_connection_string(connection_string, container_name, blob_name1)
    with open(filename1, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    #file2
    blob_name2 = filename2
    blob_client = BlobClient.from_connection_string(connection_string, container_name, blob_name2)
    with open(filename2, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    #file3
    blob_name3 = filename3
    blob_client = BlobClient.from_connection_string(connection_string, container_name, blob_name3)
    with open(filename3, "rb") as data:
        blob_client.upload_blob(data, overwrite=True)
    
    
    print("Data extraction complete! CSV files saved as 'stored_links_batch1.csv' and 'stored_links_batch2.csv'.")

if __name__ == "__main__":
    print("Running. . .")
    main()


