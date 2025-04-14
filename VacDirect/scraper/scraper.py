import requests
from bs4 import BeautifulSoup
from datetime import datetime
import json
import os
import time
import urllib3
import pymongo
from pymongo import MongoClient
import certifi
import socket
import ssl

# === CONFIG ===
BASE_URL = "https://www.harveynorman.co.nz/home-appliances/vacuums-and-floor-care"
MONGO_URI = "mongodb+srv://tommc9010:sG0Y9G2Zu7Jsy7@cluster0.gz9xv3d.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "vacdirect"
COLLECTION_NAME = "harvey_products"
BACKUP_FILE = "harvey_products.json"
MAX_RETRIES = 3
DELAY_BETWEEN_REQUESTS = 2  # seconds

# Configure requests with a longer timeout and retry strategy
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0"
})

# Disable SSL warnings for requests
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def extract_products_from_page(html):
    """Extract product information from a page's HTML"""
    soup = BeautifulSoup(html, "html.parser")
    products = []
    product_elements = soup.select("[data-product-id][data-product-name][data-product-price]")
    
    for el in product_elements:
        name = el.get("data-product-name", "").strip()
        price = el.get("data-product-price", "").strip()
        product_id = el.get("data-product-id", "").strip()
        
        if name and price:
            products.append({
                "model": name,
                "price": f"${price}" if not price.startswith("$") else price,
                "product_id": product_id,
                "retailer": "Harvey Norman",
                "scraped_at": datetime.now().isoformat()
            })
    
    return products

def scrape_page(page_num, max_retries=MAX_RETRIES):
    """Scrape a single page with retries"""
    url = f"{BASE_URL}/page-{page_num}/" if page_num > 1 else BASE_URL
    print(f"Scraping {url}")
    
    for attempt in range(max_retries):
        try:
            response = session.get(url, timeout=30)
            if response.status_code == 200:
                products = extract_products_from_page(response.text)
                print(f"Page {page_num}: Found {len(products)} products")
                return products
            else:
                print(f"Error: HTTP status {response.status_code} for page {page_num}")
        except Exception as e:
            print(f"Attempt {attempt+1}/{max_retries} failed for page {page_num}: {str(e)}")
            if attempt < max_retries - 1:
                wait_time = (attempt + 1) * DELAY_BETWEEN_REQUESTS
                print(f"Waiting {wait_time} seconds before retrying...")
                time.sleep(wait_time)
    
    print(f"Failed to scrape page {page_num} after {max_retries} attempts")
    return []

def scrape_all_pages():
    """Scrape all pages sequentially"""
    all_products = []
    page = 1
    consecutive_empty_pages = 0
    
    while consecutive_empty_pages < 2:  # Stop after 2 consecutive empty pages
        products = scrape_page(page)
        
        if products:
            all_products.extend(products)
            consecutive_empty_pages = 0
        else:
            consecutive_empty_pages += 1
            print(f"No products found on page {page}. Empty pages: {consecutive_empty_pages}/2")
        
        page += 1
        time.sleep(DELAY_BETWEEN_REQUESTS)  # Be nice to the server
    
    print(f"Completed scraping. Found {len(all_products)} products across {page-1-consecutive_empty_pages} pages.")
    return all_products

def save_to_file(data, filename=BACKUP_FILE):
    """Save data to a local JSON file as backup"""
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Successfully saved {len(data)} products to {filename}")
        return True
    except Exception as e:
        print(f"Error saving to file: {str(e)}")
        return False

def check_mongodb_connectivity():
    """Test MongoDB connection using multiple methods"""
    print("Testing MongoDB connectivity...")
    
    # Method 1: Try basic socket connection
    try:
        for host in ["ac-uguwq3v-shard-00-00.gz9xv3d.mongodb.net", 
                    "ac-uguwq3v-shard-00-01.gz9xv3d.mongodb.net", 
                    "ac-uguwq3v-shard-00-02.gz9xv3d.mongodb.net"]:
            sock = socket.create_connection((host, 27017), timeout=5)
            print(f"Socket connection to {host}:27017 successful")
            sock.close()
    except Exception as e:
        print(f"Socket connection failed: {str(e)}")
    
    # Method 2: Try PyMongo's connection with client.server_info()
    try:
        ssl._create_default_https_context = ssl._create_unverified_context
        client = MongoClient(
            MONGO_URI,
            serverSelectionTimeoutMS=5000,
            ssl=True,
            ssl_cert_reqs=ssl.CERT_NONE  # Disable certificate verification
        )
        info = client.server_info()
        print(f"MongoDB connection successful. Server info: {info}")
        return True
    except Exception as e:
        print(f"PyMongo connection test failed: {str(e)}")
        return False

def save_to_mongo(data):
    """Try to save data to MongoDB with several fallback methods"""
    if not data:
        print("No data to save to MongoDB")
        return False
    
    # First save to file as backup
    save_to_file(data)
    
    # Check connectivity first
    connectivity = check_mongodb_connectivity()
    if not connectivity:
        print("MongoDB connectivity check failed, attempting save anyway...")
    
    # Try multiple connection methods
    connection_methods = [
        # Method 1: Standard connection with certifi
        {
            "name": "Standard with certifi",
            "params": {
                "tls": True,
                "tlsCAFile": certifi.where(),
            }
        },
        # Method 2: Disable certificate validation
        {
            "name": "Disable certificate validation",
            "params": {
                "ssl": True,
                "ssl_cert_reqs": ssl.CERT_NONE
            }
        },
        # Method 3: Unverified context with tlsInsecure
        {
            "name": "tlsInsecure mode",
            "params": {
                "tlsInsecure": True
            }
        },
        # Method 4: Direct connection string modification
        {
            "name": "Modified connection string",
            "params": {},
            "uri_suffix": "&ssl=true&tlsInsecure=true"
        }
    ]
    
    for method in connection_methods:
        try:
            print(f"Trying MongoDB connection method: {method['name']}")
            
            # Modify URI if needed
            uri = MONGO_URI
            if 'uri_suffix' in method:
                uri = f"{MONGO_URI}{method['uri_suffix']}"
            
            # Set up client with timeout
            client = MongoClient(
                uri,
                serverSelectionTimeoutMS=10000,
                **method['params']
            )
            
            # Test connection
            client.admin.command('ping')
            print(f"✓ Connection successful with method: {method['name']}")
            
            # Insert data
            db = client[DB_NAME]
            collection = db[COLLECTION_NAME]
            
            # Clear and insert
            collection.delete_many({})
            result = collection.insert_many(data)
            
            print(f"✓ Successfully inserted {len(result.inserted_ids)} products into MongoDB using method: {method['name']}")
            return True
            
        except Exception as e:
            print(f"✗ Method '{method['name']}' failed: {str(e)}")
    
    print("All MongoDB connection methods failed.")
    print("Data is saved locally to file as a backup.")
    return False

def run():
    """Main function to run the scraper"""
    print("\n=== Starting Harvey Norman Scraper with File & MongoDB Output ===\n")
    start_time = time.time()
    
    # Scrape products
    products = scrape_all_pages()
    
    if products:
        # Try to save to MongoDB
        mongo_success = save_to_mongo(products)
        
        if not mongo_success:
            print("Couldn't save to MongoDB. Data is available in the local file.")
    else:
        print("No products were scraped.")
    
    end_time = time.time()
    print(f"Scraping completed in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    run()
