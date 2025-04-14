import requests
from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
import platform
import ssl
import concurrent.futures
import time
import certifi

# === CONFIG ===
BASE_URL = "https://www.harveynorman.co.nz/home-appliances/vacuums-and-floor-care"
MONGO_URI = "mongodb+srv://tommc9010:sG0Y9G2Zu7Jsy7@cluster0.gz9xv3d.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "vacdirect"
COLLECTION_NAME = "harvey_products"
MAX_WORKERS = 4  # Number of parallel workers
headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
}

def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument(f"--user-agent={headers['User-Agent']}")
    
    try:
        return webdriver.Chrome(options=chrome_options)
    except Exception as e:
        print(f"Auto-detection failed, falling back to platform-specific path: {str(e)}")
    
    system = platform.system()
    if system == "Linux":
        return webdriver.Chrome(service=Service("/usr/bin/chromedriver"), options=chrome_options)
    elif system == "Windows":
        return webdriver.Chrome(service=Service("C:\\WebDriver\\bin\\chromedriver.exe"), options=chrome_options)
    else:
        raise EnvironmentError("Unsupported OS or ChromeDriver not found")

def extract_products_from_page(html):
    soup = BeautifulSoup(html, "html.parser")
    products = []
    product_elements = soup.select("[data-product-id][data-product-name][data-product-price]")
    
    for el in product_elements:
        name = el.get("data-product-name", "").strip()
        price = el.get("data-product-price", "").strip()
        if name and price:
            products.append({
                "model": name,
                "price": f"${price}" if not price.startswith("$") else price,
                "scraped_at": datetime.now().isoformat()
            })
    
    return products

def scrape_page(page_num):
    url = f"{BASE_URL}/page-{page_num}/" if page_num > 1 else BASE_URL
    print(f"Scraping {url}")
    
    try:
        driver = create_driver()
        driver.get(url)
        html = driver.page_source
        driver.quit()
        
        products = extract_products_from_page(html)
        print(f"Page {page_num}: Found {len(products)} products")
        return products
    except Exception as e:
        print(f"Error scraping page {page_num}: {str(e)}")
        return []

def scrape_all_pages_parallel():
    # First get an estimate of total pages
    first_page = scrape_page(1)
    if not first_page:
        return []
    
    all_products = first_page
    
    # Start from page 2 (since we already have page 1)
    page = 2
    empty_pages = 0
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        # Pre-submit a batch of pages to check
        future_to_page = {executor.submit(scrape_page, p): p for p in range(page, page + MAX_WORKERS)}
        
        while future_to_page:
            # Wait for the first completed future
            done, _ = concurrent.futures.wait(
                future_to_page, 
                return_when=concurrent.futures.FIRST_COMPLETED
            )
            
            for future in done:
                page_num = future_to_page.pop(future)
                products = future.result()
                
                if products:
                    all_products.extend(products)
                    # Submit a new page to scrape
                    next_page = page + MAX_WORKERS
                    future_to_page[executor.submit(scrape_page, next_page)] = next_page
                    page += 1
                else:
                    empty_pages += 1
                    if empty_pages >= MAX_WORKERS:
                        # If multiple consecutive pages are empty, stop scraping
                        future_to_page.clear()  # Clear remaining futures to exit the loop
            
            # If all workers found empty pages, stop
            if empty_pages >= MAX_WORKERS:
                print(f"Multiple empty pages found. Ending scrape.")
                break
    
    return all_products

def save_to_mongo(data):
    try:
        # Use certifi for CA certificates
        client = MongoClient(
            MONGO_URI,
            tls=True,
            tlsCAFile=certifi.where(),
            serverSelectionTimeoutMS=30000
        )
        
        # Test connection
        client.admin.command('ping')
        print("MongoDB connection successful")
        
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        # Delete previous data and insert new data
        collection.delete_many({})
        if data:
            collection.insert_many(data)
            print(f"Inserted {len(data)} products into MongoDB Atlas.")
        else:
            print("No data to insert.")
    except Exception as e:
        print(f"MongoDB Error: {e}")
        
        # Fallback attempt with different TLS settings
        try:
            print("Trying fallback MongoDB connection...")
            client = MongoClient(
                MONGO_URI,
                tlsInsecure=True,  # This will disable all certificate validation
                serverSelectionTimeoutMS=30000
            )
            client.admin.command('ping')
            print("Fallback MongoDB connection successful")
            
            db = client[DB_NAME]
            collection = db[COLLECTION_NAME]
            collection.delete_many({})
            if data:
                collection.insert_many(data)
                print(f"Inserted {len(data)} products into MongoDB Atlas using fallback connection.")
        except Exception as e2:
            print(f"Fallback MongoDB connection also failed: {e2}")

def run():
    print("\n=== Starting Harvey Norman Scraper with MongoDB Atlas Output ===\n")
    start_time = time.time()
    
    products = scrape_all_pages_parallel()
    
    if products:
        print(f"Total products scraped: {len(products)}")
        save_to_mongo(products)
    else:
        print("No products were scraped.")
    
    end_time = time.time()
    print(f"Scraping completed in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    run()
