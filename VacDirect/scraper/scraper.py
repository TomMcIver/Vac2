import json
from bs4 import BeautifulSoup
from datetime import datetime
from pymongo import MongoClient
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
import platform
import ssl
import time
import certifi
import socket

# === CONFIG ===
BASE_URL = "https://www.harveynorman.co.nz/home-appliances/vacuums-and-floor-care"
MONGO_URI = "mongodb+srv://tommc9010:sG0Y9G2Zu7Jsy7@cluster0.gz9xv3d.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "vacdirect"
COLLECTION_NAME = "harvey_products"
BACKUP_FILE = "harvey_products.json"
MAX_RETRIES = 3
DELAY_BETWEEN_REQUESTS = 1  # seconds

def create_driver():
    """Create a headless Chrome driver with proper settings"""
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    # Set page load timeout
    chrome_options.add_argument("--page-load-strategy=none")
    
    try:
        driver = webdriver.Chrome(options=chrome_options)
        driver.set_page_load_timeout(30)  # 30 second timeout
        return driver
    except Exception as e:
        print(f"Auto-detection failed, falling back to platform-specific path: {str(e)}")
    
    system = platform.system()
    if system == "Linux":
        driver = webdriver.Chrome(service=Service("/usr/bin/chromedriver"), options=chrome_options)
        driver.set_page_load_timeout(30)
        return driver
    elif system == "Windows":
        driver = webdriver.Chrome(service=Service("C:\\WebDriver\\bin\\chromedriver.exe"), options=chrome_options)
        driver.set_page_load_timeout(30)
        return driver
    else:
        raise EnvironmentError("Unsupported OS or ChromeDriver not found")

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
    """Scrape a single page with retries using Selenium"""
    url = f"{BASE_URL}/page-{page_num}/" if page_num > 1 else BASE_URL
    print(f"Scraping {url}")
    
    for attempt in range(max_retries):
        try:
            driver = create_driver()
            driver.get(url)
            
            # Wait for content to load
            time.sleep(3)
            
            html = driver.page_source
            driver.quit()
            
            products = extract_products_from_page(html)
            print(f"Page {page_num}: Found {len(products)} products")
            return products
            
        except Exception as e:
            print(f"Attempt {attempt+1}/{max_retries} failed for page {page_num}: {str(e)}")
            
            # Close driver if it's still open
            try:
                driver.quit()
            except:
                pass
                
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

def save_to_mongo(data):
    """Save data to MongoDB with multiple fallback strategies"""
    if not data:
        print("No data to save to MongoDB")
        return False
    
    # First save to file as backup
    save_to_file(data)
    
    # Try multiple connection methods
    connection_methods = [
        # Method 1: Using dns_seedlist (recommended approach from MongoDB)
        {
            "name": "DNS Seedlist Connection",
            "params": {
                "ssl": True, 
                "ssl_ca_certs": certifi.where(),
                "retryWrites": True,
                "w": "majority",
                "directConnection": False,
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
        # Method 3: Direct connection
        {
            "name": "Direct Connection",
            "params": {
                "directConnection": True,
                "ssl": True,
                "ssl_cert_reqs": ssl.CERT_NONE
            }
        }
    ]
    
    for method in connection_methods:
        try:
            print(f"Trying MongoDB connection method: {method['name']}")
            
            # Set up client with timeout
            client = MongoClient(
                MONGO_URI,
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
