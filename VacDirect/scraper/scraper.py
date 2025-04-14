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

# === CONFIG ===
BASE_URL = "https://www.harveynorman.co.nz/home-appliances/vacuums-and-floor-care"
MONGO_URI = "mongodb+srv://tommc9010:sG0Y9G2Zu7Jsy7@cluster0.gz9xv3d.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "vacdirect"
COLLECTION_NAME = "harvey_products"
OUTPUT_FILE = "harvey_products.json"

def create_driver():
    """Create a Chrome driver with the original configuration that was working"""
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    
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
    """Extract product information - using the original method that was working"""
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
                "product_id": product_id
            })
    
    return products

def scrape_all_pages():
    """Scrape all pages using the original method that was working"""
    all_products = []
    page = 1
    
    while True:
        url = f"{BASE_URL}/page-{page}/" if page > 1 else BASE_URL
        print(f"Scraping {url}")
        
        try:
            driver = create_driver()
            driver.get(url)
            
            # Optional: wait a moment for JavaScript to load
            time.sleep(1)
            
            html = driver.page_source
            driver.quit()
            
            products = extract_products_from_page(html)
            
            if not products:
                print("No products found. Ending scrape.")
                break
                
            print(f"Page {page}: Found {len(products)} products")
            all_products.extend(products)
            page += 1
            
        except Exception as e:
            print(f"Error scraping page {page}: {str(e)}")
            try:
                driver.quit()
            except:
                pass
            # Continue to next page despite errors
            page += 1
            if page > 10:  # Safety limit
                break
    
    return all_products

def save_to_file(data, filename=OUTPUT_FILE):
    """Save data to a local JSON file"""
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
        print(f"Successfully saved {len(data)} products to {filename}")
        return True
    except Exception as e:
        print(f"Error saving to file: {str(e)}")
        return False

def save_to_mongo(data):
    """Try to save to MongoDB with simple approach"""
    try:
        # First try with simple approach
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        
        collection.delete_many({})
        collection.insert_many(data)
        print(f"Successfully inserted {len(data)} products into MongoDB")
        return True
    except Exception as e:
        print(f"MongoDB Error: {e}")
        
        # Only try one fallback method
        try:
            print("Trying fallback MongoDB connection...")
            client = MongoClient(
                MONGO_URI,
                ssl=True,
                ssl_cert_reqs=ssl.CERT_NONE
            )
            db = client[DB_NAME]
            collection = db[COLLECTION_NAME]
            
            collection.delete_many({})
            collection.insert_many(data)
            print(f"Successfully inserted {len(data)} products using fallback connection")
            return True
        except Exception as e2:
            print(f"Fallback MongoDB connection also failed: {e2}")
            return False

def run():
    """Main function to run the scraper"""
    print("\n=== Starting Harvey Norman Scraper with Original Method ===\n")
    start_time = time.time()
    
    # Scrape products
    products = scrape_all_pages()
    
    if products:
        # Always save to file first
        save_to_file(products)
        print(f"Total products scraped: {len(products)}")
        
        # Try to save to MongoDB
        mongo_success = save_to_mongo(products)
        
        if not mongo_success:
            print("MongoDB connection failed. Data is available in the local file.")
    else:
        print("No products were scraped.")
    
    end_time = time.time()
    print(f"Scraping completed in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    run()
