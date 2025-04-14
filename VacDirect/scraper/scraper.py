import time
import base64
import os
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from pymongo import MongoClient
import platform

# === CONFIG ===
BASE_URL = "https://www.harveynorman.co.nz/home-appliances/vacuums-and-floor-care"
MONGO_URI = "mongodb+srv://tommc9010:sG0Y9G2Zu7Jsy7@cluster0.gz9xv3d.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "vacdirect"
COLLECTION_NAME = "harvey_products"

def get_chromedriver():
    """
    Tries to instantiate a Chrome WebDriver. If auto-detection fails,
    it falls back to known paths based on the OS.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # modern headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    
    try:
        # Try auto-detection (if Selenium can manage it)
        return webdriver.Chrome(options=chrome_options)
    except Exception as e:
        print("Auto-detection failed, falling back to OS-specific driver paths...")
        system = platform.system()
        if system == "Linux":
            return webdriver.Chrome(service=Service("/usr/bin/chromedriver"), options=chrome_options)
        elif system == "Windows":
            return webdriver.Chrome(service=Service("C:\\WebDriver\\bin\\chromedriver.exe"), options=chrome_options)
        else:
            raise RuntimeError("Unsupported OS or ChromeDriver path not found.")

def extract_products_from_page(html):
    """
    Uses BeautifulSoup to parse the HTML and extract product data.
    The selector looks for elements with data attributes: 
    data-product-id, data-product-name, and data-product-price.
    """
    soup = BeautifulSoup(html, "html.parser")
    products = []
    product_elements = soup.select("[data-product-id][data-product-name][data-product-price]")
    
    for el in product_elements:
        name = el.get("data-product-name", "").strip()
        price = el.get("data-product-price", "").strip()
        if name and price:
            products.append({
                "model": name,
                "price": f"${price}" if not price.startswith("$") else price
            })
    return products

def scrape_all_pages():
    """
    Navigates through the product pages using Selenium.
    Uses explicit waits to give pages time to load dynamic content.
    """
    all_products = []
    page = 1

    # Initialize Chrome options and driver
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")

    driver = get_chromedriver()

    while True:
        url = f"{BASE_URL}/page-{page}/" if page > 1 else BASE_URL
        print(f"Scraping {url}")
        try:
            driver.get(url)
        except Exception as e:
            print(f"Error loading {url}: {e}")
            break

        # Explicitly wait for an element that indicates the product list has loaded.
        try:
            WebDriverWait(driver, 15).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-product-id]"))
            )
        except Exception as e:
            print(f"Timeout or error waiting for product elements on page {page}: {e}")

        # Additional wait to be safe
        time.sleep(3)
        html = driver.page_source

        products = extract_products_from_page(html)
        if not products:
            print("No products found. Ending scrape.")
            break

        print(f"Page {page}: Found {len(products)} products")
        all_products.extend(products)
        page += 1
        # Optional: slight delay between pages
        time.sleep(1)

    driver.quit()
    return all_products

def save_to_mongo(data):
    """
    Connects to MongoDB Atlas, clears the target collection, and inserts the new product data.
    """
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]
        collection.delete_many({})
        collection.insert_many(data)
        print(f"Inserted {len(data)} products into MongoDB Atlas.")
    except Exception as e:
        print(f"MongoDB Error: {e}")

def run():
    print("\n=== Starting Harvey Norman Scraper with MongoDB Atlas Output ===\n")
    products = scrape_all_pages()
    if products:
        save_to_mongo(products)
    else:
        print("No products scraped.")

if __name__ == "__main__":
    run()
