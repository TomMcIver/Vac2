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

def get_chrome_driver():
    """
    Initialize a headless Selenium Chrome driver.
    Tries auto-detection, then falls back to OS-specific paths.
    """
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # modern headless mode
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    
    try:
        # Auto-detection: works with newer Selenium versions.
        return webdriver.Chrome(options=chrome_options)
    except Exception as e:
        print("Auto-detection failed, falling back to OS-specific driver paths...")
        system = platform.system()
        if system == "Linux":
            return webdriver.Chrome(service=Service("/usr/bin/chromedriver"), options=chrome_options)
        elif system == "Windows":
            return webdriver.Chrome(service=Service("C:\\WebDriver\\bin\\chromedriver.exe"), options=chrome_options)
        else:
            raise EnvironmentError("Unsupported OS or ChromeDriver path not found.")

def get_page_source(url, delay=8):
    """
    Opens the URL using Selenium and explicitly waits for dynamic content.
    Returns: (html, products, is_404)
    """
    print(f"Opening {url}...")
    driver = get_chrome_driver()
    driver.set_page_load_timeout(180)
    
    products = []
    html = ""
    is_404 = False

    try:
        driver.get(url)

        # Use explicit wait to allow JavaScript to load product elements.
        try:
            # Wait up to 30 seconds for an element with the attribute 'data-product-id' to appear.
            WebDriverWait(driver, 30).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, "[data-product-id]"))
            )
        except Exception:
            print("Timed out waiting for product elements; proceeding anyway.")

        print("Waiting a few extra seconds to ensure full rendering...")
        time.sleep(delay)  # Additional wait to be safe

        html = driver.page_source

        # Check for 404 cues.
        page_title = driver.title
        if "can't find" in page_title.lower() or "404" in page_title:
            print(f"Detected 404 page: {page_title}")
            is_404 = True
            return html, products, is_404

        if "Oops! We can't find that page" in html or "Sorry, we couldn't find the page" in html:
            print("Detected 404 page from content")
            is_404 = True
            return html, products, is_404

        print("Extracting product data from attributes using Selenium...")
        # Try to locate product elements using data attributes.
        product_elements = driver.find_elements(By.CSS_SELECTOR, "[data-product-id][data-product-price]")
        if not product_elements:
            print("No product elements with price attributes found, trying alternative selector...")
            product_elements = driver.find_elements(By.CSS_SELECTOR, "[data-product-id][data-product-name]")

        print(f"Found {len(product_elements)} product elements with required attributes")
        processed_ids = set()

        for element in product_elements:
            try:
                product_id = element.get_attribute('data-product-id')
                if product_id in processed_ids:
                    continue
                processed_ids.add(product_id)

                product_name = element.get_attribute('data-product-name')
                if product_name and product_name.strip() and ';' not in product_name and '&' not in product_name:
                    try:
                        decoded_name = base64.b64decode(product_name).decode('utf-8')
                        if decoded_name and len(decoded_name) > 5:
                            product_name = decoded_name
                    except Exception:
                        pass

                product_price = element.get_attribute('data-product-price')
                if product_price and (product_price.replace('.', '', 1).isdigit() and product_price.count('.') <= 1):
                    product_price = f"${product_price}"

                if product_name and product_price:
                    products.append({"model": product_name, "price": product_price})
                    print(f"Extracted: {product_name} - {product_price}")
            except Exception as e:
                print(f"Error processing element: {e}")

    except Exception as e:
        print(f"Error during page processing: {e}")
    finally:
        driver.quit()

    return html, products, is_404

def extract_from_html(html):
    """
    Fallback: Extract product data from HTML using BeautifulSoup.
    """
    print("Extracting products from HTML fallback...")
    soup = BeautifulSoup(html, 'html.parser')
    products = []
    product_elements = soup.select('[data-product-id][data-product-price]')
    if not product_elements:
        product_elements = soup.select('[data-product-id][data-product-name]')

    print(f"Found {len(product_elements)} product elements in HTML")
    processed_ids = set()

    for element in product_elements:
        try:
            product_id = element.get('data-product-id')
            if product_id in processed_ids:
                continue
            processed_ids.add(product_id)

            product_name = element.get('data-product-name', '')
            product_price = element.get('data-product-price', '')

            if product_name and ';' not in product_name and '&' not in product_name:
                try:
                    decoded_name = base64.b64decode(product_name).decode('utf-8')
                    if decoded_name and len(decoded_name) > 5:
                        product_name = decoded_name
                except Exception:
                    pass

            if product_price and (product_price.replace('.', '', 1).isdigit() and product_price.count('.') <= 1):
                product_price = f"${product_price}"

            if product_name and product_price:
                products.append({"model": product_name, "price": product_price})
                print(f"Extracted from HTML: {product_name} - {product_price}")
        except Exception as e:
            print(f"Error processing HTML element: {e}")

    return products

def save_to_mongo(data):
    """
    Connects to MongoDB Atlas, clears the target collection, and inserts new product data.
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

def run_pipeline():
    """
    Paginates through the Harvey Norman product pages using Selenium,
    extracts product data, and saves them to MongoDB Atlas.
    """
    print("\n=== Starting Harvey Norman Scraper with MongoDB Atlas Output ===\n")
    all_products = []
    page_num = 1

    while True:
        current_url = f"{BASE_URL}/page-{page_num}/" if page_num > 1 else BASE_URL
        html, page_products, is_404 = get_page_source(current_url)
        if is_404 or not page_products:
            break
        all_products.extend(page_products)
        page_num += 1
        time.sleep(1)

    if all_products:
        save_to_mongo(all_products)
    else:
        print("No products scraped.")

if __name__ == "__main__":
    run_pipeline()
