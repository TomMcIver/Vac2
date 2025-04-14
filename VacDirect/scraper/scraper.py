import os
import platform
import time
import base64
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from pymongo import MongoClient

# === CONFIG ===
BASE_URL = "https://www.harveynorman.co.nz/home-appliances/vacuums-and-floor-care"
MONGO_URI = "mongodb+srv://tommc9010:sG0Y9G2Zu7Jsy7@cluster0.gz9xv3d.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "vacdirect"
COLLECTION_NAME = "harvey_products"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
}

def get_chromedriver():
    try:
        return webdriver.Chrome  # Selenium 4.6+ autodetects
    except Exception:
        system = platform.system()
        if system == "Linux":
            return lambda **kwargs: webdriver.Chrome(service=Service("/usr/bin/chromedriver"), **kwargs)
        elif system == "Windows":
            return lambda **kwargs: webdriver.Chrome(service=Service("C:\\WebDriver\\bin\\chromedriver.exe"), **kwargs)
        else:
            raise RuntimeError("Unsupported OS or missing ChromeDriver")

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
                "price": f"${price}" if not price.startswith("$") else price
            })
    return products

def scrape_all_pages():
    all_products = []
    page = 1

    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")

    ChromeDriver = get_chromedriver()
    driver = ChromeDriver(options=chrome_options)

    while True:
        url = f"{BASE_URL}/page-{page}/" if page > 1 else BASE_URL
        print(f"Scraping {url}")
        driver.get(url)
        time.sleep(2)

        products = extract_products_from_page(driver.page_source)
        if not products:
            print("No products found. Ending scrape.")
            break

        print(f"Page {page}: Found {len(products)} products")
        all_products.extend(products)
        page += 1

    driver.quit()
    return all_products

def save_to_mongo(data):
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

if __name__ == "__main__":
    run()
