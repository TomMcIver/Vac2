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

# === CONFIG ===
BASE_URL = "https://www.harveynorman.co.nz/home-appliances/vacuums-and-floor-care"
MONGO_URI = "mongodb+srv://tommc9010:sG0Y9G2Zu7Jsy7@cluster0.gz9xv3d.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "vacdirect"
COLLECTION_NAME = "harvey_products"

headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36"
}

def create_driver():
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")

    try:
        return webdriver.Chrome(options=chrome_options)
    except Exception as e:
        print("Auto-detection failed, falling back to platform-specific path")

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
                "price": f"${price}" if not price.startswith("$") else price
            })
    return products

def scrape_all_pages():
    all_products = []
    page = 1

    while True:
        url = f"{BASE_URL}/page-{page}/" if page > 1 else BASE_URL
        print(f"Scraping {url}")
        driver = create_driver()
        driver.get(url)
        html = driver.page_source
        driver.quit()

        products = extract_products_from_page(html)
        if not products:
            print("No products found. Ending scrape.")
            break

        print(f"Page {page}: Found {len(products)} products")
        all_products.extend(products)
        page += 1

    return all_products

def save_to_mongo(data):
    try:
        client = MongoClient(
            MONGO_URI,
            ssl=True,
            ssl_cert_reqs=ssl.CERT_NONE,
            ssl_version=ssl.PROTOCOL_TLSv1_2
        )
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
