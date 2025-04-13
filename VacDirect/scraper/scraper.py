import time
import base64
import os
from datetime import datetime
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from pymongo import MongoClient

# === CONFIG ===
BASE_URL = "https://www.harveynorman.co.nz/home-appliances/vacuums-and-floor-care"
CHROMEDRIVER_PATH = r"C:\\WebDriver\\bin\\chromedriver.exe"
MONGO_URI = "mongodb+srv://tommc9010:sG0Y9G2Zu7Jsy7@cluster0.gz9xv3d.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "vacdirect"
COLLECTION_NAME = "harvey_products"

def get_page_source(url, delay=8):
    chrome_options = Options()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")

    service = Service(CHROMEDRIVER_PATH)
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_page_load_timeout(180)

    products = []
    html = ""
    is_404 = False

    try:
        driver.get(url)
        time.sleep(delay)
        html = driver.page_source

        page_title = driver.title
        if "can't find" in page_title.lower() or "404" in page_title:
            is_404 = True
            return html, products, is_404

        if "Oops! We can't find that page" in html or "Sorry, we couldn't find the page" in html:
            is_404 = True
            return html, products, is_404

        product_elements = driver.find_elements(By.CSS_SELECTOR, "[data-product-id][data-product-price]")

        if not product_elements:
            product_elements = driver.find_elements(By.CSS_SELECTOR, "[data-product-id][data-product-name]")

        processed_ids = set()

        for element in product_elements:
            try:
                product_id = element.get_attribute('data-product-id')
                if product_id in processed_ids:
                    continue
                processed_ids.add(product_id)

                product_name = element.get_attribute('data-product-name')
                if product_name and ';' not in product_name and '&' not in product_name:
                    try:
                        decoded_name = base64.b64decode(product_name).decode('utf-8')
                        if decoded_name and len(decoded_name) > 5:
                            product_name = decoded_name
                    except:
                        pass

                product_price = element.get_attribute('data-product-price')
                if product_price:
                    if product_price.replace(".", "", 1).isdigit():
                        product_price = f"${product_price}"

                if product_name and product_price:
                    products.append({"model": product_name, "price": product_price})
            except:
                continue

        if not products:
            products = extract_from_html(html)

    except Exception as e:
        print(f"Error during page processing: {e}")
    finally:
        driver.quit()

    return html, products, is_404

def extract_from_html(html):
    soup = BeautifulSoup(html, 'html.parser')
    products = []
    product_elements = soup.select('[data-product-id][data-product-price]')
    if not product_elements:
        product_elements = soup.select('[data-product-id][data-product-name]')

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
                except:
                    pass

            if product_price and product_price.replace(".", "", 1).isdigit():
                product_price = f"${product_price}"

            if product_name and product_price:
                products.append({"model": product_name, "price": product_price})
        except:
            continue

    return products

def run_pipeline():
    print("\n=== Starting Harvey Norman Scraper with MongoDB Atlas Output ===\n")
    all_products = []
    page_num = 1

    while True:
        if page_num == 1:
            current_url = BASE_URL + "/"
        else:
            current_url = f"{BASE_URL}/page-{page_num}/"

        html, page_products, is_404 = get_page_source(current_url)

        if is_404:
            break

        print(f"Page {page_num}: Found {len(page_products)} products")
        all_products.extend(page_products)
        page_num += 1
        time.sleep(1)

    if not all_products:
        print("No products scraped. Aborting DB insert.")
        return

    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        collection.delete_many({})
        collection.insert_many(all_products)

        print(f"Inserted {len(all_products)} products into MongoDB Atlas.")
    except Exception as e:
        print(f"MongoDB Error: {e}")

if __name__ == "__main__":
    run_pipeline()
