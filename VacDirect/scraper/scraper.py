import json
from bs4 import BeautifulSoup
from datetime import datetime
import requests
import time
import os

# === CONFIG ===
BASE_URL = "https://www.harveynorman.co.nz/home-appliances/vacuums-and-floor-care"
OUTPUT_FILE = "harvey_products.json"

# Configure session for better performance
session = requests.Session()
session.headers.update({
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.5"
})

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

def scrape_page(page_num):
    """Scrape a single page using requests (much faster than Selenium)"""
    url = f"{BASE_URL}/page-{page_num}/" if page_num > 1 else BASE_URL
    print(f"Scraping {url}")
    
    try:
        response = session.get(url, timeout=10)
        if response.status_code == 200:
            products = extract_products_from_page(response.text)
            print(f"Page {page_num}: Found {len(products)} products")
            return products
        else:
            print(f"Error: HTTP status {response.status_code} for page {page_num}")
            return []
    except Exception as e:
        print(f"Error scraping page {page_num}: {str(e)}")
        return []

def scrape_all_pages():
    """Scrape all pages sequentially with minimum delay"""
    all_products = []
    page = 1
    empty_pages = 0
    
    while empty_pages < 2:  # Stop after 2 empty pages
        print(f"--- Processing page {page} ---")
        start_time = time.time()
        
        products = scrape_page(page)
        
        if products:
            all_products.extend(products)
            empty_pages = 0
        else:
            empty_pages += 1
            print(f"Empty page {page}. Empty count: {empty_pages}/2")
        
        # Report page scraping time
        end_time = time.time()
        print(f"Page {page} completed in {end_time - start_time:.2f} seconds")
        print(f"Running total: {len(all_products)} products")
        
        page += 1
        time.sleep(0.5)  # Brief pause between pages
    
    print(f"Completed scraping. Found {len(all_products)} products across {page-1-empty_pages} pages.")
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

def run():
    """Main function to run the scraper"""
    print("\n=== Starting Fast Harvey Norman Scraper ===\n")
    start_time = time.time()
    
    # Scrape products
    products = scrape_all_pages()
    
    if products:
        # Save to file
        save_to_file(products)
        
        # Print MongoDB connection string for later use
        print("\nTo import this data to MongoDB, you can use the MongoDB compass app or run:")
        print("mongoimport --uri=\"mongodb+srv://tommc9010:sG0Y9G2Zu7Jsy7@cluster0.gz9xv3d.mongodb.net/vacdirect\" --collection=harvey_products --file=harvey_products.json --jsonArray")
    else:
        print("No products were scraped.")
    
    end_time = time.time()
    print(f"Scraping completed in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    run()
