from fastapi import FastAPI, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from pymongo import MongoClient
from typing import List
import subprocess
import datetime

# === CONFIG ===
MONGO_URI = "mongodb+srv://tommc9010:sG0Y9G2Zu7Jsy7@cluster0.gz9xv3d.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0"
DB_NAME = "vacdirect"
COLLECTION_NAME = "harvey_products"

# === FastAPI Setup ===
app = FastAPI()

# Enable CORS for frontend access (adjust origins as needed)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

@app.get("/products")
def get_products():
    try:
        client = MongoClient(MONGO_URI)
        db = client[DB_NAME]
        collection = db[COLLECTION_NAME]

        products = list(collection.find({}, {"_id": 0}))
        return {"status": "success", "count": len(products), "data": products}
    except Exception as e:
        return {"status": "error", "message": str(e)}

@app.post("/scrape")
def trigger_scrape(background_tasks: BackgroundTasks):
    def run_scraper():
        subprocess.run(["python", "../scraper/scraper.py"])

    background_tasks.add_task(run_scraper)
    return {"status": "scraping", "started_at": datetime.datetime.utcnow().isoformat()}