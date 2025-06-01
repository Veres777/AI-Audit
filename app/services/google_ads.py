import os
import sys
import sqlite3
from datetime import datetime
from dotenv import load_dotenv
from google.auth.transport.requests import Request
from google_auth_oauthlib.flow import InstalledAppFlow
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

# Загрузка из .env
load_dotenv()

SCOPES = ["https://www.googleapis.com/auth/adwords"]
DB_FILE = "ads_data.sqlite"

CUSTOMER_ID = os.getenv("GOOGLE_ADS_CUSTOMER_ID")
DEVELOPER_TOKEN = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN")
LOGIN_CUSTOMER_ID = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID") or None

def authenticate():
    flow = InstalledAppFlow.from_client_secrets_file("credentials.json", SCOPES)
    creds = flow.run_local_server(port=8080)

    # Если нужно — обновим токен
    if creds.expired and creds.refresh_token:
        creds.refresh(Request())

    return creds

def init_db():
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        CREATE TABLE IF NOT EXISTS campaign_metrics (
            id INTEGER,
            customer_id TEXT,
            date TEXT,
            name TEXT,
            status TEXT,
            impressions INTEGER,
            clicks INTEGER,
            ctr REAL,
            cost_micros INTEGER,
            cpa_micros INTEGER,
            PRIMARY KEY (id, customer_id, date)
        )
    ''')
    conn.commit()
    conn.close()

def save_campaign(data):
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()
    c.execute('''
        INSERT OR REPLACE INTO campaign_metrics
        (id, customer_id, date, name, status, impressions, clicks, ctr, cost_micros, cpa_micros)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', data)
    conn.commit()
    conn.close()

def fetch_campaigns(client, customer_id):
    ga_service = client.get_service("GoogleAdsService")
    query = """
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            metrics.impressions,
            metrics.clicks,
            metrics.ctr,
            metrics.cost_micros,
            metrics.average_cpa_micros,
            segments.date
        FROM campaign
        WHERE campaign.status != 'REMOVED'
        AND segments.date DURING LAST_7_DAYS
        LIMIT 100
    """
    try:
        response = ga_service.search(customer_id=customer_id, query=query)
        for row in response:
            data = (
                row.campaign.id,
                customer_id,
                row.segments.date,
                row.campaign.name,
                row.campaign.status.name,
                row.metrics.impressions,
                row.metrics.clicks,
                row.metrics.ctr,
                row.metrics.cost_micros,
                row.metrics.average_cpa_micros,
            )
            save_campaign(data)
            print(f"✅ [{customer_id}] {row.campaign.name} – CTR: {row.metrics.ctr}%")
    except GoogleAdsException as ex:
        print(f"❌ Ошибка API для {customer_id}:")
        for error in ex.failure.errors:
            print(f" - {error.message}")
        sys.exit(1)

def main():
    print("📅 Запуск:", datetime.now())
    init_db()
    credentials = authenticate()

    config = {
        "developer_token": DEVELOPER_TOKEN,
        "login_customer_id": LOGIN_CUSTOMER_ID,
        "credentials": credentials,
    }

    client = GoogleAdsClient.load_from_dict(config, version="v16")
    customer_service = client.get_service("CustomerService")

    customer_ids = []
    if CUSTOMER_ID:
        customer_ids = [CUSTOMER_ID]
    else:
        accessible = customer_service.list_accessible_customers()
        customer_ids = [res.replace("customers/", "") for res in accessible.resource_names]

    for customer_id in customer_ids:
        fetch_campaigns(client, customer_id)

if __name__ == "__main__":
    main()


