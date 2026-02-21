import os
import glob
import json
import argparse
import polars as pl
from datetime import datetime, timedelta
import shutil


#configs
RAW_DIR = "./data/raw_kaggle"
LANDING_ZONE = "./data/input"
STATE_FILE = "./data/pipeline_state.json"

#file mapping
FILES = {
    "olist_orders_dataset.csv": "orders",
    "olist_order_items_dataset.csv": "order_items",
    "olist_order_payments_dataset.csv": "order_payments",
    "olist_order_reviews_dataset.csv": "order_reviews",
    "olist_customers_dataset.csv": "customers",
    "olist_products_dataset.csv": "products",
    "olist_sellers_dataset.csv": "sellers",
    "olist_geolocation_dataset.csv": "geolocation",
    "product_category_name_translation.csv": "product_category_name_translation"
}

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {"last_processed_time": "2016-09-01"}

def save_state(date_str):
    with open(STATE_FILE, "w") as f:
        json.dump({"last_processed_time": date_str}, f)

def get_orders_for_date(df_orders, target_date):
    #filter orders created on specific date
    #only parse in YYYY-MM-DD
    return df_orders.filter(pl.col("order_purchase_timestamp").str.slice(0, 10) == target_date)

def process_date(target_date, datasets):
    print(f"Processing data for date: {target_date}")

    daily_orders = get_orders_for_date(datasets["orders"], target_date)

    if daily_orders.height == 0:
        print(f"No orders found for date: {target_date}. Skipping.")
        return False

    active_order_ids = daily_orders["order_id"]
    active_customer_ids = daily_orders["customer_id"]
    
    output_path = os.path.join(LANDING_ZONE, target_date)
    os.makedirs(output_path, exist_ok=True)

    daily_orders.write_parquet(os.path.join(output_path, "orders.parquet"))

    datasets["order_items"].filter(pl.col("order_id").is_in(active_order_ids)).write_parquet(os.path.join(output_path, "order_items.parquet"))
    datasets["order_payments"].filter(pl.col("order_id").is_in(active_order_ids)).write_parquet(os.path.join(output_path, "order_payments.parquet"))
    datasets["order_reviews"].filter(pl.col("order_id").is_in(active_order_ids)).write_parquet(os.path.join(output_path, "order_reviews.parquet"))
    datasets["customers"].filter(pl.col("customer_id").is_in(active_customer_ids)).write_parquet(os.path.join(output_path, "customers.parquet"))

    active_items = datasets["order_items"].filter(pl.col("order_id").is_in(active_order_ids))
    active_seller_ids = active_items["seller_id"].unique()
    active_product_ids = active_items["product_id"].unique()

    datasets["sellers"].filter(pl.col("seller_id").is_in(active_seller_ids)).write_parquet(os.path.join(output_path, "sellers.parquet"))
    datasets["products"].filter(pl.col("product_id").is_in(active_product_ids)).write_parquet(os.path.join(output_path, "products.parquet"))

    datasets["geolocation"].write_parquet(os.path.join(output_path, "geolocation.parquet"))
    datasets["product_category_name_translation"].write_parquet(os.path.join(output_path, "product_category_name_translation.parquet"))

    print(f"Exported {daily_orders.height} orders and all related tables to {output_path}")
    return True

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["setup","daily"], default="daily", help="Setup (Backfill) or Daily Simulation")
    parser.add_argument("--date", help="Specific date to process (YYYY-MM-DD).")
    args = parser.parse_args()

    #loading data
    print("Loading Raw Datasetes...")
    datasets = {}
    for csv_file, friendly_name in FILES.items():
        file_path = os.path.join(RAW_DIR, csv_file)
        if not os.path.exists(file_path):
            print(f"Error: {csv_file} not found in {RAW_DIR}")
            return
        
        #read csv
        datasets[friendly_name] = pl.read_csv(file_path, ignore_errors=True)
    print("Datasets loaded successfully.")


    #determine date to process
    target_date = args.date

    if args.mode == "daily" and not target_date:
        #auto_increment date based on state
        state = load_state()
        last_date = datetime.strptime(state["last_processed_time"], "%Y-%m-%d")
        next_date = last_date + timedelta(days=1)
        target_date = next_date.strftime("%Y-%m-%d")

    #execute
    if target_date:
        success = process_date(target_date, datasets)
        if success:
            save_state(target_date)
            print(f"State updated. Next run will process data for: {target_date} + 1 day")


if __name__ == "__main__":
    main()