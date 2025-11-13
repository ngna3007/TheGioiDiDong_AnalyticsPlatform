#!/usr/bin/env python3
"""
ETL Pipeline for E-Commerce Analytics Platform
Team Member: Long (Data Engineer)
Purpose: Extract, Transform, and Load e-commerce data to warehouse
"""

import pandas as pd
import numpy as np
import sqlalchemy
from sqlalchemy import create_engine, text
import pyodbc
import logging
from datetime import datetime, timedelta
import os
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('etl_pipeline.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class ETLManager:
    def __init__(self, connection_string):
        """
        Initialize ETL Manager with database connection
        Connection string format:
        'mssql+pyodbc://username:password@server/database?driver=ODBC+Driver+17+for+SQL+Server'
        """
        self.engine = create_engine(connection_string)
        self.data_dir = Path("data/raw")
        self.processed_dir = Path("data/processed")
        self.processed_dir.mkdir(parents=True, exist_ok=True)

    def extract_data(self):
        """
        Extract data from CSV files
        """
        logger.info("🔍 Starting data extraction...")

        datasets = {}

        # Define file mappings
        file_mappings = {
            'customers': 'customers.csv',
            'products': 'products.csv',
            'sellers': 'sellers.csv',
            'orders': 'orders.csv',
            'order_items': 'order_items.csv',
            'order_payments': 'order_payments.csv',
            'order_reviews': 'order_reviews.csv'
        }

        for table_name, filename in file_mappings.items():
            file_path = self.data_dir / filename
            if file_path.exists():
                logger.info(f"📖 Reading {filename}...")
                df = pd.read_csv(file_path)

                # Convert date columns to datetime
                if table_name == 'orders':
                    date_columns = [
                        'order_purchase_timestamp', 'order_approved_at',
                        'order_delivered_carrier_date', 'order_delivered_customer_date',
                        'order_estimated_delivery_date'
                    ]
                    for col in date_columns:
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col], errors='coerce')

                elif table_name == 'order_reviews':
                    date_columns = ['review_creation_date', 'review_answer_timestamp']
                    for col in date_columns:
                        if col in df.columns:
                            df[col] = pd.to_datetime(df[col], errors='coerce')

                datasets[table_name] = df
                logger.info(f"✅ Loaded {len(df)} records from {filename}")
            else:
                logger.warning(f"⚠️ File not found: {filename}")

        return datasets

    def transform_data(self, datasets):
        """
        Transform and clean data
        """
        logger.info("🔄 Starting data transformation...")

        # Transform customers data
        if 'customers' in datasets:
            customers_df = datasets['customers'].copy()

            # Add customer tier based on potential spending
            customers_df['customer_tier'] = np.random.choice(['Bronze', 'Silver', 'Gold', 'Platinum'],
                                                             size=len(customers_df), p=[0.4, 0.3, 0.2, 0.1])

            # Add region mapping
            state_to_region = {
                'SP': 'Southeast', 'RJ': 'Southeast', 'MG': 'Southeast', 'ES': 'Southeast',
                'RS': 'South', 'SC': 'South', 'PR': 'South',
                'BA': 'Northeast', 'PE': 'Northeast', 'CE': 'Northeast', 'MA': 'Northeast',
                'PI': 'Northeast', 'RN': 'Northeast', 'PB': 'Northeast', 'AL': 'Northeast', 'SE': 'Northeast',
                'GO': 'Central-West', 'MT': 'Central-West', 'MS': 'Central-West', 'DF': 'Central-West',
                'AM': 'North', 'PA': 'North', 'AC': 'North', 'RO': 'North', 'RR': 'North', 'AP': 'North'
            }
            customers_df['customer_region'] = customers_df['customer_state'].map(state_to_region)

            datasets['customers'] = customers_df
            logger.info("✅ Transformed customers data")

        # Transform products data
        if 'products' in datasets:
            products_df = datasets['products'].copy()

            # Calculate product volume
            products_df['product_volume_cm3'] = (
                products_df['product_length_cm'] *
                products_df['product_height_cm'] *
                products_df['product_width_cm']
            )

            # Calculate density
            products_df['product_density_g_cm3'] = np.where(
                products_df['product_volume_cm3'] > 0,
                products_df['product_weight_g'] / products_df['product_volume_cm3'],
                0
            )

            # Add price range (will be updated after joining with orders)
            products_df['price_range'] = 'Medium'

            # Category mapping
            category_mapping = {
                'electronics': 'Electronics',
                'fashion': 'Fashion & Accessories',
                'home_garden': 'Home & Garden',
                'sports': 'Sports & Outdoors',
                'books': 'Books & Media',
                'toys': 'Toys & Games',
                'health_beauty': 'Health & Beauty',
                'automotive': 'Automotive',
                'food': 'Food & Beverages',
                'furniture': 'Furniture & Decor'
            }
            products_df['product_category_l1'] = products_df['product_category_name'].map(category_mapping)

            datasets['products'] = products_df
            logger.info("✅ Transformed products data")

        # Transform sellers data
        if 'sellers' in datasets:
            sellers_df = datasets['sellers'].copy()

            # Add seller tier and rating
            sellers_df['seller_tier'] = np.random.choice(['Basic', 'Premium', 'Enterprise'],
                                                        size=len(sellers_df), p=[0.6, 0.3, 0.1])
            sellers_df['seller_rating'] = np.round(np.random.uniform(3.5, 5.0, len(sellers_df)), 2)

            # Add region mapping
            state_to_region = {
                'SP': 'Southeast', 'RJ': 'Southeast', 'MG': 'Southeast', 'ES': 'Southeast',
                'RS': 'South', 'SC': 'South', 'PR': 'South',
                'BA': 'Northeast', 'PE': 'Northeast', 'CE': 'Northeast', 'MA': 'Northeast',
                'PI': 'Northeast', 'RN': 'Northeast', 'PB': 'Northeast', 'AL': 'Northeast', 'SE': 'Northeast',
                'GO': 'Central-West', 'MT': 'Central-West', 'MS': 'Central-West', 'DF': 'Central-West',
                'AM': 'North', 'PA': 'North', 'AC': 'North', 'RO': 'North', 'RR': 'North', 'AP': 'North'
            }
            sellers_df['seller_region'] = sellers_df['seller_state'].map(state_to_region)

            datasets['sellers'] = sellers_df
            logger.info("✅ Transformed sellers data")

        # Transform orders data
        if 'orders' in datasets and 'order_items' in datasets:
            orders_df = datasets['orders'].copy()
            order_items_df = datasets['order_items'].copy()

            # Join orders with order_items
            orders_merged = orders_df.merge(order_items_df, on='order_id', how='inner')

            # Calculate delivery metrics
            orders_merged['delivery_delay_days'] = (
                orders_merged['order_delivered_customer_date'] -
                orders_merged['order_estimated_delivery_date']
            ).dt.days

            orders_merged['processing_time_hours'] = (
                orders_merged['order_approved_at'] -
                orders_merged['order_purchase_timestamp']
            ).dt.total_seconds() / 3600

            # Delivery performance flags
            orders_merged['is_delivered_ontime'] = (
                orders_merged['delivery_delay_days'] <= 0
            ).astype(int)

            orders_merged['is_fast_delivery'] = (
                orders_merged['delivery_delay_days'] < 0
            ).astype(int)

            # Get payment information
            if 'order_payments' in datasets:
                payments_df = datasets['order_payments'].copy()
                # Group payments by order and sum installments
                order_payments_agg = payments_df.groupby('order_id')['payment_installments'].sum().reset_index()
                orders_merged = orders_merged.merge(order_payments_agg, on='order_id', how='left')
            else:
                orders_merged['payment_installments'] = 1

            datasets['orders_merged'] = orders_merged
            logger.info("✅ Transformed orders and order_items data")

        return datasets

    def load_data(self, datasets):
        """
        Load transformed data to database
        """
        logger.info("📊 Starting data loading...")

        try:
            with self.engine.connect() as conn:
                # Load customers data
                if 'customers' in datasets:
                    customers_df = datasets['customers']

                    # Check if customers already exist
                    existing_customers = pd.read_sql("SELECT customer_id FROM dim_customer", conn)
                    new_customers = customers_df[~customers_df['customer_id'].isin(existing_customers['customer_id'])]

                    if len(new_customers) > 0:
                        new_customers.to_sql('dim_customer', conn, if_exists='append', index=False)
                        logger.info(f"✅ Loaded {len(new_customers)} customers to dim_customer")
                    else:
                        logger.info("ℹ️ No new customers to load")

                # Load products data
                if 'products' in datasets:
                    products_df = datasets['products']

                    # Check if products already exist
                    existing_products = pd.read_sql("SELECT product_id FROM dim_product", conn)
                    new_products = products_df[~products_df['product_id'].isin(existing_products['product_id'])]

                    if len(new_products) > 0:
                        new_products.to_sql('dim_product', conn, if_exists='append', index=False)
                        logger.info(f"✅ Loaded {len(new_products)} products to dim_product")
                    else:
                        logger.info("ℹ️ No new products to load")

                # Load sellers data
                if 'sellers' in datasets:
                    sellers_df = datasets['sellers']

                    # Check if sellers already exist
                    existing_sellers = pd.read_sql("SELECT seller_id FROM dim_seller", conn)
                    new_sellers = sellers_df[~sellers_df['seller_id'].isin(existing_sellers['seller_id'])]

                    if len(new_sellers) > 0:
                        new_sellers.to_sql('dim_seller', conn, if_exists='append', index=False)
                        logger.info(f"✅ Loaded {len(new_sellers)} sellers to dim_seller")
                    else:
                        logger.info("ℹ️ No new sellers to load")

                # Load sales fact data
                if 'orders_merged' in datasets:
                    sales_df = datasets['orders_merged']

                    # Get dimension keys
                    customers = pd.read_sql("SELECT customer_key, customer_id FROM dim_customer", conn)
                    products = pd.read_sql("SELECT product_key, product_id FROM dim_product", conn)
                    sellers = pd.read_sql("SELECT seller_key, seller_id FROM dim_seller", conn)
                    dates = pd.read_sql("SELECT date_key, full_date FROM dim_date", conn)

                    # Join with dimension tables to get keys
                    sales_df = sales_df.merge(customers, on='customer_id')
                    sales_df = sales_df.merge(products, on='product_id')
                    sales_df = sales_df.merge(sellers, on='seller_id')

                    # Add date keys
                    sales_df['order_date'] = sales_df['order_purchase_timestamp'].dt.date
                    sales_df = sales_df.merge(
                        dates,
                        left_on='order_date',
                        right_on='full_date',
                        how='left'
                    )

                    # Prepare fact table columns
                    fact_columns = [
                        'order_id', 'order_item_id', 'customer_key', 'product_key',
                        'seller_key', 'date_key', 'order_status', 'price',
                        'freight_value', 'payment_installments', 'order_purchase_timestamp',
                        'order_approved_at', 'order_delivered_carrier_date',
                        'order_delivered_customer_date', 'order_estimated_delivery_date',
                        'delivery_delay_days', 'processing_time_hours',
                        'is_delivered_ontime', 'is_fast_delivery'
                    ]

                    fact_sales_df = sales_df[fact_columns].copy()
                    fact_sales_df['total_value'] = fact_sales_df['price'] + fact_sales_df['freight_value']
                    fact_sales_df = fact_sales_df.rename(columns={'date_key': 'order_date_key'})

                    # Add delivery date key
                    delivery_dates = sales_df['order_delivered_customer_date'].dt.date.dropna().unique()
                    delivery_date_keys = dates[dates['full_date'].isin(delivery_dates)][['full_date', 'date_key']]

                    fact_sales_df = fact_sales_df.merge(
                        delivery_date_keys,
                        left_on=sales_df['order_delivered_customer_date'].dt.date,
                        right_on='full_date',
                        how='left'
                    )
                    fact_sales_df = fact_sales_df.rename(columns={'date_key': 'delivery_date_key'})

                    # Check if sales already exist
                    existing_sales = pd.read_sql("SELECT order_id FROM fact_sales", conn)
                    new_sales = fact_sales_df[~fact_sales_df['order_id'].isin(existing_sales['order_id'])]

                    if len(new_sales) > 0:
                        # Fill missing delivery_date_key with order_date_key
                        new_sales['delivery_date_key'] = new_sales['delivery_date_key'].fillna(new_sales['order_date_key'])
                        new_sales['location_key'] = 1  # Default location key
                        new_sales['created_date'] = datetime.now()
                        new_sales['updated_date'] = datetime.now()

                        new_sales.to_sql('fact_sales', conn, if_exists='append', index=False)
                        logger.info(f"✅ Loaded {len(new_sales)} sales records to fact_sales")
                    else:
                        logger.info("ℹ️ No new sales records to load")

        except Exception as e:
            logger.error(f"❌ Error loading data: {str(e)}")
            raise

    def run_data_quality_checks(self):
        """
        Run data quality checks and generate report
        """
        logger.info("🔍 Running data quality checks...")

        try:
            with self.engine.connect() as conn:
                # Check row counts
                customer_count = pd.read_sql("SELECT COUNT(*) as count FROM dim_customer", conn).iloc[0]['count']
                product_count = pd.read_sql("SELECT COUNT(*) as count FROM dim_product", conn).iloc[0]['count']
                order_count = pd.read_sql("SELECT COUNT(*) as count FROM fact_sales", conn).iloc[0]['count']

                # Check null values
                null_checks = {
                    'Customers with null IDs': pd.read_sql(
                        "SELECT COUNT(*) as count FROM dim_customer WHERE customer_id IS NULL", conn
                    ).iloc[0]['count'],
                    'Orders with null dates': pd.read_sql(
                        "SELECT COUNT(*) as count FROM fact_sales WHERE order_purchase_timestamp IS NULL", conn
                    ).iloc[0]['count']
                }

                # Create quality report
                quality_report = {
                    'timestamp': datetime.now().isoformat(),
                    'record_counts': {
                        'customers': customer_count,
                        'products': product_count,
                        'orders': order_count
                    },
                    'null_checks': null_checks,
                    'data_quality_score': min(100, max(0, 100 - sum(null_checks.values()) * 10))
                }

                # Save quality report
                import json
                with open(self.processed_dir / 'data_quality_report.json', 'w') as f:
                    json.dump(quality_report, f, indent=2)

                logger.info(f"✅ Data quality checks completed. Quality score: {quality_report['data_quality_score']:.1f}")
                logger.info(f"📊 Records: {customer_count:,} customers, {product_count:,} products, {order_count:,} orders")

                return quality_report

        except Exception as e:
            logger.error(f"❌ Error in data quality checks: {str(e)}")
            raise

    def run_etl_pipeline(self):
        """
        Run complete ETL pipeline
        """
        start_time = datetime.now()
        logger.info("🚀 Starting ETL Pipeline...")

        try:
            # Step 1: Extract
            datasets = self.extract_data()

            # Step 2: Transform
            transformed_data = self.transform_data(datasets)

            # Step 3: Load
            self.load_data(transformed_data)

            # Step 4: Quality Checks
            quality_report = self.run_data_quality_checks()

            end_time = datetime.now()
            processing_time = (end_time - start_time).total_seconds()

            logger.info(f"✅ ETL Pipeline completed successfully!")
            logger.info(f"⏱️ Processing time: {processing_time:.2f} seconds")
            logger.info(f"📁 Quality report saved to: {self.processed_dir / 'data_quality_report.json'}")

            return {
                'status': 'success',
                'processing_time_seconds': processing_time,
                'quality_score': quality_report['data_quality_score'],
                'records_processed': quality_report['record_counts']
            }

        except Exception as e:
            logger.error(f"❌ ETL Pipeline failed: {str(e)}")
            return {
                'status': 'failed',
                'error': str(e),
                'processing_time_seconds': (datetime.now() - start_time).total_seconds()
            }

def main():
    """
    Main execution function
    """
    # Configuration - update with your actual connection details
    server = 'localhost'  # or your server name
    database = 'ECommerceAnalytics'
    username = 'your_username'  # or use Windows authentication
    password = 'your_password'

    # Connection string
    connection_string = f'mssql+pyodbc://@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes'

    # Create ETL manager and run pipeline
    etl_manager = ETLManager(connection_string)
    result = etl_manager.run_etl_pipeline()

    if result['status'] == 'success':
        print("\n🎉 ETL Pipeline Results:")
        print(f"   Processing Time: {result['processing_time_seconds']:.2f} seconds")
        print(f"   Quality Score: {result['quality_score']:.1f}%")
        print(f"   Records Processed: {result['records_processed']}")
    else:
        print(f"\n❌ ETL Pipeline Failed: {result['error']}")

if __name__ == "__main__":
    main()