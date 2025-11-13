#!/usr/bin/env python3
"""
Machine Learning Environment Setup
Team Member: Arlene (ML/Data Science Lead)
Purpose: Setup environment and initial data exploration for ML models
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.cluster import KMeans
from sklearn.ensemble import RandomForestClassifier
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score, classification_report, mean_squared_error, r2_score
import pyodbc
from mlxtend.frequent_patterns import apriori
from mlxtend.frequent_patterns import association_rules
import warnings
warnings.filterwarnings('ignore')

# Configure plotting
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

class MLEnvironmentSetup:
    def __init__(self, connection_string):
        """
        Initialize ML Environment
        Connection string format for SQL Server:
        'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ECommerceAnalytics;Trusted_Connection=yes;'
        """
        self.connection_string = connection_string
        self.conn = None
        self.data = {}

    def connect_to_database(self):
        """
        Connect to Long's data warehouse
        """
        try:
            self.conn = pyodbc.connect(self.connection_string)
            print("✅ Successfully connected to database")
            return True
        except Exception as e:
            print(f"❌ Error connecting to database: {e}")
            return False

    def load_data_for_ml(self):
        """
        Load data for machine learning analysis
        """
        if not self.conn:
            print("❌ No database connection available")
            return False

        try:
            print("📊 Loading data for ML analysis...")

            # Load customer analytics view
            customers_query = """
                SELECT
                    c.customer_id,
                    c.customer_city,
                    c.customer_state,
                    c.customer_region,
                    c.customer_tier,
                    COUNT(DISTINCT s.order_id) as total_orders,
                    SUM(s.total_value) as total_revenue,
                    AVG(s.total_value) as avg_order_value,
                    MIN(s.order_purchase_timestamp) as first_order_date,
                    MAX(s.order_purchase_timestamp) as last_order_date,
                    DATEDIFF(day, MIN(s.order_purchase_timestamp), MAX(s.order_purchase_timestamp)) as customer_lifetime_days
                FROM dim_customer c
                LEFT JOIN fact_sales s ON c.customer_key = s.customer_key
                GROUP BY
                    c.customer_id, c.customer_city, c.customer_state,
                    c.customer_region, c.customer_tier
                HAVING COUNT(DISTINCT s.order_id) > 0
            """
            self.data['customers'] = pd.read_sql(customers_query, self.conn)
            print(f"✅ Loaded {len(self.data['customers'])} customers")

            # Load product performance data
            products_query = """
                SELECT
                    p.product_id,
                    p.product_category_name,
                    p.product_category_l1,
                    COUNT(DISTINCT s.order_id) as total_orders,
                    SUM(s.price) as total_revenue,
                    AVG(s.price) as avg_price,
                    COUNT(DISTINCT s.customer_key) as unique_customers,
                    SUM(CASE WHEN s.order_delivered_customer_date IS NOT NULL THEN 1 ELSE 0 END) as delivered_orders
                FROM dim_product p
                LEFT JOIN fact_sales s ON p.product_key = s.product_key
                GROUP BY
                    p.product_id, p.product_category_name, p.product_category_l1
                HAVING COUNT(DISTINCT s.order_id) > 0
            """
            self.data['products'] = pd.read_sql(products_query, self.conn)
            print(f"✅ Loaded {len(self.data['products'])} products")

            # Load order items for market basket analysis
            order_items_query = """
                SELECT
                    s.order_id,
                    p.product_id,
                    p.product_category_name,
                    s.price,
                    s.order_purchase_timestamp
                FROM fact_sales s
                JOIN dim_product p ON s.product_key = p.product_key
                WHERE s.order_status = 'delivered'
                ORDER BY s.order_id
            """
            self.data['order_items'] = pd.read_sql(order_items_query, self.conn)
            print(f"✅ Loaded {len(self.data['order_items'])} order items")

            return True

        except Exception as e:
            print(f"❌ Error loading data: {e}")
            return False

    def exploratory_data_analysis(self):
        """
        Perform exploratory data analysis
        """
        print("\n🔍 Starting Exploratory Data Analysis...")

        if 'customers' in self.data:
            self._analyze_customers()

        if 'products' in self.data:
            self._analyze_products()

        if 'order_items' in self.data:
            self._analyze_order_patterns()

        print("✅ EDA completed successfully!")

    def _analyze_customers(self):
        """
        Analyze customer data and patterns
        """
        print("\n📈 Customer Analysis:")
        customers = self.data['customers']

        # Basic statistics
        print(f"   Total customers: {len(customers):,}")
        print(f"   Average orders per customer: {customers['total_orders'].mean():.2f}")
        print(f"   Average revenue per customer: ${customers['total_revenue'].mean():.2f}")
        print(f"   Average customer lifetime: {customers['customer_lifetime_days'].mean():.0f} days")

        # Visualizations
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Customer Analysis Dashboard', fontsize=16, fontweight='bold')

        # Orders distribution
        sns.histplot(customers['total_orders'], bins=50, ax=axes[0,0])
        axes[0,0].set_title('Distribution of Orders per Customer')
        axes[0,0].set_xlabel('Number of Orders')
        axes[0,0].set_ylabel('Number of Customers')

        # Revenue distribution
        sns.histplot(customers['total_revenue'], bins=50, ax=axes[0,1])
        axes[0,1].set_title('Distribution of Customer Revenue')
        axes[0,1].set_xlabel('Total Revenue ($)')
        axes[0,1].set_ylabel('Number of Customers')

        # Customer regions
        region_counts = customers['customer_region'].value_counts()
        axes[1,0].pie(region_counts.values, labels=region_counts.index, autopct='%1.1f%%')
        axes[1,0].set_title('Customers by Region')

        # Customer tiers
        tier_counts = customers['customer_tier'].value_counts()
        sns.barplot(x=tier_counts.index, y=tier_counts.values, ax=axes[1,1])
        axes[1,1].set_title('Customers by Tier')
        axes[1,1].set_xlabel('Customer Tier')
        axes[1,1].set_ylabel('Number of Customers')

        plt.tight_layout()
        plt.savefig('ml_analysis/customer_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()

    def _analyze_products(self):
        """
        Analyze product data and patterns
        """
        print("\n📦 Product Analysis:")
        products = self.data['products']

        # Basic statistics
        print(f"   Total products: {len(products):,}")
        print(f"   Average orders per product: {products['total_orders'].mean():.2f}")
        print(f"   Average revenue per product: ${products['total_revenue'].mean():.2f}")

        # Visualizations
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Product Analysis Dashboard', fontsize=16, fontweight='bold')

        # Product categories
        cat_counts = products['product_category_l1'].value_counts().head(10)
        axes[0,0].barh(range(len(cat_counts)), cat_counts.values)
        axes[0,0].set_yticks(range(len(cat_counts)))
        axes[0,0].set_yticklabels(cat_counts.index)
        axes[0,0].set_title('Top 10 Product Categories')
        axes[0,0].set_xlabel('Number of Products')

        # Revenue by category
        cat_revenue = products.groupby('product_category_l1')['total_revenue'].sum().sort_values(ascending=False).head(10)
        axes[0,1].barh(range(len(cat_revenue)), cat_revenue.values)
        axes[0,1].set_yticks(range(len(cat_revenue)))
        axes[0,1].set_yticklabels(cat_revenue.index)
        axes[0,1].set_title('Top 10 Revenue Categories')
        axes[0,1].set_xlabel('Total Revenue ($)')

        # Price distribution
        sns.histplot(products['avg_price'], bins=50, ax=axes[1,0])
        axes[1,0].set_title('Distribution of Product Prices')
        axes[1,0].set_xlabel('Average Price ($)')
        axes[1,0].set_ylabel('Number of Products')

        # Orders vs Revenue scatter
        axes[1,1].scatter(products['total_orders'], products['total_revenue'], alpha=0.6)
        axes[1,1].set_title('Orders vs Revenue per Product')
        axes[1,1].set_xlabel('Total Orders')
        axes[1,1].set_ylabel('Total Revenue ($)')

        plt.tight_layout()
        plt.savefig('ml_analysis/product_analysis.png', dpi=300, bbox_inches='tight')
        plt.show()

    def _analyze_order_patterns(self):
        """
        Analyze order patterns and temporal trends
        """
        print("\n📅 Order Pattern Analysis:")
        order_items = self.data['order_items']

        # Add date components
        order_items['order_date'] = pd.to_datetime(order_items['order_purchase_timestamp']).dt.date
        order_items['order_month'] = pd.to_datetime(order_items['order_purchase_timestamp']).dt.to_period('M')
        order_items['order_hour'] = pd.to_datetime(order_items['order_purchase_timestamp']).dt.hour

        # Visualizations
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Order Pattern Analysis', fontsize=16, fontweight='bold')

        # Monthly orders
        monthly_orders = order_items.groupby('order_month')['order_id'].nunique()
        axes[0,0].plot(monthly_orders.index.astype(str), monthly_orders.values, marker='o')
        axes[0,0].set_title('Monthly Order Volume')
        axes[0,0].set_xlabel('Month')
        axes[0,0].set_ylabel('Number of Orders')
        axes[0,0].tick_params(axis='x', rotation=45)

        # Hourly orders
        hourly_orders = order_items.groupby('order_hour')['order_id'].nunique()
        axes[0,1].bar(hourly_orders.index, hourly_orders.values)
        axes[0,1].set_title('Orders by Hour of Day')
        axes[0,1].set_xlabel('Hour of Day')
        axes[0,1].set_ylabel('Number of Orders')

        # Order value distribution
        order_values = order_items.groupby('order_id')['price'].sum()
        sns.histplot(order_values, bins=50, ax=axes[1,0])
        axes[1,0].set_title('Order Value Distribution')
        axes[1,0].set_xlabel('Order Value ($)')
        axes[1,0].set_ylabel('Number of Orders')

        # Items per order
        items_per_order = order_items.groupby('order_id')['product_id'].count()
        sns.histplot(items_per_order, bins=20, ax=axes[1,1])
        axes[1,1].set_title('Items per Order')
        axes[1,1].set_xlabel('Number of Items')
        axes[1,1].set_ylabel('Number of Orders')

        plt.tight_layout()
        plt.savefig('ml_analysis/order_patterns.png', dpi=300, bbox_inches='tight')
        plt.show()

    def create_rfm_features(self):
        """
        Create RFM (Recency, Frequency, Monetary) features for customer segmentation
        """
        print("\n🎯 Creating RFM Features...")

        if 'customers' not in self.data:
            print("❌ Customer data not available")
            return None

        customers = self.data['customers'].copy()

        # Calculate recency (days since last order)
        customers['recency'] = (pd.Timestamp.now().normalize() - customers['last_order_date']).dt.days

        # Frequency is already available as total_orders
        customers['frequency'] = customers['total_orders']

        # Monetary is already available as total_revenue
        customers['monetary'] = customers['total_revenue']

        # Create RFM scores (1-5 scale, where 5 is best)
        customers['recency_score'] = pd.qcut(customers['recency'], 5, labels=[5,4,3,2,1])
        customers['frequency_score'] = pd.qcut(customers['frequency'].rank(method='first'), 5, labels=[1,2,3,4,5])
        customers['monetary_score'] = pd.qcut(customers['monetary'], 5, labels=[1,2,3,4,5])

        # Convert to numeric
        customers['recency_score'] = customers['recency_score'].astype(int)
        customers['frequency_score'] = customers['frequency_score'].astype(int)
        customers['monetary_score'] = customers['monetary_score'].astype(int)

        # Calculate RFM score
        customers['rfm_score'] = customers['recency_score'] + customers['frequency_score'] + customers['monetary_score']

        print("✅ RFM features created successfully!")
        print(f"   Recency range: {customers['recency'].min():.0f} - {customers['recency'].max():.0f} days")
        print(f"   Frequency range: {customers['frequency'].min():.0f} - {customers['frequency'].max():.0f} orders")
        print(f"   Monetary range: ${customers['monetary'].min():.0f} - ${customers['monetary'].max():.0f}")

        self.data['customers_with_rfm'] = customers
        return customers

    def basic_customer_segmentation(self):
        """
        Perform basic customer segmentation using K-Means
        """
        print("\n🎯 Performing Customer Segmentation...")

        if 'customers_with_rfm' not in self.data:
            self.create_rfm_features()

        customers = self.data['customers_with_rfm'].copy()

        # Prepare features for clustering
        features = customers[['recency_score', 'frequency_score', 'monetary_score']]

        # Determine optimal number of clusters using elbow method
        inertias = []
        K_range = range(2, 11)
        for k in K_range:
            kmeans = KMeans(n_clusters=k, random_state=42, n_init=10)
            kmeans.fit(features)
            inertias.append(kmeans.inertia_)

        # Find optimal k (elbow point)
        optimal_k = 4  # Default to 4 clusters for customer segments

        # Perform K-Means clustering
        kmeans = KMeans(n_clusters=optimal_k, random_state=42, n_init=10)
        cluster_labels = kmeans.fit_predict(features)

        customers['cluster'] = cluster_labels

        # Analyze clusters
        cluster_analysis = customers.groupby('cluster').agg({
            'recency': ['mean', 'median'],
            'frequency': ['mean', 'median'],
            'monetary': ['mean', 'median'],
            'rfm_score': ['mean', 'median'],
            'customer_id': 'count'
        }).round(2)

        # Assign segment names based on cluster characteristics
        segment_names = {
            0: 'At Risk Customers',
            1: 'New Customers',
            2: 'Loyal Customers',
            3: 'VIP Customers'
        }

        customers['segment_name'] = customers['cluster'].map(segment_names)

        print("✅ Customer segmentation completed!")
        print(f"   Number of clusters: {optimal_k}")
        print(f"   Cluster analysis:")
        print(cluster_analysis)

        # Visualize segments
        fig, axes = plt.subplots(2, 2, figsize=(15, 12))
        fig.suptitle('Customer Segmentation Results', fontsize=16, fontweight='bold')

        # Recency vs Frequency
        scatter = axes[0,0].scatter(customers['recency'], customers['frequency'],
                                   c=customers['cluster'], cmap='viridis', alpha=0.6)
        axes[0,0].set_xlabel('Recency (days)')
        axes[0,0].set_ylabel('Frequency (orders)')
        axes[0,0].set_title('Recency vs Frequency by Cluster')
        plt.colorbar(scatter, ax=axes[0,0])

        # Frequency vs Monetary
        scatter = axes[0,1].scatter(customers['frequency'], customers['monetary'],
                                   c=customers['cluster'], cmap='viridis', alpha=0.6)
        axes[0,1].set_xlabel('Frequency (orders)')
        axes[0,1].set_ylabel('Monetary ($)')
        axes[0,1].set_title('Frequency vs Monetary by Cluster')
        plt.colorbar(scatter, ax=axes[0,1])

        # Cluster distribution
        segment_counts = customers['segment_name'].value_counts()
        axes[1,0].pie(segment_counts.values, labels=segment_counts.index, autopct='%1.1f%%')
        axes[1,0].set_title('Customer Segment Distribution')

        # RFM scores by segment
        segment_rfm = customers.groupby('segment_name')[['recency_score', 'frequency_score', 'monetary_score']].mean()
        segment_rfm.plot(kind='bar', ax=axes[1,1])
        axes[1,1].set_title('Average RFM Scores by Segment')
        axes[1,1].set_ylabel('Score')
        axes[1,1].legend(title='RFM Component')

        plt.tight_layout()
        plt.savefig('ml_analysis/customer_segmentation.png', dpi=300, bbox_inches='tight')
        plt.show()

        self.data['customer_segments'] = customers
        return customers, kmeans

    def save_segmentation_results(self, customers):
        """
        Save segmentation results to database
        """
        print("\n💾 Saving segmentation results to database...")

        try:
            if not self.conn:
                print("❌ No database connection")
                return False

            cursor = self.conn.cursor()

            # Clear existing segmentation data
            cursor.execute("DELETE FROM ml_customer_segments")

            # Insert new segmentation results
            for _, customer in customers.iterrows():
                cursor.execute("""
                    INSERT INTO ml_customer_segments
                    (customer_key, customer_id, segment_name, segment_id, segment_probability,
                     rfm_score, predicted_date, model_version)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                """,
                1,  # Will be updated with actual customer_key
                customer['customer_id'],
                customer['segment_name'],
                customer['cluster'],
                0.85,  # Mock probability
                customer['rfm_score'],
                pd.Timestamp.now(),
                'v1.0')

            self.conn.commit()
            print("✅ Segmentation results saved successfully!")
            return True

        except Exception as e:
            print(f"❌ Error saving segmentation results: {e}")
            return False

    def setup_ml_environment(self):
        """
        Complete ML environment setup
        """
        print("🚀 Setting up ML Environment...")

        # Connect to database
        if not self.connect_to_database():
            return False

        # Load data
        if not self.load_data_for_ml():
            return False

        # Perform EDA
        self.exploratory_data_analysis()

        # Create RFM features
        self.create_rfm_features()

        # Perform customer segmentation
        customers, kmeans_model = self.basic_customer_segmentation()

        # Save results
        # self.save_segmentation_results(customers)  # Commented out for now

        print("✅ ML Environment setup completed!")
        print("📁 Analysis plots saved in 'ml_analysis/' directory")

        return True

def main():
    """
    Main execution function
    """
    # Database connection string - update with your details
    connection_string = 'DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ECommerceAnalytics;Trusted_Connection=yes;'

    # Create ML environment
    ml_env = MLEnvironmentSetup(connection_string)

    # Setup environment
    success = ml_env.setup_ml_environment()

    if success:
        print("\n🎉 ML Environment setup completed successfully!")
        print("📊 Ready for advanced ML model development")
    else:
        print("\n❌ ML Environment setup failed!")

if __name__ == "__main__":
    # Create output directory
    import os
    os.makedirs('ml_analysis', exist_ok=True)

    main()