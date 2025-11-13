-- E-Commerce Analytics Platform Database Schema
-- Team Member: Long (Data Engineer)
-- Purpose: Data Warehouse for Analytics and ML

-- Drop existing objects if they exist (for development)
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;
DROP TABLE IF EXISTS dim_seller;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_location;

-- ========================================
-- DIMENSION TABLES
-- ========================================

-- Customer Dimension Table
CREATE TABLE dim_customer (
    customer_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_id VARCHAR(50) UNIQUE NOT NULL,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(10),
    customer_region VARCHAR(50),
    customer_segment VARCHAR(50), -- Will be populated by ML model
    customer_tier VARCHAR(20), -- Bronze, Silver, Gold, Platinum
    is_active BIT DEFAULT 1,
    created_date DATETIME DEFAULT GETDATE(),
    updated_date DATETIME DEFAULT GETDATE()
);

-- Product Dimension Table
CREATE TABLE dim_product (
    product_key INT IDENTITY(1,1) PRIMARY KEY,
    product_id VARCHAR(50) UNIQUE NOT NULL,
    product_category_name VARCHAR(100),
    product_category_l1 VARCHAR(50), -- Main category
    product_category_l2 VARCHAR(50), -- Sub category
    product_name_length INT,
    product_description_length INT,
    product_photos_qty INT,
    product_weight_g DECIMAL(10,2),
    product_length_cm DECIMAL(8,2),
    product_height_cm DECIMAL(8,2),
    product_width_cm DECIMAL(8,2),
    product_volume_cm3 DECIMAL(12,2),
    product_density_g_cm3 DECIMAL(8,4),
    price_range VARCHAR(20), -- Low, Medium, High, Premium
    is_active BIT DEFAULT 1,
    created_date DATETIME DEFAULT GETDATE(),
    updated_date DATETIME DEFAULT GETDATE()
);

-- Seller Dimension Table
CREATE TABLE dim_seller (
    seller_key INT IDENTITY(1,1) PRIMARY KEY,
    seller_id VARCHAR(50) UNIQUE NOT NULL,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(100),
    seller_state VARCHAR(10),
    seller_region VARCHAR(50),
    seller_tier VARCHAR(20), -- Basic, Premium, Enterprise
    seller_rating DECIMAL(3,2), -- Average rating
    is_active BIT DEFAULT 1,
    created_date DATETIME DEFAULT GETDATE(),
    updated_date DATETIME DEFAULT GETDATE()
);

-- Date Dimension Table
CREATE TABLE dim_date (
    date_key INT PRIMARY KEY,
    full_date DATE NOT NULL,
    day_number INT,
    day_name VARCHAR(10),
    day_suffix VARCHAR(2),
    week_number INT,
    week_name VARCHAR(10),
    month_number INT,
    month_name VARCHAR(20),
    quarter_number INT,
    quarter_name VARCHAR(10),
    year_number INT,
    is_weekend BIT,
    is_holiday BIT,
    season_name VARCHAR(20),
    fiscal_quarter INT,
    fiscal_year INT
);

-- Location Dimension Table
CREATE TABLE dim_location (
    location_key INT IDENTITY(1,1) PRIMARY KEY,
    zip_code_prefix VARCHAR(10),
    city VARCHAR(100),
    state VARCHAR(50),
    state_abbr VARCHAR(10),
    region VARCHAR(50),
    country VARCHAR(50) DEFAULT 'Brazil',
    latitude DECIMAL(10,8),
    longitude DECIMAL(11,8),
    population INT,
    gdp_per_capita DECIMAL(12,2)
);

-- ========================================
-- FACT TABLES
-- ========================================

-- Sales Fact Table (Main fact table)
CREATE TABLE fact_sales (
    sales_key BIGINT IDENTITY(1,1) PRIMARY KEY,
    order_id VARCHAR(50) NOT NULL,
    order_item_id INT NOT NULL,
    customer_key INT FOREIGN KEY REFERENCES dim_customer(customer_key),
    product_key INT FOREIGN KEY REFERENCES dim_product(product_key),
    seller_key INT FOREIGN KEY REFERENCES dim_seller(seller_key),
    order_date_key INT FOREIGN KEY REFERENCES dim_date(date_key),
    delivery_date_key INT FOREIGN KEY REFERENCES dim_date(date_key),
    location_key INT FOREIGN KEY REFERENCES dim_location(location_key),

    -- Order Information
    order_status VARCHAR(20),
    price DECIMAL(12,2) NOT NULL,
    freight_value DECIMAL(12,2) NOT NULL,
    total_value DECIMAL(12,2) NOT NULL, -- price + freight_value
    payment_installments INT,

    -- Time Dimensions
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,

    -- Delivery Metrics
    delivery_delay_days INT, -- Actual vs Estimated delivery
    processing_time_hours INT, -- Order approval time

    -- Performance Metrics
    is_delivered_ontime BIT,
    is_fast_delivery BIT, -- Delivered earlier than estimated

    -- Audit Fields
    created_date DATETIME DEFAULT GETDATE(),
    updated_date DATETIME DEFAULT GETDATE()
);

-- ========================================
-- ML RESULTS TABLES (for Arlene's ML models)
-- ========================================

-- Customer Segmentation Results
CREATE TABLE ml_customer_segments (
    segment_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_key INT FOREIGN KEY REFERENCES dim_customer(customer_key),
    customer_id VARCHAR(50),
    segment_name VARCHAR(50), -- 'High Value', 'At Risk', 'New', 'Loyal', etc.
    segment_id INT, -- Cluster number
    segment_probability DECIMAL(5,4), -- Confidence score
    rfm_score INT, -- Recency, Frequency, Monetary score
    predicted_date DATETIME DEFAULT GETDATE(),
    model_version VARCHAR(20),
    created_date DATETIME DEFAULT GETDATE()
);

-- Customer Churn Prediction Results
CREATE TABLE ml_churn_predictions (
    churn_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_key INT FOREIGN KEY REFERENCES dim_customer(customer_key),
    customer_id VARCHAR(50),
    churn_probability DECIMAL(5,4), -- 0.0000 to 1.0000
    churn_risk_level VARCHAR(20), -- 'Low', 'Medium', 'High', 'Critical'
    prediction_date DATETIME DEFAULT GETDATE(),
    model_version VARCHAR(20),
    feature_importance JSON, -- Top features contributing to prediction
    created_date DATETIME DEFAULT GETDATE()
);

-- Customer Lifetime Value Predictions
CREATE TABLE ml_clv_predictions (
    clv_key INT IDENTITY(1,1) PRIMARY KEY,
    customer_key INT FOREIGN KEY REFERENCES dim_customer(customer_key),
    customer_id VARCHAR(50),
    predicted_clv_12m DECIMAL(12,2), -- Next 12 months
    predicted_clv_24m DECIMAL(12,2), -- Next 24 months
    current_clv DECIMAL(12,2), -- Historical CLV
    clv_tier VARCHAR(20), -- Based on predicted CLV
    prediction_date DATETIME DEFAULT GETDATE(),
    model_version VARCHAR(20),
    created_date DATETIME DEFAULT GETDATE()
);

-- Market Basket Analysis Rules
CREATE TABLE ml_market_basket_rules (
    rule_key INT IDENTITY(1,1) PRIMARY KEY,
    antecedent_products VARCHAR(500), -- Product IDs bought together
    consequent_products VARCHAR(500), -- Products recommended
    support_value DECIMAL(8,6), -- Support metric
    confidence_value DECIMAL(8,6), -- Confidence metric
    lift_value DECIMAL(8,4), -- Lift metric
    conviction_value DECIMAL(8,4), -- Conviction metric
    rule_strength VARCHAR(20), -- 'Weak', 'Moderate', 'Strong'
    created_date DATETIME DEFAULT GETDATE(),
    last_updated DATETIME DEFAULT GETDATE()
);

-- ========================================
-- INDEXES FOR PERFORMANCE
-- ========================================

-- Fact Table Indexes
CREATE INDEX idx_fact_sales_customer_key ON fact_sales(customer_key);
CREATE INDEX idx_fact_sales_product_key ON fact_sales(product_key);
CREATE INDEX idx_fact_sales_seller_key ON fact_sales(seller_key);
CREATE INDEX idx_fact_sales_order_date_key ON fact_sales(order_date_key);
CREATE INDEX idx_fact_sales_order_id ON fact_sales(order_id);
CREATE INDEX idx_fact_sales_order_status ON fact_sales(order_status);

-- Dimension Table Indexes
CREATE INDEX idx_dim_customer_customer_id ON dim_customer(customer_id);
CREATE INDEX idx_dim_product_product_id ON dim_product(product_id);
CREATE INDEX idx_dim_product_category ON dim_product(product_category_name);
CREATE INDEX idx_dim_seller_seller_id ON dim_seller(seller_id);

-- ML Results Indexes
CREATE INDEX idx_ml_segments_customer_key ON ml_customer_segments(customer_key);
CREATE INDEX idx_ml_churn_customer_key ON ml_churn_predictions(customer_key);
CREATE INDEX idx_ml_clv_customer_key ON ml_clv_predictions(customer_key);

-- ========================================
-- VIEWS FOR ANALYTICS
-- ========================================

-- Customer Analytics View
CREATE VIEW v_customer_analytics AS
SELECT
    c.customer_key,
    c.customer_id,
    c.customer_city,
    c.customer_state,
    c.customer_segment,
    c.customer_tier,
    COUNT(DISTINCT s.order_id) as total_orders,
    SUM(s.total_value) as total_revenue,
    AVG(s.total_value) as avg_order_value,
    MIN(s.order_purchase_timestamp) as first_order_date,
    MAX(s.order_purchase_timestamp) as last_order_date,
    DATEDIFF(day, MIN(s.order_purchase_timestamp), MAX(s.order_purchase_timestamp)) as customer_lifetime_days,
    cs.churn_probability,
    cs.churn_risk_level,
    clv.predicted_clv_12m,
    clv.current_clv
FROM dim_customer c
LEFT JOIN fact_sales s ON c.customer_key = s.customer_key
LEFT JOIN ml_churn_predictions cs ON c.customer_key = cs.customer_key
LEFT JOIN ml_clv_predictions clv ON c.customer_key = clv.customer_key
GROUP BY
    c.customer_key, c.customer_id, c.customer_city, c.customer_state,
    c.customer_segment, c.customer_tier, cs.churn_probability,
    cs.churn_risk_level, clv.predicted_clv_12m, clv.current_clv;

-- Product Performance View
CREATE VIEW v_product_performance AS
SELECT
    p.product_key,
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
    p.product_key, p.product_id, p.product_category_name, p.product_category_l1;

-- Time Series Sales View
CREATE VIEW v_time_series_sales AS
SELECT
    d.full_date,
    d.day_name,
    d.week_name,
    d.month_name,
    d.quarter_name,
    d.year_number,
    COUNT(DISTINCT s.order_id) as total_orders,
    SUM(s.total_value) as total_revenue,
    AVG(s.total_value) as avg_order_value,
    COUNT(DISTINCT s.customer_key) as unique_customers
FROM dim_date d
LEFT JOIN fact_sales s ON d.date_key = s.order_date_key
GROUP BY
    d.full_date, d.day_name, d.week_name, d.month_name,
    d.quarter_name, d.year_number
ORDER BY d.full_date;

-- ========================================
-- STORED PROCEDURES
-- ========================================

-- Procedure to populate date dimension
CREATE PROCEDURE sp_populate_date_dimension
    @start_date DATE = '2020-01-01',
    @end_date DATE = '2025-12-31'
AS
BEGIN
    SET NOCOUNT ON;

    DECLARE @current_date DATE = @start_date;

    WHILE @current_date <= @end_date
    BEGIN
        INSERT INTO dim_date (
            date_key, full_date, day_number, day_name, day_suffix,
            week_number, week_name, month_number, month_name,
            quarter_number, quarter_name, year_number,
            is_weekend, is_holiday, season_name,
            fiscal_quarter, fiscal_year
        )
        VALUES (
            CONVERT(INT, REPLACE(CONVERT(VARCHAR, @current_date, 112), '-', '')),
            @current_date,
            DAY(@current_date),
            DATENAME(WEEKDAY, @current_date),
            CASE
                WHEN DAY(@current_date) IN (1,21,31) THEN 'st'
                WHEN DAY(@current_date) IN (2,22) THEN 'nd'
                WHEN DAY(@current_date) IN (3,23) THEN 'rd'
                ELSE 'th'
            END,
            DATEPART(WEEK, @current_date),
            'Week ' + CAST(DATEPART(WEEK, @current_date) AS VARCHAR),
            MONTH(@current_date),
            DATENAME(MONTH, @current_date),
            DATEPART(QUARTER, @current_date),
            'Q' + CAST(DATEPART(QUARTER, @current_date) AS VARCHAR),
            YEAR(@current_date),
            CASE WHEN DATENAME(WEEKDAY, @current_date) IN ('Saturday', 'Sunday') THEN 1 ELSE 0 END,
            0, -- Can be updated with actual holiday data
            CASE
                WHEN MONTH(@current_date) IN (12,1,2) THEN 'Winter'
                WHEN MONTH(@current_date) IN (3,4,5) THEN 'Spring'
                WHEN MONTH(@current_date) IN (6,7,8) THEN 'Summer'
                ELSE 'Fall'
            END,
            CASE
                WHEN MONTH(@current_date) IN (1,2,3) THEN 4
                WHEN MONTH(@current_date) IN (4,5,6) THEN 1
                WHEN MONTH(@current_date) IN (7,8,9) THEN 2
                ELSE 3
            END,
            YEAR(@current_date) + CASE
                WHEN MONTH(@current_date) <= 3 THEN 0
                ELSE 1
            END
        );

        SET @current_date = DATEADD(DAY, 1, @current_date);
    END;
END;

-- ========================================
-- SAMPLE DATA INSERT (for testing)
-- ========================================

-- Populate date dimension with sample dates
EXEC sp_populate_date_dimension '2022-01-01', '2024-12-31';

PRINT "✅ Database schema created successfully!"
PRINT "📊 Tables: dim_customer, dim_product, dim_seller, dim_date, dim_location, fact_sales"
PRINT "🤖 ML Tables: ml_customer_segments, ml_churn_predictions, ml_clv_predictions, ml_market_basket_rules"
PRINT "📈 Views: v_customer_analytics, v_product_performance, v_time_series_sales"