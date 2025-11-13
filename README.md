# DevOps Pipeline for The Gioi Di Dong Customer Data Analytics Platform

COS40006 - Computing Technology Project
Semester 1, 2024-2025
Swinburne University of Technology

## Team Members - Group 4

- Nguyen Ngoc Anh (104977768)
- Le Hoang Long (104845140)
- Nguyen Thien Phuoc (105028625)
- Arlene Phuong Brown (104504111)

## Project Overview

An automated DevOps pipeline for deploying a Customer Data Analytics Platform for The Gioi Di Dong, Vietnam's leading electronics retailer. The platform provides ML-powered insights through customer segmentation, churn prediction, and interactive dashboards.

## Technology Stack

- **Backend API**: ASP.NET Core 9.0
- **Database**: SQLite (ecommerce_analytics.db)
- **ML Framework**: Python with scikit-learn and XGBoost
- **Business Intelligence**: Tableau
- **CI/CD**: GitHub Actions
- **Containerization**: Docker
- **Cloud Platform**: Microsoft Azure
- **Monitoring**: Azure Monitor

## System Components

1. **Data Warehouse**: SQLite database with 5,000 customers and 500 sellers
2. **REST API**: ASP.NET Core Web API on port 5001
3. **ML Models**: Customer segmentation (K-Means), churn prediction (XGBoost), lifetime value estimation
4. **Dashboards**: Tableau visualizations for business insights

## Data Specifications

- Customer records: 5,000
- Seller records: 500
- Customer tiers: Silver (45%), Gold (30%), Platinum (15%), Diamond (8%), VIP (2%)
- Phone format: 09xxxxxxxx (Vietnamese mobile numbers)
- Email domains: @customer.tgdd.vn (customers), @thegioididong.com (sellers)
- Geographic coverage: 27 Vietnamese cities across 4 regions

## Setup Instructions

### Prerequisites

- .NET 9.0 SDK
- Python 3.9+
- Docker Desktop
- SQLite

### Running the API

```bash
cd ECommerceAnalytics.Api
dotnet restore
dotnet run
```

API will be available at: http://localhost:5001

### Running Data Generation

```bash
python 1_dataset_download.py
```

### Database Setup

```bash
sqlite3 ecommerce_analytics.db < 2_database_schema.sql
```

### ETL Pipeline

```bash
python 3_etl_pipeline.py
```

## Project Structure

```
.
├── 1_dataset_download.py          # Data generation script
├── 2_database_schema.sql           # Database schema
├── 3_etl_pipeline.py               # ETL pipeline
├── 5_ml_environment_setup.py       # ML environment setup
├── ecommerce_analytics.db          # SQLite database
├── data/                           # Data files
│   ├── raw/                        # Raw CSV files
│   └── processed/                  # Processed data
├── ECommerceAnalytics.Api/         # ASP.NET Core API
└── ml_analysis/                    # ML models and outputs
```

## API Endpoints

Key endpoints available at http://localhost:5001:

- `GET /api/customers` - List all customers with pagination
- `GET /api/customers/{id}` - Get customer details
- `GET /api/customers/tier/{tier}` - Filter by customer tier
- `GET /api/customers/city/{city}` - Filter by city
- `GET /swagger` - API documentation

## Data Quality

- Quality score: 98.5/100
- Phone validation: 100% compliance with 09xxxxxxxx format
- Email validation: 100% compliance with domain standards
- Data integrity: All foreign key relationships validated

## Success Metrics

- Deployment time: Target < 20 minutes (from 6 hours manual)
- Deployment success rate: Target 95% (from 70%)
- Code coverage: Target 75% overall
- MTTR: Target < 10 minutes (from 2-3 hours)

## Documentation

Additional documentation available in the project brief (COS40006_ProjectBrief_Team 4 (2).pdf).

## License

Academic project for COS40006 - Computing Technology Project
