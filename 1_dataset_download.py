#!/usr/bin/env python3
"""
Vietnamese E-Commerce Dataset Generation Script
E-Commerce Analytics Project for The Gioi Di Dong
Team Member: Long (Data Engineer)

This script generates realistic Vietnamese e-commerce data including:
- 5,000 Vietnamese customers with ASCII names (no diacritics)
- 500 Vietnamese sellers with @thegioididong.com emails
- Complete contact information (phone, email)
- Vietnamese geographic data (cities, regions)
- Customer tier segmentation (Silver, Gold, Platinum, Diamond, VIP)
"""

import pandas as pd
import numpy as np
import os
import random
import unicodedata
from pathlib import Path
from datetime import datetime, timedelta

class VietnameseDatasetGenerator:
    def __init__(self, data_dir="data/raw"):
        self.data_dir = Path(data_dir)
        self.data_dir.mkdir(parents=True, exist_ok=True)

    def remove_vietnamese_diacritics(self, text):
        """Remove Vietnamese diacritics while keeping base characters"""
        if not text:
            return text

        # Normalize Unicode
        nfkd_form = unicodedata.normalize('NFD', text)
        ascii_text = ''.join([c for c in nfkd_form if not unicodedata.combining(c)])

        # Handle specific Vietnamese characters
        diacritic_map = {
            'Đ': 'D', 'đ': 'd',
            'Ă': 'A', 'ă': 'a',
            'Â': 'A', 'â': 'a',
            'Ê': 'E', 'ê': 'e',
            'Ô': 'O', 'ô': 'o',
            'Ơ': 'O', 'ơ': 'o',
            'Ư': 'U', 'ư': 'u'
        }

        for vietnamese, ascii_char in diacritic_map.items():
            ascii_text = ascii_text.replace(vietnamese, ascii_char)

        return ascii_text

    def generate_vietnamese_name(self, is_male=None):
        """Generate Vietnamese name with proper ASCII conversion"""
        vietnamese_family_names = [
            "Nguyễn", "Trần", "Lê", "Phạm", "Hoàng", "Vũ", "Đỗ", "Bùi", "Đinh", "Ngô",
            "Dương", "Võ", "Phan", "Trương", "Lý", "Cao", "Trịnh", "Mai", "Lâm", "Tôn",
            "Hà", "Quách", "Thôi", "Kiều", "Đàng", "Lưu", "Triệu", "Tô", "Đặng",
            "Đoàn", "Hồ", "Ứng", "Vi", "Kim", "Ong", "Thái", "Yến", "Chu"
        ]

        vietnamese_middle_names_male = [
            "Văn", "Hữu", "Quốc", "Minh", "Đình", "Thanh", "Hoàng", "Gia", "Thế", "Trung",
            "Xuân", "Hải", "Nam", "Anh", "Tùng", "Cường", "Duy", "Hùng", "Quân", "Phong",
            "Sơn", "Hào", "Kiệt", "Mạnh", "Phúc", "Lộc", "Thành", "Thiên", "Nhật", "Long"
        ]

        vietnamese_middle_names_female = [
            "Thị", "Ngọc", "Thanh", "Thúy", "Hoài", "Quỳnh", "Lan", "Hương", "Linh",
            "Thu", "Oanh", "Yến", "Anh", "Tuyết", "Diễm", "My", "Lệ", "Kim", "Xuân",
            "Hà", "Giang", "Ngân", "Vy", "Trang", "Phương", "Quyên", "Thảo", "Duyên", "Chi"
        ]

        vietnamese_first_names_male = [
            "An", "Bảo", "Bình", "Chiến", "Chương", "Dũng", "Đại", "Đạt", "Đức", "Giàu",
            "Hào", "Hiếu", "Hoài", "Hùng", "Huy", "Khang", "Khiêm", "Kiên", "Khoa", "Khôi",
            "Linh", "Long", "Luân", "Mạnh", "Nam", "Nghĩa", "Nhân", "Phi", "Phong", "Phúc",
            "Quốc", "Quân", "Quyết", "Sáng", "Sơn", "Strong", "Tài", "Tâm", "Tấn", "Thái",
            "Thành", "Thiên", "Thiện", "Thắng", "Thịnh", "Tiến", "Toàn", "Trí", "Trung",
            "Trường", "Tú", "Tuấn", "Tùng", "Vinh", "Vũ", "Xuân", "Yến"
        ]

        vietnamese_first_names_female = [
            "An", "Anh", "Ái", "Băng", "Bi", "Chi", "Chinh", "Dung", "Diễm", "Giang",
            "Gia", "Hà", "Hân", "Hiền", "Hoài", "Hoa", "Hồng", "Huyền", "Hương", "Khánh",
            "Kiều", "Lam", "Lan", "Linh", "Loan", "Lộc", "Lựu", "Ly", "Mai", "Mẫn",
            "My", "Nga", "Nghi", "Ngọc", "Nhi", "Như", "Oanh", "Phong", "Phượng", "Quyên",
            "Quỳnh", "Sao", "Sương", "Thu", "Thảo", "Thủy", "Tiên", "Trang", "Trinh", "Trúc",
            "Tú", "Tuyền", "Tường", "Vy", "Xuân", "Yến", "Ánh", "Bảo", "Cúc"
        ]

        if is_male is None:
            is_male = random.choice([True, False])

        family_name = random.choice(vietnamese_family_names)

        if is_male:
            middle_name = random.choice(vietnamese_middle_names_male)
            first_name = random.choice(vietnamese_first_names_male)
        else:
            middle_name = random.choice(vietnamese_middle_names_female)
            first_name = random.choice(vietnamese_first_names_female)

        vietnamese_name = f"{family_name} {middle_name} {first_name}"
        ascii_name = self.remove_vietnamese_diacritics(vietnamese_name)

        # Capitalize properly
        parts = ascii_name.split()
        ascii_name = ' '.join([part[0].upper() + part[1:].lower() if len(part) > 1 else part.upper() for part in parts])

        return ascii_name, is_male

    def generate_customers_dataset(self, num_customers=5000):
        """Generate Vietnamese customers dataset"""
        print(f"Generating {num_customers:,} Vietnamese customers...")

        # Vietnamese cities with regions
        vietnamese_cities = [
            {"city": "Hanoi", "state": "Hanoi", "region": "North"},
            {"city": "Ho Chi Minh City", "state": "HCMC", "region": "South"},
            {"city": "Da Nang", "state": "Da Nang", "region": "Central"},
            {"city": "Can Tho", "state": "Can Tho", "region": "South"},
            {"city": "Hai Phong", "state": "Hai Phong", "region": "North"},
            {"city": "Bien Hoa", "state": "Dong Nai", "region": "South"},
            {"city": "Buon Ma Thuot", "state": "Dak Lak", "region": "Central Highlands"},
            {"city": "Hue", "state": "Thua Thien Hue", "region": "Central"},
            {"city": "Vung Tau", "state": "Ba Ria-Vung Tau", "region": "South"},
            {"city": "Thu Dau Mot", "state": "Binh Duong", "region": "South"},
            {"city": "Rach Gia", "state": "Kien Giang", "region": "South"},
            {"city": "Long Xuyen", "state": "An Giang", "region": "South"},
            {"city": "Thai Nguyen", "state": "Thai Nguyen", "region": "North"},
            {"city": "Thanh Hoa", "state": "Thanh Hoa", "region": "North"},
            {"city": "Nam Dinh", "state": "Nam Dinh", "region": "North"},
            {"city": "Quang Ninh", "state": "Quang Ninh", "region": "North"},
            {"city": "Da Lat", "state": "Lam Dong", "region": "Central Highlands"},
            {"city": "Nha Trang", "state": "Khanh Hoa", "region": "Central"},
            {"city": "Phan Thiet", "state": "Binh Thuan", "region": "Central"},
            {"city": "Quy Nhon", "state": "Binh Dinh", "region": "Central"},
            {"city": "Pleiku", "state": "Gia Lai", "region": "Central Highlands"},
            {"city": "Tuy Hoa", "state": "Phu Yen", "region": "Central"},
            {"city": "Tam Ky", "state": "Quang Nam", "region": "Central"},
            {"city": "Ha Tinh", "state": "Ha Tinh", "region": "North"},
            {"city": "Vinh", "state": "Nghe An", "region": "North"},
            {"city": "Dong Ha", "state": "Quang Tri", "region": "Central"},
            {"city": "Dong Hoi", "state": "Quang Binh", "region": "North"}
        ]

        customer_tiers = ["Silver", "Gold", "Platinum", "Diamond", "VIP"]
        tier_weights = [0.45, 0.30, 0.15, 0.08, 0.02]

        customers = []
        for i in range(num_customers):
            name, is_male = self.generate_vietnamese_name()
            city_data = random.choice(vietnamese_cities)
            tier = random.choices(customer_tiers, weights=tier_weights)[0]
            is_active = random.choices([True, False], weights=[0.85, 0.15])[0]

            # Generate Vietnamese phone (10 digits: 09xxxxxxxx)
            phone = f"09{random.randint(10000000, 99999999):08d}"

            # Generate email
            name_parts = name.lower().replace(' ', '.')
            email = f"{name_parts}{random.randint(1, 999):03d}@customer.tgdd.vn"

            # Generate created date (last 3 years)
            days_ago = random.randint(1, 1095)
            created_date = (datetime.now() - timedelta(days=days_ago)).strftime('%Y-%m-%dT%H:%M:%S')

            customers.append({
                'customer_id': f'KH{i+1:06d}',
                'customer_unique_id': f'UNIQUE{i+1:07d}',
                'customer_name': name,
                'customer_phone': phone,
                'customer_email': email,
                'customer_city': city_data['city'],
                'customer_state': city_data['state'],
                'customer_region': city_data['region'],
                'customer_tier': tier,
                'is_active': is_active,
                'created_date': created_date
            })

        customers_df = pd.DataFrame(customers)
        file_path = self.data_dir / 'customers.csv'
        customers_df.to_csv(file_path, index=False)
        print(f"   Created customers.csv ({len(customers_df):,} rows)")

        return customers_df

    def generate_sellers_dataset(self, num_sellers=500):
        """Generate Vietnamese sellers dataset"""
        print(f"Generating {num_sellers:,} Vietnamese sellers...")

        vietnamese_cities = [
            {"city": "Hanoi", "state": "Hanoi"},
            {"city": "Ho Chi Minh City", "state": "HCMC"},
            {"city": "Da Nang", "state": "Da Nang"},
            {"city": "Can Tho", "state": "Can Tho"},
            {"city": "Hai Phong", "state": "Hai Phong"},
            {"city": "Bien Hoa", "state": "Dong Nai"},
            {"city": "Buon Ma Thuot", "state": "Dak Lak"},
            {"city": "Hue", "state": "Thua Thien Hue"},
            {"city": "Vung Tau", "state": "Ba Ria-Vung Tau"},
            {"city": "Thu Dau Mot", "state": "Binh Duong"},
            {"city": "Rach Gia", "state": "Kien Giang"},
            {"city": "Long Xuyen", "state": "An Giang"},
            {"city": "Thai Nguyen", "state": "Thai Nguyen"},
            {"city": "Thanh Hoa", "state": "Thanh Hoa"},
            {"city": "Nam Dinh", "state": "Nam Dinh"},
            {"city": "Quang Ninh", "state": "Quang Ninh"},
            {"city": "Da Lat", "state": "Lam Dong"},
            {"city": "Nha Trang", "state": "Khanh Hoa"},
            {"city": "Phan Thiet", "state": "Binh Thuan"},
            {"city": "Quy Nhon", "state": "Binh Dinh"},
            {"city": "Pleiku", "state": "Gia Lai"},
            {"city": "Tuy Hoa", "state": "Phu Yen"},
            {"city": "Tam Ky", "state": "Quang Nam"},
            {"city": "Ha Tinh", "state": "Ha Tinh"},
            {"city": "Vinh", "state": "Nghe An"},
            {"city": "Dong Ha", "state": "Quang Tri"},
            {"city": "Dong Hoi", "state": "Quang Binh"}
        ]

        sellers = []
        for i in range(num_sellers):
            seller_name, _ = self.generate_vietnamese_name()
            city_data = random.choice(vietnamese_cities)

            # Generate Vietnamese zip code
            if city_data['state'] == 'Hanoi':
                zip_code = f"1{random.randint(00000, 99999):05d}"
            elif city_data['state'] == 'HCMC':
                zip_code = f"7{random.randint(00000, 99999):05d}"
            else:
                zip_code = f"{random.randint(100000, 999999):06d}"

            # Generate email and phone
            email_prefix = seller_name.lower().replace(' ', '.')
            email = f"{email_prefix}{random.randint(1, 999):03d}@thegioididong.com"
            phone = f"09{random.randint(10000000, 99999999):08d}"

            sellers.append({
                'seller_id': f'seller_{i+1:03d}',
                'seller_name': seller_name,
                'seller_email': email,
                'seller_phone': phone,
                'seller_zip_code_prefix': zip_code,
                'seller_city': city_data['city'],
                'seller_state': city_data['state']
            })

        sellers_df = pd.DataFrame(sellers)
        file_path = self.data_dir / 'sellers.csv'
        sellers_df.to_csv(file_path, index=False)
        print(f"   Created sellers.csv ({len(sellers_df):,} rows)")

        return sellers_df

    def create_data_dictionary(self):
        """Create comprehensive data dictionary for Vietnamese e-commerce data"""
        dictionary = {
            'customers.csv': {
                'description': 'Vietnamese customer information with contact details',
                'columns': {
                    'customer_id': 'Unique customer identifier (KH000000 format)',
                    'customer_unique_id': 'System-generated unique ID (UNIQUE0000000 format)',
                    'customer_name': 'Customer name (Vietnamese ASCII, no diacritics)',
                    'customer_phone': 'Vietnamese mobile phone (09xxxxxxxx format)',
                    'customer_email': 'Customer email (@customer.tgdd.vn domain)',
                    'customer_city': 'Customer city (Vietnamese cities in English)',
                    'customer_state': 'Customer state/province',
                    'customer_region': 'Geographic region (North, South, Central, Central Highlands)',
                    'customer_tier': 'Customer tier (Silver, Gold, Platinum, Diamond, VIP)',
                    'is_active': 'Customer account status (boolean)',
                    'created_date': 'Account creation timestamp (ISO format)'
                }
            },
            'sellers.csv': {
                'description': 'Vietnamese seller information for The Gioi Di Dong',
                'columns': {
                    'seller_id': 'Unique seller identifier (seller_000 format)',
                    'seller_name': 'Seller name (Vietnamese ASCII, no diacritics)',
                    'seller_email': 'Seller email (@thegioididong.com domain)',
                    'seller_phone': 'Seller phone (Vietnamese mobile format)',
                    'seller_zip_code_prefix': 'Vietnamese postal code (6 digits)',
                    'seller_city': 'Seller city location',
                    'seller_state': 'Seller state/province'
                }
            }
        }

        dict_path = self.data_dir.parent / 'data_dictionary.txt'
        with open(dict_path, 'w', encoding='utf-8') as f:
            f.write("VIETNAMESE E-COMMERCE DATA DICTIONARY\n")
            f.write("The Gioi Di Dong Analytics Platform\n")
            f.write("=" * 50 + "\n\n")

            for filename, info in dictionary.items():
                f.write(f"{filename}\n")
                f.write("-" * len(filename) + "\n")
                f.write(f"Description: {info['description']}\n\n")
                f.write("Columns:\n")
                for col, desc in info['columns'].items():
                    f.write(f"  {col}: {desc}\n")
                f.write("\n" + "=" * 50 + "\n\n")

        print(f"   Created data dictionary: {dict_path}")

def main():
    """Main execution function"""
    print("=" * 60)
    print("Vietnamese E-Commerce Dataset Generation")
    print("The Gioi Di Dong Analytics Platform")
    print("=" * 60)
    print()

    generator = VietnameseDatasetGenerator()

    # Generate customers dataset (5,000 records)
    customers_df = generator.generate_customers_dataset(num_customers=5000)

    # Generate sellers dataset (500 records)
    sellers_df = generator.generate_sellers_dataset(num_sellers=500)

    # Create data dictionary
    generator.create_data_dictionary()

    print("\nDataset Summary:")
    print(f"  Customers: {len(customers_df):,} records")
    print(f"  Sellers: {len(sellers_df):,} records")
    print(f"  Data location: {generator.data_dir}")

    print("\nData Quality:")
    print(f"  Vietnamese ASCII names (no diacritics): 100%")
    print(f"  Phone format (09xxxxxxxx): 100%")
    print(f"  Email domains: @customer.tgdd.vn, @thegioididong.com")
    print(f"  Geographic coverage: 27 Vietnamese cities")
    print(f"  Customer tiers: Silver, Gold, Platinum, Diamond, VIP")

    print("\nDataset generation completed successfully!")
    print("Files are ready for ETL pipeline development")

if __name__ == "__main__":
    main()
