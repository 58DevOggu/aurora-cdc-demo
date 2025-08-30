#!/usr/bin/env python3
"""
TPC-H Database Setup Script for Aurora MySQL
Creates 500+ tables with TPC-H data for CDC demo
"""

import argparse
import mysql.connector
import random
import time
from datetime import datetime, timedelta
from decimal import Decimal
import json
import sys
from typing import List, Dict, Any
import concurrent.futures
import threading

class TPCHDatabaseSetup:
    """Setup TPC-H database with multiple tables for CDC demo."""
    
    def __init__(self, host: str, port: int, username: str, password: str, scale: int = 1):
        """Initialize database connection."""
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.scale = scale  # Scale factor for data size
        self.database = "tpch"
        self.connection = None
        self.tables_created = 0
        self.lock = threading.Lock()
        
        # TPC-H base tables
        self.base_tables = [
            "customer", "orders", "lineitem", "part", "supplier",
            "partsupp", "nation", "region"
        ]
        
        # Additional tables to reach 500+ (organized by business domain)
        self.business_domains = {
            "sales": ["sales_transactions", "sales_targets", "sales_performance", "sales_regions", "sales_teams"],
            "inventory": ["inventory_levels", "inventory_movements", "inventory_adjustments", "warehouse_locations", "stock_alerts"],
            "finance": ["invoices", "payments", "credit_notes", "financial_periods", "tax_rates"],
            "logistics": ["shipments", "tracking_events", "carriers", "delivery_routes", "shipping_costs"],
            "customer_service": ["support_tickets", "customer_feedback", "service_levels", "response_times", "satisfaction_scores"],
            "product": ["product_catalog", "product_categories", "product_reviews", "product_images", "product_specifications"],
            "marketing": ["campaigns", "promotions", "customer_segments", "email_campaigns", "ad_performance"],
            "hr": ["employees", "departments", "payroll", "attendance", "performance_reviews"],
            "manufacturing": ["production_orders", "work_centers", "bill_of_materials", "quality_checks", "machine_maintenance"],
            "analytics": ["daily_summaries", "monthly_aggregates", "kpi_metrics", "forecast_data", "trend_analysis"]
        }
    
    def connect(self):
        """Establish database connection."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                autocommit=False
            )
            print(f"✓ Connected to Aurora MySQL at {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"✗ Failed to connect: {e}")
            return False
    
    def create_database(self):
        """Create TPC-H database if not exists."""
        cursor = self.connection.cursor()
        try:
            cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
            cursor.execute(f"USE {self.database}")
            self.connection.commit()
            print(f"✓ Database '{self.database}' ready")
        except Exception as e:
            print(f"✗ Error creating database: {e}")
            raise
        finally:
            cursor.close()
    
    def create_tpch_tables(self):
        """Create TPC-H base tables with CDC-friendly schema."""
        cursor = self.connection.cursor()
        
        # Region table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS region (
                r_regionkey INT PRIMARY KEY,
                r_name CHAR(25) NOT NULL,
                r_comment VARCHAR(152),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_updated (updated_at)
            ) ENGINE=InnoDB
        """)
        
        # Nation table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS nation (
                n_nationkey INT PRIMARY KEY,
                n_name CHAR(25) NOT NULL,
                n_regionkey INT NOT NULL,
                n_comment VARCHAR(152),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_updated (updated_at),
                FOREIGN KEY (n_regionkey) REFERENCES region(r_regionkey)
            ) ENGINE=InnoDB
        """)
        
        # Supplier table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS supplier (
                s_suppkey INT PRIMARY KEY,
                s_name CHAR(25) NOT NULL,
                s_address VARCHAR(40),
                s_nationkey INT NOT NULL,
                s_phone CHAR(15),
                s_acctbal DECIMAL(15,2),
                s_comment VARCHAR(101),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_updated (updated_at),
                INDEX idx_nation (s_nationkey),
                FOREIGN KEY (s_nationkey) REFERENCES nation(n_nationkey)
            ) ENGINE=InnoDB
        """)
        
        # Part table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS part (
                p_partkey INT PRIMARY KEY,
                p_name VARCHAR(55) NOT NULL,
                p_mfgr CHAR(25),
                p_brand CHAR(10),
                p_type VARCHAR(25),
                p_size INT,
                p_container CHAR(10),
                p_retailprice DECIMAL(15,2),
                p_comment VARCHAR(23),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_updated (updated_at),
                INDEX idx_brand (p_brand),
                INDEX idx_type (p_type)
            ) ENGINE=InnoDB
        """)
        
        # PartSupp table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS partsupp (
                ps_partkey INT NOT NULL,
                ps_suppkey INT NOT NULL,
                ps_availqty INT,
                ps_supplycost DECIMAL(15,2),
                ps_comment VARCHAR(199),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (ps_partkey, ps_suppkey),
                INDEX idx_updated (updated_at),
                FOREIGN KEY (ps_partkey) REFERENCES part(p_partkey),
                FOREIGN KEY (ps_suppkey) REFERENCES supplier(s_suppkey)
            ) ENGINE=InnoDB
        """)
        
        # Customer table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS customer (
                c_custkey INT PRIMARY KEY,
                c_name VARCHAR(25) NOT NULL,
                c_address VARCHAR(40),
                c_nationkey INT NOT NULL,
                c_phone CHAR(15),
                c_acctbal DECIMAL(15,2),
                c_mktsegment CHAR(10),
                c_comment VARCHAR(117),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_updated (updated_at),
                INDEX idx_nation (c_nationkey),
                INDEX idx_mktsegment (c_mktsegment),
                FOREIGN KEY (c_nationkey) REFERENCES nation(n_nationkey)
            ) ENGINE=InnoDB
        """)
        
        # Orders table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS orders (
                o_orderkey INT PRIMARY KEY,
                o_custkey INT NOT NULL,
                o_orderstatus CHAR(1),
                o_totalprice DECIMAL(15,2),
                o_orderdate DATE,
                o_orderpriority CHAR(15),
                o_clerk CHAR(15),
                o_shippriority INT,
                o_comment VARCHAR(79),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                INDEX idx_updated (updated_at),
                INDEX idx_customer (o_custkey),
                INDEX idx_orderdate (o_orderdate),
                INDEX idx_status (o_orderstatus),
                FOREIGN KEY (o_custkey) REFERENCES customer(c_custkey)
            ) ENGINE=InnoDB
        """)
        
        # LineItem table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS lineitem (
                l_orderkey INT NOT NULL,
                l_partkey INT NOT NULL,
                l_suppkey INT NOT NULL,
                l_linenumber INT NOT NULL,
                l_quantity DECIMAL(15,2),
                l_extendedprice DECIMAL(15,2),
                l_discount DECIMAL(15,2),
                l_tax DECIMAL(15,2),
                l_returnflag CHAR(1),
                l_linestatus CHAR(1),
                l_shipdate DATE,
                l_commitdate DATE,
                l_receiptdate DATE,
                l_shipinstruct CHAR(25),
                l_shipmode CHAR(10),
                l_comment VARCHAR(44),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                PRIMARY KEY (l_orderkey, l_linenumber),
                INDEX idx_updated (updated_at),
                INDEX idx_shipdate (l_shipdate),
                INDEX idx_partkey (l_partkey),
                INDEX idx_suppkey (l_suppkey),
                FOREIGN KEY (l_orderkey) REFERENCES orders(o_orderkey),
                FOREIGN KEY (l_partkey) REFERENCES part(p_partkey),
                FOREIGN KEY (l_suppkey) REFERENCES supplier(s_suppkey)
            ) ENGINE=InnoDB
        """)
        
        self.connection.commit()
        print("✓ Created TPC-H base tables")
    
    def create_domain_tables(self):
        """Create additional domain-specific tables to reach 500+ tables."""
        cursor = self.connection.cursor()
        
        for domain, tables in self.business_domains.items():
            for base_table in tables:
                # Create multiple variants of each table (current, history, staging, etc.)
                variants = ["", "_history", "_staging", "_temp", "_archive", "_audit"]
                
                for variant in variants:
                    table_name = f"{domain}_{base_table}{variant}"
                    
                    # Create table with generic schema
                    cursor.execute(f"""
                        CREATE TABLE IF NOT EXISTS {table_name} (
                            id INT AUTO_INCREMENT PRIMARY KEY,
                            entity_id INT NOT NULL,
                            entity_type VARCHAR(50),
                            status VARCHAR(20) DEFAULT 'active',
                            data JSON,
                            metric_value DECIMAL(15,2),
                            reference_date DATE,
                            created_by VARCHAR(50),
                            updated_by VARCHAR(50),
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
                            version INT DEFAULT 1,
                            is_deleted BOOLEAN DEFAULT FALSE,
                            INDEX idx_updated (updated_at),
                            INDEX idx_entity (entity_id, entity_type),
                            INDEX idx_status (status),
                            INDEX idx_date (reference_date)
                        ) ENGINE=InnoDB
                    """)
                    
                    with self.lock:
                        self.tables_created += 1
                    
                    if self.tables_created % 50 == 0:
                        print(f"  Created {self.tables_created} tables...")
        
        self.connection.commit()
        print(f"✓ Created {self.tables_created} domain tables")
    
    def load_tpch_data(self):
        """Load sample TPC-H data based on scale factor."""
        cursor = self.connection.cursor()
        
        # Load regions
        regions = [
            (0, 'AFRICA', 'Special regions in Africa'),
            (1, 'AMERICA', 'Americas region'),
            (2, 'ASIA', 'Asian markets'),
            (3, 'EUROPE', 'European Union'),
            (4, 'MIDDLE EAST', 'Middle Eastern countries')
        ]
        
        for r in regions:
            cursor.execute("""
                INSERT INTO region (r_regionkey, r_name, r_comment) 
                VALUES (%s, %s, %s)
                ON DUPLICATE KEY UPDATE r_name=VALUES(r_name)
            """, r)
        
        # Load nations (sample)
        nations = [
            (0, 'ALGERIA', 0), (1, 'ARGENTINA', 1), (2, 'BRAZIL', 1),
            (3, 'CANADA', 1), (4, 'EGYPT', 4), (5, 'ETHIOPIA', 0),
            (6, 'FRANCE', 3), (7, 'GERMANY', 3), (8, 'INDIA', 2),
            (9, 'INDONESIA', 2), (10, 'IRAN', 4), (11, 'IRAQ', 4),
            (12, 'JAPAN', 2), (13, 'JORDAN', 4), (14, 'KENYA', 0),
            (15, 'MOROCCO', 0), (16, 'MOZAMBIQUE', 0), (17, 'PERU', 1),
            (18, 'CHINA', 2), (19, 'ROMANIA', 3), (20, 'SAUDI ARABIA', 4),
            (21, 'VIETNAM', 2), (22, 'RUSSIA', 3), (23, 'UNITED KINGDOM', 3),
            (24, 'UNITED STATES', 1)
        ]
        
        for n in nations:
            cursor.execute("""
                INSERT INTO nation (n_nationkey, n_name, n_regionkey, n_comment) 
                VALUES (%s, %s, %s, 'Nation in region')
                ON DUPLICATE KEY UPDATE n_name=VALUES(n_name)
            """, n)
        
        self.connection.commit()
        
        # Generate scaled data
        print(f"Loading data with scale factor {self.scale}...")
        
        # Calculated record counts based on scale
        num_customers = 150000 * self.scale
        num_orders = 1500000 * self.scale
        num_suppliers = 10000 * self.scale
        num_parts = 200000 * self.scale
        
        # Use parallel loading for better performance
        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            futures = []
            
            # Load customers
            futures.append(executor.submit(self._load_customers, num_customers))
            
            # Load suppliers
            futures.append(executor.submit(self._load_suppliers, num_suppliers))
            
            # Load parts
            futures.append(executor.submit(self._load_parts, num_parts))
            
            # Wait for basic data to load
            for future in futures:
                future.result()
            
            # Load orders (depends on customers)
            self._load_orders(num_orders)
        
        print("✓ Loaded TPC-H sample data")
    
    def _load_customers(self, count: int):
        """Load customer data."""
        conn = mysql.connector.connect(
            host=self.host, port=self.port,
            user=self.username, password=self.password,
            database=self.database
        )
        cursor = conn.cursor()
        
        batch_size = 1000
        segments = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD']
        
        for i in range(0, count, batch_size):
            batch = []
            for j in range(min(batch_size, count - i)):
                cust_id = i + j + 1
                batch.append((
                    cust_id,
                    f'Customer_{cust_id}',
                    f'{random.randint(1, 999)} Main St',
                    random.randint(0, 24),
                    f'{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}',
                    round(random.uniform(-999, 9999), 2),
                    random.choice(segments),
                    'Standard customer account'
                ))
            
            cursor.executemany("""
                INSERT IGNORE INTO customer 
                (c_custkey, c_name, c_address, c_nationkey, c_phone, c_acctbal, c_mktsegment, c_comment)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            
            if (i + batch_size) % 10000 == 0:
                conn.commit()
                print(f"    Loaded {i + batch_size} customers...")
        
        conn.commit()
        cursor.close()
        conn.close()
    
    def _load_suppliers(self, count: int):
        """Load supplier data."""
        conn = mysql.connector.connect(
            host=self.host, port=self.port,
            user=self.username, password=self.password,
            database=self.database
        )
        cursor = conn.cursor()
        
        batch_size = 1000
        
        for i in range(0, count, batch_size):
            batch = []
            for j in range(min(batch_size, count - i)):
                supp_id = i + j + 1
                batch.append((
                    supp_id,
                    f'Supplier_{supp_id}',
                    f'{random.randint(1, 999)} Industrial Blvd',
                    random.randint(0, 24),
                    f'{random.randint(100, 999)}-{random.randint(100, 999)}-{random.randint(1000, 9999)}',
                    round(random.uniform(0, 10000), 2),
                    'Reliable supplier'
                ))
            
            cursor.executemany("""
                INSERT IGNORE INTO supplier 
                (s_suppkey, s_name, s_address, s_nationkey, s_phone, s_acctbal, s_comment)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, batch)
            
            if (i + batch_size) % 10000 == 0:
                conn.commit()
                print(f"    Loaded {i + batch_size} suppliers...")
        
        conn.commit()
        cursor.close()
        conn.close()
    
    def _load_parts(self, count: int):
        """Load part data."""
        conn = mysql.connector.connect(
            host=self.host, port=self.port,
            user=self.username, password=self.password,
            database=self.database
        )
        cursor = conn.cursor()
        
        batch_size = 1000
        types = ['STANDARD', 'SMALL', 'MEDIUM', 'LARGE', 'ECONOMY', 'PROMO']
        brands = ['Brand#1', 'Brand#2', 'Brand#3', 'Brand#4', 'Brand#5']
        containers = ['SM PKG', 'LG PKG', 'MED BOX', 'LG BOX', 'SM CASE', 'LG CASE']
        
        for i in range(0, count, batch_size):
            batch = []
            for j in range(min(batch_size, count - i)):
                part_id = i + j + 1
                batch.append((
                    part_id,
                    f'Part_{part_id}',
                    f'Manufacturer_{random.randint(1, 5)}',
                    random.choice(brands),
                    random.choice(types),
                    random.randint(1, 50),
                    random.choice(containers),
                    round(random.uniform(10, 1000), 2),
                    'Standard part'
                ))
            
            cursor.executemany("""
                INSERT IGNORE INTO part 
                (p_partkey, p_name, p_mfgr, p_brand, p_type, p_size, p_container, p_retailprice, p_comment)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            
            if (i + batch_size) % 10000 == 0:
                conn.commit()
                print(f"    Loaded {i + batch_size} parts...")
        
        conn.commit()
        cursor.close()
        conn.close()
    
    def _load_orders(self, count: int):
        """Load order data."""
        cursor = self.connection.cursor()
        
        batch_size = 1000
        statuses = ['F', 'O', 'P']  # Finished, Open, Pending
        priorities = ['1-URGENT', '2-HIGH', '3-MEDIUM', '4-LOW', '5-NONE']
        
        # Get customer count
        cursor.execute("SELECT MAX(c_custkey) FROM customer")
        max_customer = cursor.fetchone()[0] or 1
        
        for i in range(0, count, batch_size):
            batch = []
            for j in range(min(batch_size, count - i)):
                order_id = i + j + 1
                order_date = datetime.now() - timedelta(days=random.randint(0, 365))
                batch.append((
                    order_id,
                    random.randint(1, max_customer),
                    random.choice(statuses),
                    round(random.uniform(100, 100000), 2),
                    order_date.date(),
                    random.choice(priorities),
                    f'Clerk_{random.randint(1, 100)}',
                    random.randint(0, 7),
                    'Standard order'
                ))
            
            cursor.executemany("""
                INSERT IGNORE INTO orders 
                (o_orderkey, o_custkey, o_orderstatus, o_totalprice, o_orderdate, 
                 o_orderpriority, o_clerk, o_shippriority, o_comment)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, batch)
            
            if (i + batch_size) % 10000 == 0:
                self.connection.commit()
                print(f"    Loaded {i + batch_size} orders...")
        
        self.connection.commit()
    
    def setup_cdc_permissions(self):
        """Setup CDC user and permissions."""
        cursor = self.connection.cursor()
        
        try:
            # Create CDC user
            cursor.execute("""
                CREATE USER IF NOT EXISTS 'cdc_user'@'%' 
                IDENTIFIED BY 'CDC_Demo_Password_2024!'
            """)
            
            # Grant necessary permissions for CDC
            cursor.execute("""
                GRANT SELECT, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'cdc_user'@'%'
            """)
            
            cursor.execute("""
                GRANT ALL PRIVILEGES ON tpch.* TO 'cdc_user'@'%'
            """)
            
            cursor.execute("FLUSH PRIVILEGES")
            
            self.connection.commit()
            print("✓ CDC user and permissions configured")
            
        except Exception as e:
            print(f"Warning: Could not create CDC user: {e}")
        finally:
            cursor.close()
    
    def verify_setup(self):
        """Verify the database setup."""
        cursor = self.connection.cursor()
        
        # Check table counts
        cursor.execute("""
            SELECT COUNT(*) FROM information_schema.tables 
            WHERE table_schema = %s
        """, (self.database,))
        table_count = cursor.fetchone()[0]
        
        print(f"\n=== Setup Verification ===")
        print(f"Total tables created: {table_count}")
        
        # Check record counts for main tables
        for table in self.base_tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            count = cursor.fetchone()[0]
            print(f"  {table}: {count:,} records")
        
        # Check binary logging
        cursor.execute("SHOW VARIABLES LIKE 'log_bin'")
        log_bin = cursor.fetchone()
        print(f"\nBinary logging: {log_bin[1] if log_bin else 'Not set'}")
        
        cursor.close()
        
        return table_count >= 500
    
    def run(self):
        """Run the complete setup process."""
        if not self.connect():
            return False
        
        try:
            print("\n=== Starting TPC-H Database Setup ===")
            
            # Create database
            self.create_database()
            
            # Create tables
            print("\nCreating tables...")
            self.create_tpch_tables()
            self.create_domain_tables()
            
            # Load data
            print(f"\nLoading data (scale factor: {self.scale})...")
            self.load_tpch_data()
            
            # Setup CDC
            print("\nConfiguring CDC...")
            self.setup_cdc_permissions()
            
            # Verify
            success = self.verify_setup()
            
            if success:
                print("\n✓ Setup completed successfully!")
            else:
                print("\n⚠ Setup completed with warnings")
            
            return success
            
        except Exception as e:
            print(f"\n✗ Setup failed: {e}")
            return False
        finally:
            if self.connection:
                self.connection.close()


def main():
    parser = argparse.ArgumentParser(description='Setup TPC-H database for CDC demo')
    parser.add_argument('--host', required=True, help='Aurora endpoint')
    parser.add_argument('--port', type=int, default=3306, help='Aurora port')
    parser.add_argument('--username', required=True, help='Database username')
    parser.add_argument('--password', required=True, help='Database password')
    parser.add_argument('--scale', type=int, default=1, help='TPC-H scale factor (1=1GB)')
    
    args = parser.parse_args()
    
    setup = TPCHDatabaseSetup(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        scale=args.scale
    )
    
    success = setup.run()
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()