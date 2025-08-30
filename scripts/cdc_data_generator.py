#!/usr/bin/env python3
"""
CDC Data Generator for Aurora MySQL
Continuously generates realistic data changes across 500+ tables
"""

import argparse
import mysql.connector
import random
import time
import json
import threading
import signal
import sys
from datetime import datetime, timedelta
from decimal import Decimal
from typing import List, Dict, Any, Optional
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CDCDataGenerator:
    """Generate continuous data changes for CDC testing."""
    
    def __init__(self, host: str, port: int, username: str, password: str,
                 interval: int = 5, batch_size: int = 100):
        """Initialize the data generator."""
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = "tpch"
        self.interval = interval  # Seconds between batches
        self.batch_size = batch_size  # Changes per batch
        self.running = False
        self.connection = None
        self.threads = []
        
        # Statistics
        self.stats = {
            "inserts": 0,
            "updates": 0,
            "deletes": 0,
            "errors": 0,
            "start_time": datetime.now()
        }
        
        # Operation weights (adjust for different scenarios)
        self.operation_weights = {
            "insert": 40,
            "update": 50,
            "delete": 10
        }
        
        # Business scenarios
        self.scenarios = [
            self.new_customer_order,
            self.update_order_status,
            self.adjust_inventory,
            self.process_payment,
            self.ship_order,
            self.cancel_order,
            self.update_customer_info,
            self.add_product_review,
            self.process_return,
            self.update_prices
        ]
    
    def connect(self):
        """Establish database connection."""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                port=self.port,
                user=self.username,
                password=self.password,
                database=self.database,
                autocommit=False
            )
            logger.info(f"Connected to Aurora MySQL at {self.host}:{self.port}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            return False
    
    def new_customer_order(self, cursor):
        """Simulate new customer order scenario."""
        try:
            # Get random customer
            cursor.execute("""
                SELECT c_custkey, c_name FROM customer 
                ORDER BY RAND() LIMIT 1
            """)
            customer = cursor.fetchone()
            
            if not customer:
                return
            
            # Create new order
            order_id = int(time.time() * 1000) % 2147483647
            order_date = datetime.now()
            total_price = round(random.uniform(100, 10000), 2)
            
            cursor.execute("""
                INSERT INTO orders (o_orderkey, o_custkey, o_orderstatus, 
                    o_totalprice, o_orderdate, o_orderpriority, o_clerk, 
                    o_shippriority, o_comment)
                VALUES (%s, %s, 'O', %s, %s, '3-MEDIUM', %s, 0, 'New order')
                ON DUPLICATE KEY UPDATE 
                    o_totalprice = VALUES(o_totalprice),
                    updated_at = CURRENT_TIMESTAMP
            """, (order_id, customer[0], total_price, order_date.date(), 
                  f'Clerk_{random.randint(1, 100)}'))
            
            # Add line items
            num_items = random.randint(1, 5)
            for line_num in range(1, num_items + 1):
                cursor.execute("""
                    SELECT p_partkey, p_retailprice FROM part 
                    ORDER BY RAND() LIMIT 1
                """)
                part = cursor.fetchone()
                
                if part:
                    quantity = random.randint(1, 10)
                    price = float(part[1]) * quantity
                    
                    cursor.execute("""
                        INSERT INTO lineitem (l_orderkey, l_partkey, l_suppkey,
                            l_linenumber, l_quantity, l_extendedprice, l_discount,
                            l_tax, l_returnflag, l_linestatus, l_shipdate,
                            l_commitdate, l_receiptdate, l_shipinstruct, 
                            l_shipmode, l_comment)
                        VALUES (%s, %s, 1, %s, %s, %s, 0.05, 0.08, 'N', 'O',
                            %s, %s, %s, 'NONE', 'SHIP', 'Standard delivery')
                        ON DUPLICATE KEY UPDATE 
                            l_quantity = VALUES(l_quantity),
                            updated_at = CURRENT_TIMESTAMP
                    """, (order_id, part[0], line_num, quantity, price,
                          order_date.date(), 
                          (order_date + timedelta(days=3)).date(),
                          (order_date + timedelta(days=7)).date()))
            
            self.stats["inserts"] += 1 + num_items
            logger.debug(f"Created order {order_id} with {num_items} items")
            
        except Exception as e:
            logger.error(f"Error in new_customer_order: {e}")
            self.stats["errors"] += 1
    
    def update_order_status(self, cursor):
        """Update order status (shipping, delivery, etc.)."""
        try:
            # Get random open order
            cursor.execute("""
                SELECT o_orderkey FROM orders 
                WHERE o_orderstatus = 'O' 
                ORDER BY RAND() LIMIT 1
            """)
            order = cursor.fetchone()
            
            if order:
                new_status = random.choice(['P', 'F'])  # Processing or Finished
                cursor.execute("""
                    UPDATE orders 
                    SET o_orderstatus = %s,
                        o_comment = CONCAT('Status updated: ', %s),
                        updated_at = CURRENT_TIMESTAMP
                    WHERE o_orderkey = %s
                """, (new_status, datetime.now().isoformat(), order[0]))
                
                self.stats["updates"] += 1
                logger.debug(f"Updated order {order[0]} status to {new_status}")
                
        except Exception as e:
            logger.error(f"Error in update_order_status: {e}")
            self.stats["errors"] += 1
    
    def adjust_inventory(self, cursor):
        """Adjust inventory levels for parts."""
        try:
            # Update multiple parts inventory
            num_parts = random.randint(5, 20)
            
            cursor.execute("""
                SELECT ps_partkey, ps_suppkey, ps_availqty 
                FROM partsupp 
                ORDER BY RAND() LIMIT %s
            """, (num_parts,))
            
            parts = cursor.fetchall()
            
            for part in parts:
                adjustment = random.randint(-10, 50)
                new_qty = max(0, part[2] + adjustment)
                
                cursor.execute("""
                    UPDATE partsupp 
                    SET ps_availqty = %s,
                        ps_comment = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE ps_partkey = %s AND ps_suppkey = %s
                """, (new_qty, f'Inventory adjusted by {adjustment}', 
                      part[0], part[1]))
            
            self.stats["updates"] += len(parts)
            logger.debug(f"Adjusted inventory for {len(parts)} parts")
            
        except Exception as e:
            logger.error(f"Error in adjust_inventory: {e}")
            self.stats["errors"] += 1
    
    def process_payment(self, cursor):
        """Process customer payment."""
        try:
            # Update customer account balance
            cursor.execute("""
                SELECT c_custkey, c_acctbal FROM customer 
                ORDER BY RAND() LIMIT 1
            """)
            customer = cursor.fetchone()
            
            if customer:
                payment = round(random.uniform(100, 5000), 2)
                new_balance = float(customer[1]) - payment
                
                cursor.execute("""
                    UPDATE customer 
                    SET c_acctbal = %s,
                        c_comment = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE c_custkey = %s
                """, (new_balance, f'Payment processed: ${payment}', customer[0]))
                
                # Update related tables
                cursor.execute("""
                    INSERT INTO finance_payments 
                    (entity_id, entity_type, metric_value, data, reference_date)
                    VALUES (%s, 'customer', %s, %s, %s)
                """, (customer[0], payment, 
                      json.dumps({"type": "payment", "method": "credit"}),
                      datetime.now().date()))
                
                self.stats["updates"] += 1
                self.stats["inserts"] += 1
                logger.debug(f"Processed payment of ${payment} for customer {customer[0]}")
                
        except Exception as e:
            logger.error(f"Error in process_payment: {e}")
            self.stats["errors"] += 1
    
    def ship_order(self, cursor):
        """Update shipping information."""
        try:
            # Get random pending order
            cursor.execute("""
                SELECT o_orderkey FROM orders 
                WHERE o_orderstatus IN ('O', 'P')
                ORDER BY RAND() LIMIT 1
            """)
            order = cursor.fetchone()
            
            if order:
                ship_date = datetime.now()
                
                # Update line items
                cursor.execute("""
                    UPDATE lineitem 
                    SET l_shipdate = %s,
                        l_linestatus = 'S',
                        l_comment = 'Shipped',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE l_orderkey = %s AND l_linestatus = 'O'
                """, (ship_date.date(), order[0]))
                
                # Add shipping record
                cursor.execute("""
                    INSERT INTO logistics_shipments 
                    (entity_id, entity_type, status, data, reference_date)
                    VALUES (%s, 'order', 'shipped', %s, %s)
                """, (order[0], 
                      json.dumps({"carrier": "FedEx", "tracking": f"TRK{order[0]}"}),
                      ship_date.date()))
                
                self.stats["updates"] += 1
                self.stats["inserts"] += 1
                logger.debug(f"Shipped order {order[0]}")
                
        except Exception as e:
            logger.error(f"Error in ship_order: {e}")
            self.stats["errors"] += 1
    
    def cancel_order(self, cursor):
        """Cancel an order."""
        try:
            # Get random open order
            cursor.execute("""
                SELECT o_orderkey FROM orders 
                WHERE o_orderstatus = 'O' 
                ORDER BY RAND() LIMIT 1
            """)
            order = cursor.fetchone()
            
            if order:
                # Soft delete - mark as cancelled
                cursor.execute("""
                    UPDATE orders 
                    SET o_orderstatus = 'C',
                        o_comment = 'Order cancelled',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE o_orderkey = %s
                """, (order[0],))
                
                # Update line items
                cursor.execute("""
                    UPDATE lineitem 
                    SET l_returnflag = 'R',
                        l_linestatus = 'C',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE l_orderkey = %s
                """, (order[0],))
                
                self.stats["updates"] += 2
                logger.debug(f"Cancelled order {order[0]}")
                
        except Exception as e:
            logger.error(f"Error in cancel_order: {e}")
            self.stats["errors"] += 1
    
    def update_customer_info(self, cursor):
        """Update customer information."""
        try:
            cursor.execute("""
                SELECT c_custkey FROM customer 
                ORDER BY RAND() LIMIT 1
            """)
            customer = cursor.fetchone()
            
            if customer:
                updates = []
                values = []
                
                # Random updates
                if random.random() > 0.5:
                    updates.append("c_phone = %s")
                    values.append(f'{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}')
                
                if random.random() > 0.5:
                    updates.append("c_address = %s")
                    values.append(f'{random.randint(1,999)} Updated St')
                
                if random.random() > 0.5:
                    segments = ['AUTOMOBILE', 'BUILDING', 'FURNITURE', 'MACHINERY', 'HOUSEHOLD']
                    updates.append("c_mktsegment = %s")
                    values.append(random.choice(segments))
                
                if updates:
                    updates.append("updated_at = CURRENT_TIMESTAMP")
                    values.append(customer[0])
                    
                    query = f"UPDATE customer SET {', '.join(updates)} WHERE c_custkey = %s"
                    cursor.execute(query, values)
                    
                    self.stats["updates"] += 1
                    logger.debug(f"Updated customer {customer[0]} info")
                    
        except Exception as e:
            logger.error(f"Error in update_customer_info: {e}")
            self.stats["errors"] += 1
    
    def add_product_review(self, cursor):
        """Add product reviews."""
        try:
            # Get random part
            cursor.execute("""
                SELECT p_partkey, p_name FROM part 
                ORDER BY RAND() LIMIT 1
            """)
            part = cursor.fetchone()
            
            if part:
                rating = random.randint(1, 5)
                
                cursor.execute("""
                    INSERT INTO product_product_reviews 
                    (entity_id, entity_type, metric_value, data, reference_date)
                    VALUES (%s, 'part', %s, %s, %s)
                """, (part[0], rating,
                      json.dumps({
                          "rating": rating,
                          "comment": f"Review for {part[1]}",
                          "verified": True
                      }),
                      datetime.now().date()))
                
                self.stats["inserts"] += 1
                logger.debug(f"Added review for part {part[0]}")
                
        except Exception as e:
            logger.error(f"Error in add_product_review: {e}")
            self.stats["errors"] += 1
    
    def process_return(self, cursor):
        """Process order returns."""
        try:
            # Get random finished order
            cursor.execute("""
                SELECT o_orderkey FROM orders 
                WHERE o_orderstatus = 'F' 
                AND o_orderdate > DATE_SUB(NOW(), INTERVAL 30 DAY)
                ORDER BY RAND() LIMIT 1
            """)
            order = cursor.fetchone()
            
            if order:
                # Update line items for return
                cursor.execute("""
                    UPDATE lineitem 
                    SET l_returnflag = 'R',
                        l_comment = 'Returned item',
                        updated_at = CURRENT_TIMESTAMP
                    WHERE l_orderkey = %s 
                    AND l_linenumber = 1
                """, (order[0],))
                
                # Add return record
                cursor.execute("""
                    INSERT INTO customer_service_support_tickets 
                    (entity_id, entity_type, status, data, reference_date)
                    VALUES (%s, 'order', 'return', %s, %s)
                """, (order[0],
                      json.dumps({"reason": "defective", "refund": True}),
                      datetime.now().date()))
                
                self.stats["updates"] += 1
                self.stats["inserts"] += 1
                logger.debug(f"Processed return for order {order[0]}")
                
        except Exception as e:
            logger.error(f"Error in process_return: {e}")
            self.stats["errors"] += 1
    
    def update_prices(self, cursor):
        """Update product prices."""
        try:
            # Update multiple part prices
            num_parts = random.randint(10, 50)
            
            cursor.execute("""
                SELECT p_partkey, p_retailprice FROM part 
                ORDER BY RAND() LIMIT %s
            """, (num_parts,))
            
            parts = cursor.fetchall()
            
            for part in parts:
                # Price change between -10% to +10%
                change = random.uniform(0.9, 1.1)
                new_price = round(float(part[1]) * change, 2)
                
                cursor.execute("""
                    UPDATE part 
                    SET p_retailprice = %s,
                        p_comment = %s,
                        updated_at = CURRENT_TIMESTAMP
                    WHERE p_partkey = %s
                """, (new_price, f'Price updated from ${part[1]}', part[0]))
            
            self.stats["updates"] += len(parts)
            logger.debug(f"Updated prices for {len(parts)} parts")
            
        except Exception as e:
            logger.error(f"Error in update_prices: {e}")
            self.stats["errors"] += 1
    
    def generate_batch(self):
        """Generate a batch of changes."""
        if not self.connection or not self.connection.is_connected():
            self.connect()
        
        cursor = self.connection.cursor()
        
        try:
            # Execute random scenarios
            for _ in range(self.batch_size):
                scenario = random.choice(self.scenarios)
                scenario(cursor)
            
            # Commit batch
            self.connection.commit()
            
            # Log progress
            total_changes = self.stats["inserts"] + self.stats["updates"] + self.stats["deletes"]
            runtime = (datetime.now() - self.stats["start_time"]).total_seconds()
            rate = total_changes / runtime if runtime > 0 else 0
            
            logger.info(f"Batch complete - Total changes: {total_changes:,} "
                       f"(Rate: {rate:.1f}/sec, Errors: {self.stats['errors']})")
            
        except Exception as e:
            logger.error(f"Error in batch generation: {e}")
            self.connection.rollback()
        finally:
            cursor.close()
    
    def run_continuous(self):
        """Run continuous data generation."""
        logger.info(f"Starting continuous data generation (interval: {self.interval}s, batch: {self.batch_size})")
        self.running = True
        
        while self.running:
            try:
                self.generate_batch()
                time.sleep(self.interval)
            except KeyboardInterrupt:
                break
            except Exception as e:
                logger.error(f"Error in continuous generation: {e}")
                time.sleep(self.interval)
    
    def stop(self):
        """Stop data generation."""
        logger.info("Stopping data generation...")
        self.running = False
        
        # Print final statistics
        runtime = (datetime.now() - self.stats["start_time"]).total_seconds()
        total_changes = self.stats["inserts"] + self.stats["updates"] + self.stats["deletes"]
        
        print("\n=== Final Statistics ===")
        print(f"Runtime: {runtime:.1f} seconds")
        print(f"Total changes: {total_changes:,}")
        print(f"  Inserts: {self.stats['inserts']:,}")
        print(f"  Updates: {self.stats['updates']:,}")
        print(f"  Deletes: {self.stats['deletes']:,}")
        print(f"Errors: {self.stats['errors']:,}")
        print(f"Average rate: {total_changes/runtime:.1f} changes/sec")
        
        if self.connection:
            self.connection.close()


def signal_handler(signum, frame):
    """Handle shutdown signals."""
    global generator
    if generator:
        generator.stop()
    sys.exit(0)


def main():
    global generator
    
    parser = argparse.ArgumentParser(description='CDC Data Generator for Aurora MySQL')
    parser.add_argument('--host', required=True, help='Aurora endpoint')
    parser.add_argument('--port', type=int, default=3306, help='Aurora port')
    parser.add_argument('--username', required=True, help='Database username')
    parser.add_argument('--password', required=True, help='Database password')
    parser.add_argument('--interval', type=int, default=5, 
                       help='Seconds between batches (default: 5)')
    parser.add_argument('--batch-size', type=int, default=100,
                       help='Changes per batch (default: 100)')
    parser.add_argument('--duration', type=int, default=0,
                       help='Run duration in seconds (0=continuous)')
    
    args = parser.parse_args()
    
    # Setup signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Create and run generator
    generator = CDCDataGenerator(
        host=args.host,
        port=args.port,
        username=args.username,
        password=args.password,
        interval=args.interval,
        batch_size=args.batch_size
    )
    
    if not generator.connect():
        sys.exit(1)
    
    if args.duration > 0:
        # Run for specified duration
        def timeout_handler():
            time.sleep(args.duration)
            generator.stop()
        
        timeout_thread = threading.Thread(target=timeout_handler)
        timeout_thread.start()
    
    # Run generator
    generator.run_continuous()


if __name__ == "__main__":
    main()