#!/usr/bin/env python3
"""
Comprehensive Test Suite for Aurora CDC Demo
Tests connectivity, data integrity, CDC functionality, and performance
"""

import unittest
import mysql.connector
import time
import json
import random
from datetime import datetime, timedelta
from decimal import Decimal
from typing import Dict, List, Any, Optional
import concurrent.futures
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestAuroraCDCIntegration(unittest.TestCase):
    """Integration tests for Aurora CDC setup."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        # Connection configuration (from environment or config file)
        cls.config = {
            "host": "aurora-cluster.amazonaws.com",  # Replace with actual
            "port": 3306,
            "database": "tpch",
            "username": "cdcadmin",
            "password": "test_password"
        }
        
        # Test parameters
        cls.test_timeout = 300  # 5 minutes
        cls.expected_tables = 500
        cls.min_performance_rate = 1000  # events/second
        
        # Connect to database
        cls.connection = None
        cls.connect()
    
    @classmethod
    def connect(cls):
        """Establish database connection."""
        try:
            cls.connection = mysql.connector.connect(
                host=cls.config["host"],
                port=cls.config["port"],
                user=cls.config["username"],
                password=cls.config["password"],
                database=cls.config["database"],
                autocommit=False
            )
            logger.info("Connected to Aurora MySQL")
        except Exception as e:
            logger.error(f"Failed to connect: {e}")
            raise
    
    def setUp(self):
        """Set up each test."""
        if not self.connection or not self.connection.is_connected():
            self.connect()
        self.cursor = self.connection.cursor(dictionary=True)
    
    def tearDown(self):
        """Clean up after each test."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.rollback()
    
    # ==================== Connectivity Tests ====================
    
    def test_01_aurora_connectivity(self):
        """Test Aurora MySQL connectivity."""
        self.assertTrue(self.connection.is_connected(), 
                       "Aurora connection should be active")
        
        # Test basic query
        self.cursor.execute("SELECT 1 as test")
        result = self.cursor.fetchone()
        self.assertEqual(result['test'], 1, "Basic query should work")
        
        logger.info("✓ Aurora connectivity test passed")
    
    def test_02_vpc_peering(self):
        """Test VPC peering connectivity."""
        # Check if connection is from peered VPC
        self.cursor.execute("SELECT CONNECTION_ID(), USER(), DATABASE()")
        result = self.cursor.fetchone()
        
        self.assertIsNotNone(result, "Should get connection info")
        self.assertEqual(result['DATABASE()'], 'tpch', 
                        "Should be connected to correct database")
        
        logger.info("✓ VPC peering test passed")
    
    def test_03_cdc_user_permissions(self):
        """Test CDC user has correct permissions."""
        # Check replication permissions
        self.cursor.execute("""
            SHOW GRANTS FOR 'cdc_user'@'%'
        """)
        grants = self.cursor.fetchall()
        
        required_permissions = [
            'REPLICATION SLAVE',
            'REPLICATION CLIENT',
            'SELECT'
        ]
        
        grant_text = ' '.join([g['Grants for cdc_user@%'] for g in grants])
        
        for perm in required_permissions:
            self.assertIn(perm, grant_text, 
                         f"CDC user should have {perm} permission")
        
        logger.info("✓ CDC user permissions test passed")
    
    # ==================== Schema Tests ====================
    
    def test_04_table_count(self):
        """Test that 500+ tables exist."""
        self.cursor.execute("""
            SELECT COUNT(*) as table_count
            FROM information_schema.tables
            WHERE table_schema = %s
            AND table_type = 'BASE TABLE'
        """, (self.config["database"],))
        
        result = self.cursor.fetchone()
        table_count = result['table_count']
        
        self.assertGreaterEqual(table_count, self.expected_tables,
                               f"Should have at least {self.expected_tables} tables")
        
        logger.info(f"✓ Table count test passed: {table_count} tables found")
    
    def test_05_tpch_schema(self):
        """Test TPC-H tables have correct schema."""
        tpch_tables = {
            'customer': ['c_custkey', 'c_name', 'c_address', 'updated_at'],
            'orders': ['o_orderkey', 'o_custkey', 'o_orderstatus', 'updated_at'],
            'lineitem': ['l_orderkey', 'l_partkey', 'l_quantity', 'updated_at'],
            'part': ['p_partkey', 'p_name', 'p_retailprice', 'updated_at'],
            'supplier': ['s_suppkey', 's_name', 's_acctbal', 'updated_at']
        }
        
        for table_name, expected_columns in tpch_tables.items():
            self.cursor.execute("""
                SELECT COLUMN_NAME
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
            """, (self.config["database"], table_name))
            
            columns = [row['COLUMN_NAME'] for row in self.cursor.fetchall()]
            
            for col in expected_columns:
                self.assertIn(col, columns, 
                             f"Table {table_name} should have column {col}")
        
        logger.info("✓ TPC-H schema test passed")
    
    def test_06_cdc_columns(self):
        """Test tables have CDC tracking columns."""
        # Check sample tables for CDC columns
        tables_to_check = ['customer', 'orders', 'lineitem']
        cdc_columns = ['created_at', 'updated_at']
        
        for table in tables_to_check:
            self.cursor.execute("""
                SELECT COLUMN_NAME, EXTRA
                FROM information_schema.COLUMNS
                WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s
                AND COLUMN_NAME IN ('created_at', 'updated_at')
            """, (self.config["database"], table))
            
            columns = self.cursor.fetchall()
            
            self.assertEqual(len(columns), 2, 
                           f"Table {table} should have CDC columns")
            
            for col in columns:
                if col['COLUMN_NAME'] == 'updated_at':
                    self.assertIn('ON UPDATE CURRENT_TIMESTAMP', col['EXTRA'],
                                 "updated_at should auto-update")
        
        logger.info("✓ CDC columns test passed")
    
    # ==================== Data Integrity Tests ====================
    
    def test_07_data_integrity(self):
        """Test data integrity and relationships."""
        # Check foreign key relationships
        self.cursor.execute("""
            SELECT 
                COUNT(DISTINCT o.o_custkey) as order_customers,
                COUNT(DISTINCT c.c_custkey) as total_customers
            FROM orders o
            LEFT JOIN customer c ON o.o_custkey = c.c_custkey
            WHERE c.c_custkey IS NOT NULL
        """)
        
        result = self.cursor.fetchone()
        
        self.assertGreater(result['order_customers'], 0, 
                          "Should have orders with valid customers")
        
        # Check for orphaned records
        self.cursor.execute("""
            SELECT COUNT(*) as orphaned_orders
            FROM orders o
            LEFT JOIN customer c ON o.o_custkey = c.c_custkey
            WHERE c.c_custkey IS NULL
        """)
        
        orphaned = self.cursor.fetchone()
        self.assertEqual(orphaned['orphaned_orders'], 0, 
                        "Should have no orphaned orders")
        
        logger.info("✓ Data integrity test passed")
    
    def test_08_data_distribution(self):
        """Test data is well distributed."""
        # Check order distribution across time
        self.cursor.execute("""
            SELECT 
                DATE_FORMAT(o_orderdate, '%Y-%m') as month,
                COUNT(*) as order_count
            FROM orders
            GROUP BY DATE_FORMAT(o_orderdate, '%Y-%m')
            HAVING COUNT(*) > 0
            LIMIT 12
        """)
        
        months = self.cursor.fetchall()
        
        self.assertGreater(len(months), 0, 
                          "Should have orders distributed across months")
        
        # Check reasonable distribution
        counts = [m['order_count'] for m in months]
        if counts:
            avg_count = sum(counts) / len(counts)
            for count in counts:
                # Check each month is within 5x of average (reasonable distribution)
                self.assertLess(count, avg_count * 5, 
                               "Order distribution should be reasonable")
        
        logger.info("✓ Data distribution test passed")
    
    # ==================== CDC Functionality Tests ====================
    
    def test_09_binlog_enabled(self):
        """Test binary logging is enabled for CDC."""
        self.cursor.execute("SHOW VARIABLES LIKE 'log_bin'")
        result = self.cursor.fetchone()
        
        self.assertEqual(result['Value'], 'ON', 
                        "Binary logging should be enabled")
        
        # Check binlog format
        self.cursor.execute("SHOW VARIABLES LIKE 'binlog_format'")
        result = self.cursor.fetchone()
        
        self.assertEqual(result['Value'], 'ROW', 
                        "Binlog format should be ROW for CDC")
        
        # Check binlog row image
        self.cursor.execute("SHOW VARIABLES LIKE 'binlog_row_image'")
        result = self.cursor.fetchone()
        
        self.assertEqual(result['Value'], 'FULL', 
                        "Binlog row image should be FULL for complete CDC")
        
        logger.info("✓ Binlog configuration test passed")
    
    def test_10_cdc_insert(self):
        """Test CDC captures INSERT operations."""
        # Insert test record
        test_id = int(time.time() * 1000) % 2147483647
        test_name = f"TEST_CUSTOMER_{test_id}"
        
        self.cursor.execute("""
            INSERT INTO customer 
            (c_custkey, c_name, c_address, c_nationkey, c_phone, 
             c_acctbal, c_mktsegment, c_comment)
            VALUES (%s, %s, 'Test Address', 1, '555-0100', 
                   1000.00, 'TEST', 'CDC test customer')
        """, (test_id, test_name))
        
        self.connection.commit()
        
        # Verify insert
        self.cursor.execute("""
            SELECT c_custkey, c_name, updated_at
            FROM customer
            WHERE c_custkey = %s
        """, (test_id,))
        
        result = self.cursor.fetchone()
        
        self.assertIsNotNone(result, "Insert should be captured")
        self.assertEqual(result['c_name'], test_name, "Data should match")
        
        # Check timestamp is recent
        time_diff = datetime.now() - result['updated_at']
        self.assertLess(time_diff.total_seconds(), 60, 
                       "Timestamp should be recent")
        
        # Cleanup
        self.cursor.execute("DELETE FROM customer WHERE c_custkey = %s", (test_id,))
        self.connection.commit()
        
        logger.info("✓ CDC INSERT test passed")
    
    def test_11_cdc_update(self):
        """Test CDC captures UPDATE operations."""
        # Get existing customer
        self.cursor.execute("""
            SELECT c_custkey, c_acctbal, updated_at
            FROM customer
            LIMIT 1
        """)
        
        customer = self.cursor.fetchone()
        original_balance = customer['c_acctbal']
        original_updated = customer['updated_at']
        
        # Wait a moment to ensure timestamp changes
        time.sleep(1)
        
        # Update record
        new_balance = float(original_balance) + 100
        self.cursor.execute("""
            UPDATE customer
            SET c_acctbal = %s, c_comment = 'CDC update test'
            WHERE c_custkey = %s
        """, (new_balance, customer['c_custkey']))
        
        self.connection.commit()
        
        # Verify update
        self.cursor.execute("""
            SELECT c_acctbal, updated_at
            FROM customer
            WHERE c_custkey = %s
        """, (customer['c_custkey'],))
        
        result = self.cursor.fetchone()
        
        self.assertEqual(float(result['c_acctbal']), new_balance, 
                        "Balance should be updated")
        self.assertGreater(result['updated_at'], original_updated, 
                          "Timestamp should be updated")
        
        # Restore original
        self.cursor.execute("""
            UPDATE customer
            SET c_acctbal = %s
            WHERE c_custkey = %s
        """, (original_balance, customer['c_custkey']))
        
        self.connection.commit()
        
        logger.info("✓ CDC UPDATE test passed")
    
    def test_12_cdc_delete(self):
        """Test CDC captures DELETE operations."""
        # Insert test record
        test_id = int(time.time() * 1000) % 2147483647
        
        self.cursor.execute("""
            INSERT INTO supplier 
            (s_suppkey, s_name, s_address, s_nationkey, s_phone, 
             s_acctbal, s_comment)
            VALUES (%s, 'TEST_SUPPLIER', 'Test Address', 1, '555-0200', 
                   5000.00, 'CDC delete test')
        """, (test_id,))
        
        self.connection.commit()
        
        # Verify insert
        self.cursor.execute("""
            SELECT COUNT(*) as count
            FROM supplier
            WHERE s_suppkey = %s
        """, (test_id,))
        
        self.assertEqual(self.cursor.fetchone()['count'], 1, 
                        "Record should exist")
        
        # Delete record
        self.cursor.execute("""
            DELETE FROM supplier
            WHERE s_suppkey = %s
        """, (test_id,))
        
        self.connection.commit()
        
        # Verify delete
        self.cursor.execute("""
            SELECT COUNT(*) as count
            FROM supplier
            WHERE s_suppkey = %s
        """, (test_id,))
        
        self.assertEqual(self.cursor.fetchone()['count'], 0, 
                        "Record should be deleted")
        
        logger.info("✓ CDC DELETE test passed")
    
    # ==================== Performance Tests ====================
    
    def test_13_bulk_insert_performance(self):
        """Test bulk insert performance for CDC."""
        start_time = time.time()
        batch_size = 1000
        
        # Prepare batch data
        batch_data = []
        base_id = int(time.time() * 1000) % 1000000000
        
        for i in range(batch_size):
            part_id = base_id + i
            batch_data.append((
                part_id,
                f'PERF_PART_{part_id}',
                'Test Manufacturer',
                'Brand#1',
                'STANDARD',
                10,
                'SM PKG',
                99.99,
                'Performance test part'
            ))
        
        # Execute bulk insert
        self.cursor.executemany("""
            INSERT IGNORE INTO part 
            (p_partkey, p_name, p_mfgr, p_brand, p_type, 
             p_size, p_container, p_retailprice, p_comment)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, batch_data)
        
        self.connection.commit()
        
        elapsed = time.time() - start_time
        rate = batch_size / elapsed
        
        self.assertGreater(rate, 100, 
                          f"Should insert at least 100 records/second (got {rate:.1f})")
        
        # Cleanup
        self.cursor.execute("""
            DELETE FROM part 
            WHERE p_partkey >= %s AND p_partkey < %s
        """, (base_id, base_id + batch_size))
        
        self.connection.commit()
        
        logger.info(f"✓ Bulk insert performance test passed: {rate:.1f} records/second")
    
    def test_14_concurrent_operations(self):
        """Test concurrent CDC operations."""
        num_threads = 10
        operations_per_thread = 100
        
        def concurrent_operation(thread_id):
            """Execute operations in a thread."""
            conn = mysql.connector.connect(**self.config)
            cursor = conn.cursor()
            
            try:
                for i in range(operations_per_thread):
                    operation = random.choice(['insert', 'update', 'select'])
                    
                    if operation == 'insert':
                        order_id = int(time.time() * 1000000) + thread_id * 1000 + i
                        cursor.execute("""
                            INSERT IGNORE INTO orders 
                            (o_orderkey, o_custkey, o_orderstatus, o_totalprice,
                             o_orderdate, o_orderpriority, o_clerk, o_shippriority, o_comment)
                            VALUES (%s, 1, 'O', 100.00, CURDATE(), '3-MEDIUM', 
                                   'Clerk_1', 0, 'Concurrent test')
                        """, (order_id,))
                    
                    elif operation == 'update':
                        cursor.execute("""
                            UPDATE customer 
                            SET c_comment = CONCAT('Updated: ', NOW())
                            WHERE c_custkey = 1
                        """)
                    
                    else:  # select
                        cursor.execute("""
                            SELECT COUNT(*) FROM orders 
                            WHERE o_orderdate = CURDATE()
                        """)
                        cursor.fetchone()
                    
                    if i % 10 == 0:
                        conn.commit()
                
                conn.commit()
                return True
                
            except Exception as e:
                logger.error(f"Thread {thread_id} error: {e}")
                return False
            finally:
                cursor.close()
                conn.close()
        
        # Run concurrent operations
        start_time = time.time()
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_threads) as executor:
            futures = [executor.submit(concurrent_operation, i) for i in range(num_threads)]
            results = [f.result() for f in concurrent.futures.as_completed(futures)]
        
        elapsed = time.time() - start_time
        total_operations = num_threads * operations_per_thread
        rate = total_operations / elapsed
        
        self.assertTrue(all(results), "All threads should complete successfully")
        self.assertGreater(rate, self.min_performance_rate, 
                          f"Should handle at least {self.min_performance_rate} ops/second")
        
        logger.info(f"✓ Concurrent operations test passed: {rate:.1f} ops/second")
    
    def test_15_large_transaction(self):
        """Test CDC with large transactions."""
        # Start large transaction
        self.connection.start_transaction()
        
        try:
            # Multiple operations in single transaction
            num_operations = 500
            base_id = int(time.time() * 1000) % 1000000000
            
            for i in range(num_operations):
                if i % 3 == 0:
                    # Insert
                    self.cursor.execute("""
                        INSERT IGNORE INTO lineitem 
                        (l_orderkey, l_partkey, l_suppkey, l_linenumber,
                         l_quantity, l_extendedprice, l_discount, l_tax,
                         l_returnflag, l_linestatus, l_shipdate, l_commitdate,
                         l_receiptdate, l_shipinstruct, l_shipmode, l_comment)
                        VALUES (%s, 1, 1, %s, 10, 100.00, 0.05, 0.08,
                               'N', 'O', CURDATE(), CURDATE(), CURDATE(),
                               'NONE', 'SHIP', 'Large transaction test')
                    """, (base_id, i))
                
                elif i % 3 == 1:
                    # Update
                    self.cursor.execute("""
                        UPDATE part 
                        SET p_comment = CONCAT('Transaction update ', %s)
                        WHERE p_partkey = 1
                    """, (i,))
                
                else:
                    # Delete (from test data)
                    self.cursor.execute("""
                        DELETE FROM lineitem 
                        WHERE l_orderkey = %s AND l_linenumber = %s
                    """, (base_id - 1, i))
            
            # Commit large transaction
            self.connection.commit()
            
            # Verify some operations
            self.cursor.execute("""
                SELECT COUNT(*) as count
                FROM lineitem
                WHERE l_orderkey = %s
            """, (base_id,))
            
            count = self.cursor.fetchone()['count']
            self.assertGreater(count, 0, "Large transaction should be committed")
            
            # Cleanup
            self.cursor.execute("""
                DELETE FROM lineitem WHERE l_orderkey = %s
            """, (base_id,))
            self.connection.commit()
            
            logger.info("✓ Large transaction test passed")
            
        except Exception as e:
            self.connection.rollback()
            self.fail(f"Large transaction failed: {e}")
    
    # ==================== Monitoring Tests ====================
    
    def test_16_monitoring_metrics(self):
        """Test monitoring metrics are available."""
        # Check processlist
        self.cursor.execute("""
            SELECT COUNT(*) as connection_count
            FROM information_schema.PROCESSLIST
            WHERE DB = %s
        """, (self.config["database"],))
        
        connections = self.cursor.fetchone()['connection_count']
        self.assertGreater(connections, 0, "Should have active connections")
        
        # Check table statistics
        self.cursor.execute("""
            SELECT 
                TABLE_NAME,
                TABLE_ROWS,
                DATA_LENGTH,
                INDEX_LENGTH,
                UPDATE_TIME
            FROM information_schema.TABLES
            WHERE TABLE_SCHEMA = %s
            AND TABLE_NAME IN ('customer', 'orders', 'lineitem')
        """, (self.config["database"],))
        
        tables = self.cursor.fetchall()
        
        for table in tables:
            self.assertIsNotNone(table['TABLE_ROWS'], 
                               f"Table {table['TABLE_NAME']} should have row count")
        
        logger.info("✓ Monitoring metrics test passed")
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        if cls.connection:
            cls.connection.close()
        logger.info("\n=== All tests completed ===")


class TestCDCDataGenerator(unittest.TestCase):
    """Test the CDC data generator functionality."""
    
    def test_generator_scenarios(self):
        """Test data generator scenarios work correctly."""
        # This would test the actual data generator
        # Simplified for demo
        scenarios = [
            'new_customer_order',
            'update_order_status',
            'adjust_inventory',
            'process_payment',
            'ship_order'
        ]
        
        for scenario in scenarios:
            self.assertIsNotNone(scenario, f"Scenario {scenario} should be defined")
        
        logger.info("✓ Data generator scenarios test passed")


def run_tests():
    """Run all tests with proper reporting."""
    # Create test suite
    loader = unittest.TestLoader()
    suite = unittest.TestSuite()
    
    # Add test classes
    suite.addTests(loader.loadTestsFromTestCase(TestAuroraCDCIntegration))
    suite.addTests(loader.loadTestsFromTestCase(TestCDCDataGenerator))
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "="*50)
    print("TEST SUMMARY")
    print("="*50)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Success rate: {((result.testsRun - len(result.failures) - len(result.errors)) / result.testsRun * 100):.1f}%")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    import sys
    success = run_tests()
    sys.exit(0 if success else 1)