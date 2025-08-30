#!/usr/bin/env python3
"""
Comprehensive Test Suite for Debezium CDC Approach
Tests Kafka, Debezium, and DLT pipeline integration
"""

import unittest
import json
import time
import mysql.connector
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import KafkaError
import requests
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TestDebeziumIntegration(unittest.TestCase):
    """Test suite for Debezium CDC integration."""
    
    @classmethod
    def setUpClass(cls):
        """Set up test environment."""
        cls.config = {
            # Kafka configuration
            "kafka_bootstrap": "localhost:9092",
            "schema_registry": "http://localhost:8081",
            "kafka_connect": "http://localhost:8083",
            
            # Aurora configuration
            "aurora_host": "localhost",  # Replace with actual
            "aurora_port": 3306,
            "aurora_user": "cdc_user",
            "aurora_password": "test_password",
            "aurora_database": "tpch",
            
            # Test configuration
            "test_timeout": 60,
            "connector_name": "aurora-mysql-connector"
        }
        
        # Initialize clients
        cls.kafka_admin = None
        cls.kafka_producer = None
        cls.kafka_consumer = None
        cls.mysql_connection = None
        
        cls._initialize_clients()
    
    @classmethod
    def _initialize_clients(cls):
        """Initialize test clients."""
        try:
            # Kafka Admin Client
            cls.kafka_admin = KafkaAdminClient(
                bootstrap_servers=cls.config["kafka_bootstrap"],
                client_id='test-admin'
            )
            
            # Kafka Producer
            cls.kafka_producer = KafkaProducer(
                bootstrap_servers=cls.config["kafka_bootstrap"],
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            
            # MySQL Connection
            cls.mysql_connection = mysql.connector.connect(
                host=cls.config["aurora_host"],
                port=cls.config["aurora_port"],
                user=cls.config["aurora_user"],
                password=cls.config["aurora_password"],
                database=cls.config["aurora_database"]
            )
            
            logger.info("Test clients initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize clients: {e}")
            raise
    
    def setUp(self):
        """Set up each test."""
        self.cursor = self.mysql_connection.cursor(dictionary=True)
    
    def tearDown(self):
        """Clean up after each test."""
        if self.cursor:
            self.cursor.close()
        if self.kafka_consumer:
            self.kafka_consumer.close()
    
    # ==================== Kafka Tests ====================
    
    def test_01_kafka_connectivity(self):
        """Test Kafka cluster connectivity."""
        # Check cluster metadata
        metadata = self.kafka_admin._client.cluster
        self.assertIsNotNone(metadata, "Should get cluster metadata")
        
        # Check brokers
        brokers = metadata.brokers()
        self.assertGreater(len(brokers), 0, "Should have at least one broker")
        
        logger.info(f"✓ Kafka cluster has {len(brokers)} broker(s)")
    
    def test_02_schema_registry(self):
        """Test Schema Registry connectivity."""
        try:
            response = requests.get(f"{self.config['schema_registry']}/subjects")
            self.assertEqual(response.status_code, 200, 
                           "Schema Registry should be accessible")
            
            subjects = response.json()
            logger.info(f"✓ Schema Registry has {len(subjects)} subjects")
            
        except requests.exceptions.RequestException as e:
            self.fail(f"Cannot connect to Schema Registry: {e}")
    
    def test_03_kafka_connect(self):
        """Test Kafka Connect cluster."""
        try:
            # Check Kafka Connect health
            response = requests.get(f"{self.config['kafka_connect']}/")
            self.assertEqual(response.status_code, 200, 
                           "Kafka Connect should be running")
            
            # Check connector plugins
            response = requests.get(f"{self.config['kafka_connect']}/connector-plugins")
            plugins = response.json()
            
            # Check for Debezium MySQL connector
            debezium_found = any(
                'debezium' in p['class'].lower() and 'mysql' in p['class'].lower()
                for p in plugins
            )
            self.assertTrue(debezium_found, 
                          "Debezium MySQL connector should be available")
            
            logger.info(f"✓ Kafka Connect has {len(plugins)} connector plugins")
            
        except requests.exceptions.RequestException as e:
            self.fail(f"Cannot connect to Kafka Connect: {e}")
    
    # ==================== Debezium Connector Tests ====================
    
    def test_04_debezium_connector_deployment(self):
        """Test Debezium connector deployment."""
        connector_name = self.config["connector_name"]
        
        # Check if connector exists
        response = requests.get(
            f"{self.config['kafka_connect']}/connectors/{connector_name}"
        )
        
        if response.status_code == 404:
            logger.warning(f"Connector {connector_name} not found. Deploy it first.")
            self.skipTest("Connector not deployed")
        
        self.assertEqual(response.status_code, 200, 
                        "Connector should exist")
        
        # Check connector status
        response = requests.get(
            f"{self.config['kafka_connect']}/connectors/{connector_name}/status"
        )
        status = response.json()
        
        self.assertEqual(status['connector']['state'], 'RUNNING',
                        "Connector should be running")
        
        # Check tasks
        self.assertGreater(len(status['tasks']), 0, 
                          "Connector should have at least one task")
        
        for task in status['tasks']:
            self.assertEqual(task['state'], 'RUNNING',
                           f"Task {task['id']} should be running")
        
        logger.info(f"✓ Debezium connector is running with {len(status['tasks'])} task(s)")
    
    def test_05_cdc_topics_created(self):
        """Test that CDC topics are created."""
        # List all topics
        topics = self.kafka_admin.list_topics()
        
        # Check for CDC topics
        cdc_topics = [t for t in topics if t.startswith('cdc.')]
        self.assertGreater(len(cdc_topics), 0, 
                          "Should have CDC topics created")
        
        # Check for main tables
        expected_tables = ['customer', 'orders', 'lineitem']
        for table in expected_tables:
            topic = f"cdc.{self.config['aurora_database']}.{table}"
            self.assertIn(topic, topics, 
                         f"Topic {topic} should exist")
        
        logger.info(f"✓ Found {len(cdc_topics)} CDC topics")
    
    # ==================== CDC Functionality Tests ====================
    
    def test_06_cdc_insert_capture(self):
        """Test CDC captures INSERT operations."""
        table = "test_cdc_insert"
        topic = f"cdc.{self.config['aurora_database']}.{table}"
        
        # Create test table
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INT PRIMARY KEY,
                data VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
        """)
        self.mysql_connection.commit()
        
        # Create Kafka consumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.config["kafka_bootstrap"],
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=10000
        )
        
        # Insert test data
        test_id = int(time.time())
        test_data = f"Test INSERT {test_id}"
        
        self.cursor.execute(f"""
            INSERT INTO {table} (id, data) VALUES (%s, %s)
        """, (test_id, test_data))
        self.mysql_connection.commit()
        
        # Consume CDC event
        event_found = False
        for message in consumer:
            event = message.value
            
            if event.get('after', {}).get('id') == test_id:
                self.assertEqual(event.get('op'), 'c', "Operation should be 'c' (create)")
                self.assertEqual(event.get('after', {}).get('data'), test_data,
                               "Data should match")
                event_found = True
                break
        
        self.assertTrue(event_found, "CDC event should be captured")
        
        # Cleanup
        self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
        self.mysql_connection.commit()
        consumer.close()
        
        logger.info("✓ CDC INSERT capture test passed")
    
    def test_07_cdc_update_capture(self):
        """Test CDC captures UPDATE operations."""
        # Use existing customer table
        self.cursor.execute("""
            SELECT c_custkey, c_acctbal 
            FROM customer 
            LIMIT 1
        """)
        customer = self.cursor.fetchone()
        
        if not customer:
            self.skipTest("No customer data available")
        
        topic = f"cdc.{self.config['aurora_database']}.customer"
        
        # Create consumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.config["kafka_bootstrap"],
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=10000
        )
        
        # Update customer
        new_balance = float(customer['c_acctbal']) + 100
        self.cursor.execute("""
            UPDATE customer 
            SET c_acctbal = %s, c_comment = 'CDC test update'
            WHERE c_custkey = %s
        """, (new_balance, customer['c_custkey']))
        self.mysql_connection.commit()
        
        # Consume CDC event
        event_found = False
        for message in consumer:
            event = message.value
            
            if event.get('after', {}).get('c_custkey') == customer['c_custkey']:
                self.assertEqual(event.get('op'), 'u', "Operation should be 'u' (update)")
                self.assertIsNotNone(event.get('before'), "Should have before image")
                self.assertEqual(float(event.get('after', {}).get('c_acctbal')), 
                               new_balance, "New balance should match")
                event_found = True
                break
        
        self.assertTrue(event_found, "CDC UPDATE event should be captured")
        
        # Restore original value
        self.cursor.execute("""
            UPDATE customer 
            SET c_acctbal = %s
            WHERE c_custkey = %s
        """, (customer['c_acctbal'], customer['c_custkey']))
        self.mysql_connection.commit()
        
        consumer.close()
        logger.info("✓ CDC UPDATE capture test passed")
    
    def test_08_cdc_delete_capture(self):
        """Test CDC captures DELETE operations."""
        table = "test_cdc_delete"
        topic = f"cdc.{self.config['aurora_database']}.{table}"
        
        # Create test table
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INT PRIMARY KEY,
                data VARCHAR(100)
            )
        """)
        
        # Insert test data
        test_id = int(time.time())
        self.cursor.execute(f"""
            INSERT INTO {table} (id, data) VALUES (%s, 'To be deleted')
        """, (test_id,))
        self.mysql_connection.commit()
        
        # Create consumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.config["kafka_bootstrap"],
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=10000
        )
        
        # Delete record
        self.cursor.execute(f"""
            DELETE FROM {table} WHERE id = %s
        """, (test_id,))
        self.mysql_connection.commit()
        
        # Consume CDC event
        event_found = False
        for message in consumer:
            event = message.value
            
            if event.get('before', {}).get('id') == test_id:
                self.assertEqual(event.get('op'), 'd', "Operation should be 'd' (delete)")
                self.assertIsNone(event.get('after'), "After should be None for delete")
                event_found = True
                break
        
        self.assertTrue(event_found, "CDC DELETE event should be captured")
        
        # Cleanup
        self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
        self.mysql_connection.commit()
        consumer.close()
        
        logger.info("✓ CDC DELETE capture test passed")
    
    # ==================== Performance Tests ====================
    
    def test_09_cdc_throughput(self):
        """Test CDC throughput performance."""
        table = "test_throughput"
        topic = f"cdc.{self.config['aurora_database']}.{table}"
        batch_size = 1000
        
        # Create test table
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INT PRIMARY KEY,
                data VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        self.mysql_connection.commit()
        
        # Insert batch of records
        start_time = time.time()
        base_id = int(time.time() * 1000)
        
        for i in range(batch_size):
            self.cursor.execute(f"""
                INSERT INTO {table} (id, data) 
                VALUES (%s, %s)
            """, (base_id + i, f"Throughput test {i}"))
        
        self.mysql_connection.commit()
        insert_time = time.time() - start_time
        
        # Create consumer to count events
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.config["kafka_bootstrap"],
            auto_offset_reset='earliest',
            consumer_timeout_ms=30000
        )
        
        # Count CDC events
        event_count = 0
        consume_start = time.time()
        
        for message in consumer:
            event_count += 1
            if event_count >= batch_size:
                break
        
        consume_time = time.time() - consume_start
        
        # Calculate throughput
        insert_rate = batch_size / insert_time
        consume_rate = event_count / consume_time
        
        logger.info(f"✓ Throughput test results:")
        logger.info(f"  Insert rate: {insert_rate:.1f} records/sec")
        logger.info(f"  CDC consume rate: {consume_rate:.1f} events/sec")
        
        self.assertGreater(insert_rate, 100, 
                          "Should insert at least 100 records/sec")
        self.assertGreater(consume_rate, 50,
                          "Should consume at least 50 CDC events/sec")
        
        # Cleanup
        self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
        self.mysql_connection.commit()
        consumer.close()
    
    def test_10_cdc_latency(self):
        """Test CDC end-to-end latency."""
        table = "test_latency"
        topic = f"cdc.{self.config['aurora_database']}.{table}"
        
        # Create test table
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INT PRIMARY KEY,
                data VARCHAR(100),
                created_at TIMESTAMP(6) DEFAULT CURRENT_TIMESTAMP(6)
            )
        """)
        self.mysql_connection.commit()
        
        # Create consumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=self.config["kafka_bootstrap"],
            auto_offset_reset='latest',
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            consumer_timeout_ms=5000
        )
        
        # Measure latency for multiple events
        latencies = []
        
        for i in range(10):
            test_id = int(time.time() * 1000) + i
            
            # Record insert time
            insert_time = datetime.now()
            
            self.cursor.execute(f"""
                INSERT INTO {table} (id, data) 
                VALUES (%s, %s)
            """, (test_id, f"Latency test {i}"))
            self.mysql_connection.commit()
            
            # Wait for CDC event
            for message in consumer:
                event = json.loads(message.value.decode('utf-8')) if isinstance(message.value, bytes) else message.value
                
                if event.get('after', {}).get('id') == test_id:
                    # Calculate latency
                    receive_time = datetime.now()
                    latency = (receive_time - insert_time).total_seconds()
                    latencies.append(latency)
                    break
        
        # Calculate statistics
        if latencies:
            avg_latency = sum(latencies) / len(latencies)
            max_latency = max(latencies)
            min_latency = min(latencies)
            
            logger.info(f"✓ CDC Latency test results:")
            logger.info(f"  Average: {avg_latency*1000:.1f}ms")
            logger.info(f"  Min: {min_latency*1000:.1f}ms")
            logger.info(f"  Max: {max_latency*1000:.1f}ms")
            
            self.assertLess(avg_latency, 5.0, 
                           "Average latency should be less than 5 seconds")
        
        # Cleanup
        self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
        self.mysql_connection.commit()
        consumer.close()
    
    # ==================== Schema Evolution Tests ====================
    
    def test_11_schema_evolution(self):
        """Test schema evolution handling."""
        table = "test_schema_evolution"
        topic = f"cdc.{self.config['aurora_database']}.{table}"
        
        # Create initial table
        self.cursor.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                id INT PRIMARY KEY,
                name VARCHAR(100)
            )
        """)
        self.mysql_connection.commit()
        
        # Insert initial data
        self.cursor.execute(f"""
            INSERT INTO {table} (id, name) VALUES (1, 'Initial')
        """)
        self.mysql_connection.commit()
        
        # Add new column
        self.cursor.execute(f"""
            ALTER TABLE {table} ADD COLUMN email VARCHAR(100)
        """)
        self.mysql_connection.commit()
        
        # Insert data with new schema
        self.cursor.execute(f"""
            INSERT INTO {table} (id, name, email) 
            VALUES (2, 'With Email', 'test@example.com')
        """)
        self.mysql_connection.commit()
        
        # Check schema registry for evolution
        response = requests.get(
            f"{self.config['schema_registry']}/subjects/{topic}-value/versions"
        )
        
        if response.status_code == 200:
            versions = response.json()
            self.assertGreater(len(versions), 0, 
                             "Should have schema versions")
            logger.info(f"✓ Schema has {len(versions)} version(s)")
        
        # Cleanup
        self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
        self.mysql_connection.commit()
    
    @classmethod
    def tearDownClass(cls):
        """Clean up test environment."""
        if cls.kafka_admin:
            cls.kafka_admin.close()
        if cls.kafka_producer:
            cls.kafka_producer.close()
        if cls.mysql_connection:
            cls.mysql_connection.close()
        
        logger.info("Test cleanup completed")


def run_tests():
    """Run all Debezium integration tests."""
    # Create test suite
    loader = unittest.TestLoader()
    suite = loader.loadTestsFromTestCase(TestDebeziumIntegration)
    
    # Run tests
    runner = unittest.TextTestRunner(verbosity=2)
    result = runner.run(suite)
    
    # Print summary
    print("\n" + "="*50)
    print("DEBEZIUM TEST SUMMARY")
    print("="*50)
    print(f"Tests run: {result.testsRun}")
    print(f"Failures: {len(result.failures)}")
    print(f"Errors: {len(result.errors)}")
    print(f"Skipped: {len(result.skipped)}")
    
    return result.wasSuccessful()


if __name__ == "__main__":
    import sys
    success = run_tests()
    sys.exit(0 if success else 1)