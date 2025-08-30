# Debezium CDC Approach for Aurora MySQL to Databricks

This folder contains an alternative CDC implementation using Debezium, Kafka, and Delta Live Tables (DLT) as outlined in the [Databricks DLT tutorial](https://docs.databricks.com/aws/en/dlt/tutorial-pipelines).

## Architecture

```
Aurora MySQL → Debezium → Kafka → Delta Live Tables → Unity Catalog (Delta)
```

## Comparison with Custom DataSource Approach

| Feature | Custom DataSource | Debezium Approach |
|---------|------------------|-------------------|
| **Setup Complexity** | Simple (Direct connection) | Complex (Kafka + Debezium) |
| **Latency** | <100ms | ~1-5 seconds |
| **Scalability** | Good (20 parallel connections) | Excellent (Kafka partitions) |
| **Reliability** | Checkpointing | Kafka + DLT guarantees |
| **Schema Evolution** | Manual handling | Automatic with Schema Registry |
| **Operational Overhead** | Low | High (Kafka cluster management) |
| **Cost** | Aurora + Databricks | Aurora + Kafka + Databricks |
| **CDC Features** | Basic (timestamp-based) | Advanced (true binlog parsing) |

## Quick Start

### 1. Deploy Kafka and Debezium
```bash
cd setup
./deploy_kafka_debezium.sh
```

### 2. Configure Aurora Connector
```bash
./configure_aurora_connector.sh
```

### 3. Create DLT Pipeline
```bash
./create_dlt_pipeline.sh
```

### 4. Run Tests
```bash
cd ../tests
python test_debezium_integration.py
```

## Components

- **Kafka**: MSK (Managed Streaming for Kafka) or self-managed
- **Debezium**: MySQL connector for binlog parsing
- **Schema Registry**: Confluent Schema Registry for schema evolution
- **Delta Live Tables**: Streaming pipeline in Databricks
- **Unity Catalog**: Target storage with governance

## Benefits of Debezium Approach

1. **True CDC**: Reads MySQL binlog directly
2. **Exactly-once semantics**: Kafka + DLT guarantees
3. **Schema evolution**: Automatic handling
4. **Decoupled architecture**: Kafka as intermediate buffer
5. **Multiple consumers**: Same CDC stream for multiple targets

## When to Use This Approach

Choose Debezium when you need:
- Multiple consumers for CDC data
- Complex transformations before landing
- Integration with existing Kafka infrastructure
- Advanced CDC features (DDL capture, schema changes)
- Decoupled, microservices architecture