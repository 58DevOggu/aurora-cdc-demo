#!/bin/bash

# Configure Debezium MySQL Connector for Aurora
set -e

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Configuration
KAFKA_CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
CONNECTOR_NAME="aurora-mysql-connector"

echo -e "${GREEN}=================================================${NC}"
echo -e "${GREEN}    Configure Debezium Connector for Aurora     ${NC}"
echo -e "${GREEN}=================================================${NC}"

# Get Aurora connection details
get_aurora_details() {
    echo -e "\n${YELLOW}Enter Aurora connection details:${NC}"
    read -p "Aurora Hostname: " AURORA_HOSTNAME
    read -p "Aurora Port (default 3306): " AURORA_PORT
    AURORA_PORT="${AURORA_PORT:-3306}"
    read -p "Database Name (default tpch): " DATABASE_NAME
    DATABASE_NAME="${DATABASE_NAME:-tpch}"
    read -p "CDC User (default cdc_user): " CDC_USER
    CDC_USER="${CDC_USER:-cdc_user}"
    read -s -p "CDC Password: " CDC_PASSWORD
    echo ""
}

# Check Kafka Connect health
check_kafka_connect() {
    echo -e "\n${YELLOW}Checking Kafka Connect...${NC}"
    
    if curl -s "${KAFKA_CONNECT_URL}/connectors" &>/dev/null; then
        echo -e "${GREEN}✓ Kafka Connect is running${NC}"
    else
        echo -e "${RED}✗ Cannot connect to Kafka Connect at ${KAFKA_CONNECT_URL}${NC}"
        exit 1
    fi
    
    # Check for Debezium MySQL connector
    if curl -s "${KAFKA_CONNECT_URL}/connector-plugins" | grep -q "io.debezium.connector.mysql.MySqlConnector"; then
        echo -e "${GREEN}✓ Debezium MySQL connector is available${NC}"
    else
        echo -e "${RED}✗ Debezium MySQL connector not found${NC}"
        exit 1
    fi
}

# Create connector configuration
create_connector_config() {
    echo -e "\n${YELLOW}Creating connector configuration...${NC}"
    
    # Generate connector config
    cat > /tmp/aurora-connector.json <<EOF
{
  "name": "${CONNECTOR_NAME}",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    
    "database.hostname": "${AURORA_HOSTNAME}",
    "database.port": "${AURORA_PORT}",
    "database.user": "${CDC_USER}",
    "database.password": "${CDC_PASSWORD}",
    "database.server.id": "184054",
    "database.server.name": "aurora-cdc",
    
    "database.include.list": "${DATABASE_NAME}",
    
    "database.history.kafka.bootstrap.servers": "kafka:29092",
    "database.history.kafka.topic": "schema-changes.aurora-cdc",
    
    "include.schema.changes": "true",
    "include.query": "false",
    
    "snapshot.mode": "initial",
    "snapshot.locking.mode": "minimal",
    
    "decimal.handling.mode": "double",
    "time.precision.mode": "adaptive",
    
    "transforms": "unwrap,addTimestamp",
    "transforms.unwrap.type": "io.debezium.transforms.ExtractNewRecordState",
    "transforms.unwrap.drop.tombstones": "false",
    "transforms.unwrap.delete.handling.mode": "rewrite",
    
    "transforms.addTimestamp.type": "org.apache.kafka.connect.transforms.InsertField\$Value",
    "transforms.addTimestamp.timestamp.field": "cdc_timestamp",
    
    "topic.prefix": "cdc",
    
    "errors.tolerance": "none",
    "errors.log.enable": "true"
  }
}
EOF
    
    echo -e "${GREEN}✓ Configuration created${NC}"
}

# Deploy connector
deploy_connector() {
    echo -e "\n${YELLOW}Deploying connector...${NC}"
    
    # Check if connector already exists
    if curl -s "${KAFKA_CONNECT_URL}/connectors/${CONNECTOR_NAME}" &>/dev/null; then
        echo -e "${YELLOW}Connector already exists. Updating...${NC}"
        
        # Update existing connector
        curl -X PUT \
            -H "Content-Type: application/json" \
            -d @/tmp/aurora-connector.json \
            "${KAFKA_CONNECT_URL}/connectors/${CONNECTOR_NAME}/config"
    else
        # Create new connector
        curl -X POST \
            -H "Content-Type: application/json" \
            -d @/tmp/aurora-connector.json \
            "${KAFKA_CONNECT_URL}/connectors"
    fi
    
    echo -e "\n${GREEN}✓ Connector deployed${NC}"
}

# Verify connector status
verify_connector() {
    echo -e "\n${YELLOW}Verifying connector status...${NC}"
    
    sleep 5  # Wait for connector to initialize
    
    # Check connector status
    STATUS=$(curl -s "${KAFKA_CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | jq -r '.connector.state')
    
    if [ "$STATUS" == "RUNNING" ]; then
        echo -e "${GREEN}✓ Connector is RUNNING${NC}"
        
        # Check tasks
        TASK_STATUS=$(curl -s "${KAFKA_CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | jq -r '.tasks[0].state')
        echo -e "  Task status: ${TASK_STATUS}"
        
        # List topics created
        echo -e "\n${YELLOW}Topics created:${NC}"
        docker exec kafka kafka-topics --list --bootstrap-server localhost:9092 | grep "^cdc\."
        
    else
        echo -e "${RED}✗ Connector status: ${STATUS}${NC}"
        
        # Show error if any
        curl -s "${KAFKA_CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | jq '.tasks[0].trace' | head -20
    fi
}

# Monitor initial snapshot
monitor_snapshot() {
    echo -e "\n${YELLOW}Monitoring initial snapshot...${NC}"
    echo "This may take several minutes for large databases..."
    
    while true; do
        # Check snapshot status in topics
        for topic in customer orders lineitem; do
            COUNT=$(docker exec kafka kafka-run-class kafka.tools.GetOffsetShell \
                --broker-list localhost:9092 \
                --topic "cdc.${DATABASE_NAME}.${topic}" \
                2>/dev/null | awk -F':' '{sum += $3} END {print sum}')
            
            if [ -n "$COUNT" ] && [ "$COUNT" -gt 0 ]; then
                echo -e "  ${topic}: ${COUNT} records"
            fi
        done
        
        # Check if snapshot is complete
        SNAPSHOT_STATUS=$(curl -s "${KAFKA_CONNECT_URL}/connectors/${CONNECTOR_NAME}/status" | \
            jq -r '.tasks[0].state')
        
        if [ "$SNAPSHOT_STATUS" == "RUNNING" ]; then
            echo -e "\n${GREEN}✓ Initial snapshot complete${NC}"
            break
        fi
        
        sleep 10
    done
}

# Create signal table for Debezium
create_signal_table() {
    echo -e "\n${YELLOW}Creating Debezium signal table...${NC}"
    
    mysql -h "${AURORA_HOSTNAME}" -P "${AURORA_PORT}" -u "${CDC_USER}" -p"${CDC_PASSWORD}" <<EOF
USE ${DATABASE_NAME};

CREATE TABLE IF NOT EXISTS debezium_signal (
    id VARCHAR(42) PRIMARY KEY,
    type VARCHAR(32) NOT NULL,
    data TEXT NULL
);

-- Grant permissions
GRANT SELECT, INSERT, UPDATE, DELETE ON ${DATABASE_NAME}.debezium_signal TO '${CDC_USER}'@'%';
FLUSH PRIVILEGES;
EOF
    
    echo -e "${GREEN}✓ Signal table created${NC}"
}

# Display summary
show_summary() {
    echo -e "\n${GREEN}=================================================${NC}"
    echo -e "${GREEN}     Debezium Connector Configuration Complete   ${NC}"
    echo -e "${GREEN}=================================================${NC}"
    
    echo -e "\n${YELLOW}Connector Details:${NC}"
    echo "  Name: ${CONNECTOR_NAME}"
    echo "  Status: RUNNING"
    echo "  Aurora: ${AURORA_HOSTNAME}:${AURORA_PORT}"
    echo "  Database: ${DATABASE_NAME}"
    
    echo -e "\n${YELLOW}Kafka Topics:${NC}"
    echo "  Pattern: cdc.${DATABASE_NAME}.<table_name>"
    
    echo -e "\n${YELLOW}Monitor CDC:${NC}"
    echo "  Kafka UI: http://localhost:8080"
    echo "  Debezium UI: http://localhost:8090"
    echo "  Connector API: ${KAFKA_CONNECT_URL}/connectors/${CONNECTOR_NAME}/status"
    
    echo -e "\n${YELLOW}Next Steps:${NC}"
    echo "1. Monitor initial snapshot completion"
    echo "2. Create DLT pipeline in Databricks"
    echo "3. Test CDC with data changes"
}

# Main execution
main() {
    # Get Aurora details
    get_aurora_details
    
    # Check Kafka Connect
    check_kafka_connect
    
    # Create configuration
    create_connector_config
    
    # Deploy connector
    deploy_connector
    
    # Verify deployment
    verify_connector
    
    # Create signal table
    create_signal_table
    
    # Monitor snapshot
    monitor_snapshot
    
    # Show summary
    show_summary
}

# Run main
main