#!/bin/bash

# Resume setup for manually created Aurora (not CloudFormation)

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Configuration
DEMO_NAME="${DEMO_NAME:-aurora-cdc-demo}"
AWS_REGION="${AWS_REGION:-us-east-1}"
CLUSTER_ID="${CLUSTER_ID:-aurora-cdc-demo-cluster}"
TPCH_SCALE="${TPCH_SCALE:-1}"

echo -e "${BLUE}=================================================${NC}"
echo -e "${BLUE}    Resuming Setup (Manual Aurora)              ${NC}"
echo -e "${BLUE}=================================================${NC}"

# Function to check Aurora cluster directly (not via CloudFormation)
check_aurora_cluster() {
    echo -e "\n${YELLOW}Checking Aurora cluster...${NC}"
    
    # Check cluster directly in RDS
    CLUSTER_INFO=$(aws rds describe-db-clusters \
        --db-cluster-identifier ${CLUSTER_ID} \
        --query 'DBClusters[0]' \
        --output json 2>/dev/null || echo "{}")
    
    if [ "$CLUSTER_INFO" == "{}" ]; then
        echo -e "${RED}✗ Aurora cluster ${CLUSTER_ID} not found${NC}"
        echo "Please ensure the cluster exists or run ./create_aurora_final.sh"
        exit 1
    fi
    
    # Extract cluster details
    AURORA_ENDPOINT=$(echo $CLUSTER_INFO | jq -r '.Endpoint')
    AURORA_PORT=$(echo $CLUSTER_INFO | jq -r '.Port')
    CLUSTER_STATUS=$(echo $CLUSTER_INFO | jq -r '.Status')
    ENGINE_VERSION=$(echo $CLUSTER_INFO | jq -r '.EngineVersion')
    
    if [ "$CLUSTER_STATUS" != "available" ]; then
        echo -e "${YELLOW}⚠ Cluster status: ${CLUSTER_STATUS}${NC}"
        echo "Waiting for cluster to be available..."
        aws rds wait db-cluster-available --db-cluster-identifier ${CLUSTER_ID}
    fi
    
    echo -e "${GREEN}✓ Aurora cluster found and available${NC}"
    echo "  Cluster ID: ${CLUSTER_ID}"
    echo "  Endpoint: ${AURORA_ENDPOINT}"
    echo "  Port: ${AURORA_PORT}"
    echo "  Status: ${CLUSTER_STATUS}"
    echo "  Version: ${ENGINE_VERSION}"
}

# Function to get database credentials
get_database_credentials() {
    echo -e "\n${YELLOW}Getting database credentials...${NC}"
    
    # First check if we have saved config
    if [ -f "config/aurora_connection.json" ]; then
        echo "Found saved configuration"
        DB_USERNAME=$(jq -r '.username' config/aurora_connection.json 2>/dev/null || echo "")
        DB_PASSWORD=$(jq -r '.password' config/aurora_connection.json 2>/dev/null || echo "")
        
        if [ -n "$DB_USERNAME" ] && [ -n "$DB_PASSWORD" ]; then
            echo -e "${GREEN}✓ Using saved credentials${NC}"
            return
        fi
    fi
    
    # Try to get from Secrets Manager
    SECRET_NAME="${DEMO_NAME}-aurora-credentials"
    DB_CREDENTIALS=$(aws secretsmanager get-secret-value \
        --secret-id ${SECRET_NAME} \
        --query SecretString \
        --output text 2>/dev/null || echo "")
    
    if [ -n "$DB_CREDENTIALS" ]; then
        echo -e "${GREEN}✓ Found credentials in Secrets Manager${NC}"
        DB_USERNAME=$(echo ${DB_CREDENTIALS} | jq -r '.username')
        DB_PASSWORD=$(echo ${DB_CREDENTIALS} | jq -r '.password')
    else
        echo -e "${YELLOW}Credentials not found. Please enter manually:${NC}"
        read -p "Database username (default: admin): " DB_USERNAME
        DB_USERNAME="${DB_USERNAME:-admin}"
        read -s -p "Database password: " DB_PASSWORD
        echo ""
        
        # Save to Secrets Manager for future use
        echo -e "\n${YELLOW}Saving credentials to Secrets Manager...${NC}"
        aws secretsmanager create-secret \
            --name ${SECRET_NAME} \
            --secret-string "{\"username\":\"${DB_USERNAME}\",\"password\":\"${DB_PASSWORD}\"}" \
            --region ${AWS_REGION} 2>/dev/null || \
        aws secretsmanager update-secret \
            --secret-id ${SECRET_NAME} \
            --secret-string "{\"username\":\"${DB_USERNAME}\",\"password\":\"${DB_PASSWORD}\"}" \
            --region ${AWS_REGION}
    fi
}

# Function to test Aurora connection
test_aurora_connection() {
    echo -e "\n${YELLOW}Testing Aurora connection...${NC}"
    
    # Install pymysql if needed
    python3 -c "import pymysql" 2>/dev/null || {
        echo "Installing pymysql..."
        pip3 install -q pymysql
    }
    
    # Test connection
    python3 - <<EOF
import sys
import pymysql

try:
    connection = pymysql.connect(
        host='${AURORA_ENDPOINT}',
        port=${AURORA_PORT},
        user='${DB_USERNAME}',
        password='${DB_PASSWORD}'
    )
    print("✓ Successfully connected to Aurora")
    
    # Check binlog settings
    with connection.cursor() as cursor:
        cursor.execute("SHOW VARIABLES LIKE 'binlog_format'")
        binlog_format = cursor.fetchone()
        cursor.execute("SHOW VARIABLES LIKE 'binlog_row_image'")
        binlog_row_image = cursor.fetchone()
        cursor.execute("SHOW VARIABLES LIKE 'log_bin'")
        log_bin = cursor.fetchone()
        
        print(f"  binlog_format: {binlog_format[1] if binlog_format else 'NOT SET'}")
        print(f"  binlog_row_image: {binlog_row_image[1] if binlog_row_image else 'NOT SET'}")
        print(f"  log_bin: {log_bin[1] if log_bin else 'NOT SET'}")
        
        if binlog_format and binlog_format[1] == 'ROW':
            print("✓ CDC parameters are properly configured")
        else:
            print("⚠ CDC parameters may need configuration")
    
    connection.close()
    sys.exit(0)
    
except Exception as e:
    print(f"✗ Connection failed: {e}")
    sys.exit(1)
EOF
    
    if [ $? -ne 0 ]; then
        echo -e "${RED}Cannot connect to Aurora${NC}"
        exit 1
    fi
}

# Function to setup database
setup_database() {
    echo -e "\n${YELLOW}Setting up TPC-H database...${NC}"
    
    # Check if database already exists
    DB_EXISTS=$(python3 -c "
import pymysql
try:
    conn = pymysql.connect(host='${AURORA_ENDPOINT}', port=${AURORA_PORT}, user='${DB_USERNAME}', password='${DB_PASSWORD}')
    cursor = conn.cursor()
    cursor.execute('SHOW DATABASES LIKE \"tpch\"')
    result = cursor.fetchone()
    print('true' if result else 'false')
    conn.close()
except:
    print('false')
" 2>/dev/null)
    
    if [ "$DB_EXISTS" == "true" ]; then
        echo -e "${GREEN}✓ Database 'tpch' already exists${NC}"
        
        # Check if tables exist
        TABLE_COUNT=$(python3 -c "
import pymysql
conn = pymysql.connect(host='${AURORA_ENDPOINT}', port=${AURORA_PORT}, user='${DB_USERNAME}', password='${DB_PASSWORD}', database='tpch')
cursor = conn.cursor()
cursor.execute('SHOW TABLES')
tables = cursor.fetchall()
print(len(tables))
conn.close()
" 2>/dev/null || echo "0")
        
        echo "  Found $TABLE_COUNT tables"
        
        if [ "$TABLE_COUNT" -gt 0 ]; then
            echo -e "${YELLOW}Database already populated. Skipping data load.${NC}"
            return
        fi
    fi
    
    # Install requirements
    echo "Installing Python requirements..."
    pip3 install -q pymysql pandas numpy faker tqdm 2>/dev/null || true
    
    # Run database setup script
    if [ -f "../../scripts/setup_tpch_database.py" ]; then
        echo "Running TPC-H setup script..."
        python3 ../../scripts/setup_tpch_database.py \
            --host ${AURORA_ENDPOINT} \
            --port ${AURORA_PORT} \
            --username ${DB_USERNAME} \
            --password "${DB_PASSWORD}" \
            --scale ${TPCH_SCALE}
    else
        echo -e "${YELLOW}TPC-H setup script not found. Creating basic database...${NC}"
        python3 -c "
import pymysql
conn = pymysql.connect(host='${AURORA_ENDPOINT}', port=${AURORA_PORT}, user='${DB_USERNAME}', password='${DB_PASSWORD}')
cursor = conn.cursor()
cursor.execute('CREATE DATABASE IF NOT EXISTS tpch')
cursor.execute('USE tpch')

# Create a simple test table
cursor.execute('''
CREATE TABLE IF NOT EXISTS test_table (
    id INT PRIMARY KEY AUTO_INCREMENT,
    data VARCHAR(100),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
)
''')

# Insert some test data
for i in range(10):
    cursor.execute('INSERT INTO test_table (data) VALUES (%s)', (f'Test record {i}',))

conn.commit()
print('✓ Created test database with sample table')
conn.close()
"
    fi
    
    echo -e "${GREEN}✓ Database setup completed${NC}"
}

# Function to save configuration
save_configuration() {
    echo -e "\n${YELLOW}Saving configuration...${NC}"
    
    mkdir -p config
    
    # Save Aurora connection info
    cat > config/aurora_connection.json <<EOF
{
    "cluster_id": "${CLUSTER_ID}",
    "host": "${AURORA_ENDPOINT}",
    "port": ${AURORA_PORT},
    "database": "tpch",
    "username": "${DB_USERNAME}",
    "password": "${DB_PASSWORD}",
    "region": "${AWS_REGION}",
    "engine_version": "${ENGINE_VERSION}"
}
EOF
    
    # Save Databricks configuration
    cat > config/databricks_connection.json <<EOF
{
    "aurora": {
        "host": "${AURORA_ENDPOINT}",
        "port": ${AURORA_PORT},
        "database": "tpch",
        "username": "${DB_USERNAME}",
        "password": "${DB_PASSWORD}"
    },
    "unity_catalog": {
        "catalog": "cdc_demo",
        "schema": "tpch",
        "volume_path": "/Volumes/cdc_demo/tpch/cdc_data",
        "checkpoint_path": "/Volumes/cdc_demo/tpch/checkpoints"
    },
    "streaming": {
        "parallelism": "20",
        "batch_size": "10000",
        "max_tables_per_batch": "50",
        "fetch_interval_seconds": "900",
        "real_time_mode": "true"
    }
}
EOF
    
    echo -e "${GREEN}✓ Configuration files saved${NC}"
}

# Function to generate Databricks notebook
generate_databricks_notebook() {
    echo -e "\n${YELLOW}Generating Databricks notebook...${NC}"
    
    if [ -f "../../scripts/generate_databricks_notebook.py" ]; then
        python3 ../../scripts/generate_databricks_notebook.py \
            --config config/databricks_connection.json \
            --output ../../notebooks/demo/demo_cdc_setup.py
        
        echo -e "${GREEN}✓ Databricks notebook generated${NC}"
        echo "  Location: notebooks/demo/demo_cdc_setup.py"
    else
        echo -e "${YELLOW}Notebook generator not found. Skipping.${NC}"
    fi
}

# Function to display summary
display_summary() {
    echo -e "\n${GREEN}=================================================${NC}"
    echo -e "${GREEN}          Setup Completed Successfully!          ${NC}"
    echo -e "${GREEN}=================================================${NC}"
    
    echo -e "\n${BLUE}Aurora MySQL Details:${NC}"
    echo "  Endpoint: ${AURORA_ENDPOINT}"
    echo "  Port: ${AURORA_PORT}"
    echo "  Database: tpch"
    echo "  Username: ${DB_USERNAME}"
    
    echo -e "\n${BLUE}Configuration Files:${NC}"
    echo "  config/aurora_connection.json"
    echo "  config/databricks_connection.json"
    
    echo -e "\n${BLUE}Next Steps:${NC}"
    echo ""
    echo "1. Test Aurora connection:"
    echo "   mysql -h ${AURORA_ENDPOINT} -P ${AURORA_PORT} -u ${DB_USERNAME} -p"
    echo ""
    echo "2. Start CDC data generator:"
    echo "   python3 ../../scripts/cdc_data_generator.py --config config/aurora_connection.json"
    echo ""
    echo "3. Import Databricks notebook:"
    echo "   notebooks/demo/demo_cdc_setup.py"
    echo ""
    echo "4. For Debezium approach:"
    echo "   cd ../../debezium-approach/setup"
    echo "   docker-compose up -d"
    echo "   ./configure_aurora_connector.sh"
    
    echo -e "\n${BLUE}Useful Commands:${NC}"
    echo "  View password:"
    echo "    cat config/aurora_connection.json | jq -r '.password'"
    echo "  Or from Secrets Manager:"
    echo "    aws secretsmanager get-secret-value --secret-id ${DEMO_NAME}-aurora-credentials --query SecretString --output text | jq -r '.password'"
    
    echo -e "\n${GREEN}Your Aurora CDC environment is ready!${NC}"
}

# Main execution
main() {
    echo ""
    
    # Check Aurora cluster directly (not CloudFormation)
    check_aurora_cluster
    
    # Get database credentials
    get_database_credentials
    
    # Test Aurora connection
    test_aurora_connection
    
    # Setup database
    setup_database
    
    # Save configuration
    save_configuration
    
    # Generate Databricks notebook
    generate_databricks_notebook
    
    # Display summary
    display_summary
}

# Run main function
main