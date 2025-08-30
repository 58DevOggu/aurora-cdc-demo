#!/bin/bash

# Aurora CDC Demo - One-Click Setup Script
# This script sets up Aurora MySQL with TPC-H data and configures CDC for Databricks

set -e

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
DEMO_NAME="${DEMO_NAME:-aurora-cdc-demo}"
AWS_REGION="${AWS_REGION:-us-east-1}"
ENVIRONMENT="${ENVIRONMENT:-demo}"
TPCH_SCALE="${TPCH_SCALE:-1}" # Scale factor for TPC-H data (1 = 1GB)

echo -e "${GREEN}=================================================${NC}"
echo -e "${GREEN}    Aurora CDC Demo Setup for Databricks        ${NC}"
echo -e "${GREEN}=================================================${NC}"

# Function to check prerequisites
check_prerequisites() {
    echo -e "\n${YELLOW}Checking prerequisites...${NC}"
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        echo -e "${RED}AWS CLI is not installed. Please install it first.${NC}"
        exit 1
    fi
    
    # Check jq
    if ! command -v jq &> /dev/null; then
        echo -e "${YELLOW}Installing jq...${NC}"
        if [[ "$OSTYPE" == "darwin"* ]]; then
            brew install jq
        else
            sudo apt-get update && sudo apt-get install -y jq
        fi
    fi
    
    # Check MySQL client
    if ! command -v mysql &> /dev/null; then
        echo -e "${YELLOW}Installing MySQL client...${NC}"
        if [[ "$OSTYPE" == "darwin"* ]]; then
            brew install mysql-client
        else
            sudo apt-get update && sudo apt-get install -y mysql-client
        fi
    fi
    
    # Check Python
    if ! command -v python3 &> /dev/null; then
        echo -e "${RED}Python 3 is not installed. Please install it first.${NC}"
        exit 1
    fi
    
    echo -e "${GREEN}✓ All prerequisites satisfied${NC}"
}

# Function to get Databricks VPC info
get_databricks_vpc_info() {
    echo -e "\n${YELLOW}Enter your Databricks workspace VPC information:${NC}"
    read -p "Databricks VPC ID: " DATABRICKS_VPC_ID
    read -p "Databricks VPC CIDR (e.g., 10.1.0.0/16): " DATABRICKS_VPC_CIDR
    read -p "Databricks AWS Account ID: " DATABRICKS_ACCOUNT_ID
    read -p "Databricks Region (default: ${AWS_REGION}): " DATABRICKS_REGION
    DATABRICKS_REGION="${DATABRICKS_REGION:-$AWS_REGION}"
    
    # Save configuration
    cat > config/databricks_vpc.json <<EOF
{
    "vpc_id": "${DATABRICKS_VPC_ID}",
    "vpc_cidr": "${DATABRICKS_VPC_CIDR}",
    "account_id": "${DATABRICKS_ACCOUNT_ID}",
    "region": "${DATABRICKS_REGION}"
}
EOF
    
    echo -e "${GREEN}✓ Databricks VPC configuration saved${NC}"
}

# Function to deploy infrastructure
deploy_infrastructure() {
    echo -e "\n${YELLOW}Deploying AWS infrastructure...${NC}"
    
    # Create S3 bucket for CloudFormation templates
    BUCKET_NAME="${DEMO_NAME}-cf-templates-$(date +%s)"
    aws s3 mb s3://${BUCKET_NAME} --region ${AWS_REGION}
    
    # Upload CloudFormation templates
    aws s3 cp ../infrastructure/cloudformation/ s3://${BUCKET_NAME}/ --recursive
    
    # Deploy VPC stack
    echo -e "${YELLOW}Creating VPC for Aurora...${NC}"
    aws cloudformation deploy \
        --template-file ../infrastructure/cloudformation/vpc-aurora-demo.yaml \
        --stack-name ${DEMO_NAME}-vpc \
        --parameter-overrides \
            EnvironmentName=${ENVIRONMENT} \
            DatabricksVPCId=${DATABRICKS_VPC_ID} \
            DatabricksVPCCIDR=${DATABRICKS_VPC_CIDR} \
            DatabricksAccountId=${DATABRICKS_ACCOUNT_ID} \
        --capabilities CAPABILITY_IAM \
        --region ${AWS_REGION}
    
    # Get VPC outputs
    VPC_ID=$(aws cloudformation describe-stacks \
        --stack-name ${DEMO_NAME}-vpc \
        --query "Stacks[0].Outputs[?OutputKey=='VPCId'].OutputValue" \
        --output text \
        --region ${AWS_REGION})
    
    SUBNET_IDS=$(aws cloudformation describe-stacks \
        --stack-name ${DEMO_NAME}-vpc \
        --query "Stacks[0].Outputs[?OutputKey=='DatabaseSubnets'].OutputValue" \
        --output text \
        --region ${AWS_REGION})
    
    # Deploy Aurora stack
    echo -e "${YELLOW}Creating Aurora MySQL cluster...${NC}"
    aws cloudformation deploy \
        --template-file ../infrastructure/cloudformation/aurora-cluster-demo.yaml \
        --stack-name ${DEMO_NAME}-aurora \
        --parameter-overrides \
            EnvironmentName=${ENVIRONMENT} \
            VPCId=${VPC_ID} \
            SubnetIds=${SUBNET_IDS} \
            TPCHScale=${TPCH_SCALE} \
        --capabilities CAPABILITY_IAM \
        --region ${AWS_REGION}
    
    # Get Aurora endpoints
    AURORA_ENDPOINT=$(aws cloudformation describe-stacks \
        --stack-name ${DEMO_NAME}-aurora \
        --query "Stacks[0].Outputs[?OutputKey=='ClusterEndpoint'].OutputValue" \
        --output text \
        --region ${AWS_REGION})
    
    AURORA_PORT=$(aws cloudformation describe-stacks \
        --stack-name ${DEMO_NAME}-aurora \
        --query "Stacks[0].Outputs[?OutputKey=='Port'].OutputValue" \
        --output text \
        --region ${AWS_REGION})
    
    echo -e "${GREEN}✓ Infrastructure deployed successfully${NC}"
    echo -e "Aurora Endpoint: ${AURORA_ENDPOINT}:${AURORA_PORT}"
}

# Function to setup database and load TPC-H data
setup_database() {
    echo -e "\n${YELLOW}Setting up database and loading TPC-H data...${NC}"
    
    # Get database credentials from Secrets Manager
    SECRET_ARN=$(aws secretsmanager list-secrets \
        --filters Key=name,Values=${DEMO_NAME}-aurora-credentials \
        --query "SecretList[0].ARN" \
        --output text \
        --region ${AWS_REGION})
    
    DB_CREDENTIALS=$(aws secretsmanager get-secret-value \
        --secret-id ${SECRET_ARN} \
        --query SecretString \
        --output text \
        --region ${AWS_REGION})
    
    DB_USERNAME=$(echo ${DB_CREDENTIALS} | jq -r '.username')
    DB_PASSWORD=$(echo ${DB_CREDENTIALS} | jq -r '.password')
    
    # Run database setup script
    python3 ../scripts/setup_tpch_database.py \
        --host ${AURORA_ENDPOINT} \
        --port ${AURORA_PORT} \
        --username ${DB_USERNAME} \
        --password ${DB_PASSWORD} \
        --scale ${TPCH_SCALE}
    
    echo -e "${GREEN}✓ Database setup completed${NC}"
}

# Function to start CDC data generator
start_cdc_generator() {
    echo -e "\n${YELLOW}Starting CDC data generator...${NC}"
    
    # Install Python dependencies
    pip3 install -r ../requirements/demo.txt
    
    # Start the generator in background
    nohup python3 ../scripts/cdc_data_generator.py \
        --host ${AURORA_ENDPOINT} \
        --port ${AURORA_PORT} \
        --username ${DB_USERNAME} \
        --password ${DB_PASSWORD} \
        --interval 5 \
        --batch-size 100 \
        > cdc_generator.log 2>&1 &
    
    CDC_PID=$!
    echo ${CDC_PID} > cdc_generator.pid
    
    echo -e "${GREEN}✓ CDC data generator started (PID: ${CDC_PID})${NC}"
}

# Function to setup Databricks connection
setup_databricks_connection() {
    echo -e "\n${YELLOW}Setting up Databricks connection...${NC}"
    
    # Create connection configuration file
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
    
    # Generate Databricks notebook
    python3 ../scripts/generate_databricks_notebook.py \
        --config config/databricks_connection.json \
        --output ../notebooks/demo/demo_cdc_setup.py
    
    echo -e "${GREEN}✓ Databricks connection configured${NC}"
    echo -e "${YELLOW}Import the generated notebook to your Databricks workspace:${NC}"
    echo -e "  ../notebooks/demo/demo_cdc_setup.py"
}

# Function to run tests
run_tests() {
    echo -e "\n${YELLOW}Running validation tests...${NC}"
    
    python3 ../tests/demo_validation.py \
        --host ${AURORA_ENDPOINT} \
        --port ${AURORA_PORT} \
        --username ${DB_USERNAME} \
        --password ${DB_PASSWORD}
    
    echo -e "${GREEN}✓ All tests passed${NC}"
}

# Function to display summary
display_summary() {
    echo -e "\n${GREEN}=================================================${NC}"
    echo -e "${GREEN}          Demo Setup Completed!                  ${NC}"
    echo -e "${GREEN}=================================================${NC}"
    
    echo -e "\n${YELLOW}Aurora MySQL Details:${NC}"
    echo -e "  Endpoint: ${AURORA_ENDPOINT}"
    echo -e "  Port: ${AURORA_PORT}"
    echo -e "  Database: tpch"
    echo -e "  Username: ${DB_USERNAME}"
    
    echo -e "\n${YELLOW}CDC Data Generator:${NC}"
    echo -e "  Status: Running (PID: ${CDC_PID})"
    echo -e "  Log file: cdc_generator.log"
    
    echo -e "\n${YELLOW}Next Steps:${NC}"
    echo -e "  1. Import the notebook to Databricks: ../notebooks/demo/demo_cdc_setup.py"
    echo -e "  2. Run the notebook to start CDC streaming"
    echo -e "  3. Monitor the streaming dashboard"
    
    echo -e "\n${YELLOW}Useful Commands:${NC}"
    echo -e "  Stop CDC generator: kill \$(cat cdc_generator.pid)"
    echo -e "  View CDC logs: tail -f cdc_generator.log"
    echo -e "  Connect to Aurora: mysql -h ${AURORA_ENDPOINT} -P ${AURORA_PORT} -u ${DB_USERNAME} -p"
    echo -e "  Cleanup: ./cleanup_demo.sh"
}

# Main execution
main() {
    # Create necessary directories
    mkdir -p config logs
    
    # Check prerequisites
    check_prerequisites
    
    # Get Databricks VPC info
    get_databricks_vpc_info
    
    # Deploy infrastructure
    deploy_infrastructure
    
    # Setup database
    setup_database
    
    # Start CDC generator
    start_cdc_generator
    
    # Setup Databricks connection
    setup_databricks_connection
    
    # Run tests
    run_tests
    
    # Display summary
    display_summary
}

# Run main function
main