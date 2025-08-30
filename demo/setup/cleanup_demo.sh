#!/bin/bash

# Aurora CDC Demo - Cleanup Script
# Removes all demo resources from AWS

set -e

# Color codes
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Configuration
DEMO_NAME="${DEMO_NAME:-aurora-cdc-demo}"
AWS_REGION="${AWS_REGION:-us-east-1}"

echo -e "${YELLOW}=================================================${NC}"
echo -e "${YELLOW}         Aurora CDC Demo Cleanup                 ${NC}"
echo -e "${YELLOW}=================================================${NC}"

# Confirm deletion
echo -e "\n${RED}WARNING: This will delete all demo resources!${NC}"
echo -e "The following will be removed:"
echo -e "  - Aurora MySQL cluster and instances"
echo -e "  - VPC and networking resources"
echo -e "  - VPC peering connections"
echo -e "  - Secrets in Secrets Manager"
echo -e "  - CloudFormation stacks"
echo -e "  - All data will be permanently deleted!"
echo ""
read -p "Are you sure you want to continue? Type 'yes' to confirm: " confirmation

if [ "$confirmation" != "yes" ]; then
    echo -e "${YELLOW}Cleanup cancelled${NC}"
    exit 0
fi

# Function to stop CDC generator
stop_cdc_generator() {
    echo -e "\n${YELLOW}Stopping CDC data generator...${NC}"
    
    if [ -f "cdc_generator.pid" ]; then
        PID=$(cat cdc_generator.pid)
        if ps -p $PID > /dev/null; then
            kill $PID
            echo -e "${GREEN}✓ CDC generator stopped${NC}"
        else
            echo "CDC generator not running"
        fi
        rm -f cdc_generator.pid
    else
        echo "No CDC generator PID file found"
    fi
}

# Function to delete CloudFormation stacks
delete_stacks() {
    echo -e "\n${YELLOW}Deleting CloudFormation stacks...${NC}"
    
    # Delete Aurora stack first (depends on VPC)
    AURORA_STACK="${DEMO_NAME}-aurora"
    if aws cloudformation describe-stacks --stack-name ${AURORA_STACK} --region ${AWS_REGION} &>/dev/null; then
        echo "Deleting Aurora stack..."
        aws cloudformation delete-stack \
            --stack-name ${AURORA_STACK} \
            --region ${AWS_REGION}
        
        echo "Waiting for Aurora stack deletion..."
        aws cloudformation wait stack-delete-complete \
            --stack-name ${AURORA_STACK} \
            --region ${AWS_REGION} || true
        
        echo -e "${GREEN}✓ Aurora stack deleted${NC}"
    else
        echo "Aurora stack not found"
    fi
    
    # Delete VPC stack
    VPC_STACK="${DEMO_NAME}-vpc"
    if aws cloudformation describe-stacks --stack-name ${VPC_STACK} --region ${AWS_REGION} &>/dev/null; then
        echo "Deleting VPC stack..."
        aws cloudformation delete-stack \
            --stack-name ${VPC_STACK} \
            --region ${AWS_REGION}
        
        echo "Waiting for VPC stack deletion..."
        aws cloudformation wait stack-delete-complete \
            --stack-name ${VPC_STACK} \
            --region ${AWS_REGION} || true
        
        echo -e "${GREEN}✓ VPC stack deleted${NC}"
    else
        echo "VPC stack not found"
    fi
}

# Function to clean up secrets
cleanup_secrets() {
    echo -e "\n${YELLOW}Cleaning up Secrets Manager...${NC}"
    
    SECRET_NAME="${DEMO_NAME}-aurora-credentials"
    
    # Check if secret exists
    if aws secretsmanager describe-secret --secret-id ${SECRET_NAME} --region ${AWS_REGION} &>/dev/null; then
        # Delete secret immediately (skip recovery window)
        aws secretsmanager delete-secret \
            --secret-id ${SECRET_NAME} \
            --force-delete-without-recovery \
            --region ${AWS_REGION}
        
        echo -e "${GREEN}✓ Secret deleted${NC}"
    else
        echo "Secret not found"
    fi
}

# Function to clean up S3 buckets
cleanup_s3() {
    echo -e "\n${YELLOW}Cleaning up S3 buckets...${NC}"
    
    # Find and delete CloudFormation template buckets
    BUCKETS=$(aws s3 ls --region ${AWS_REGION} | grep "${DEMO_NAME}-cf-templates" | awk '{print $3}')
    
    for bucket in $BUCKETS; do
        echo "Deleting bucket: $bucket"
        
        # Delete all objects first
        aws s3 rm s3://${bucket} --recursive --region ${AWS_REGION}
        
        # Delete bucket
        aws s3 rb s3://${bucket} --region ${AWS_REGION}
        
        echo -e "${GREEN}✓ Bucket $bucket deleted${NC}"
    done
}

# Function to clean up VPC peering
cleanup_peering() {
    echo -e "\n${YELLOW}Cleaning up VPC peering connections...${NC}"
    
    # Find peering connections with our demo tag
    PEERING_IDS=$(aws ec2 describe-vpc-peering-connections \
        --filters "Name=tag:Name,Values=${DEMO_NAME}-databricks-peering" \
        --query "VpcPeeringConnections[].VpcPeeringConnectionId" \
        --output text \
        --region ${AWS_REGION})
    
    if [ -n "$PEERING_IDS" ]; then
        for peering_id in $PEERING_IDS; do
            echo "Deleting peering connection: $peering_id"
            aws ec2 delete-vpc-peering-connection \
                --vpc-peering-connection-id ${peering_id} \
                --region ${AWS_REGION}
        done
        echo -e "${GREEN}✓ Peering connections deleted${NC}"
    else
        echo "No peering connections found"
    fi
}

# Function to clean up local files
cleanup_local() {
    echo -e "\n${YELLOW}Cleaning up local files...${NC}"
    
    # Remove configuration files
    rm -f config/databricks_vpc.json
    rm -f config/databricks_connection.json
    rm -f cdc_generator.log
    rm -f cdc_generator.pid
    
    # Remove generated notebooks
    rm -f ../notebooks/demo/demo_cdc_setup.py
    
    echo -e "${GREEN}✓ Local files cleaned${NC}"
}

# Function to display cleanup summary
display_summary() {
    echo -e "\n${GREEN}=================================================${NC}"
    echo -e "${GREEN}         Cleanup Completed Successfully          ${NC}"
    echo -e "${GREEN}=================================================${NC}"
    
    echo -e "\n${YELLOW}Resources removed:${NC}"
    echo -e "  ✓ Aurora MySQL cluster"
    echo -e "  ✓ VPC and networking"
    echo -e "  ✓ VPC peering connections"
    echo -e "  ✓ Secrets Manager secrets"
    echo -e "  ✓ S3 buckets"
    echo -e "  ✓ CloudFormation stacks"
    echo -e "  ✓ Local configuration files"
    
    echo -e "\n${YELLOW}Note:${NC}"
    echo -e "  - Delta tables in Databricks Unity Catalog were NOT deleted"
    echo -e "  - You may want to manually clean those up if needed"
    echo -e "  - Check AWS Cost Explorer for any remaining charges"
}

# Main cleanup process
main() {
    echo -e "\n${YELLOW}Starting cleanup process...${NC}"
    
    # Stop CDC generator
    stop_cdc_generator
    
    # Delete CloudFormation stacks
    delete_stacks
    
    # Clean up secrets
    cleanup_secrets
    
    # Clean up S3
    cleanup_s3
    
    # Clean up VPC peering
    cleanup_peering
    
    # Clean up local files
    cleanup_local
    
    # Display summary
    display_summary
}

# Run cleanup
main