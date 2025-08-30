#!/bin/bash

set -e

ENVIRONMENT=${1:-dev}
STACK_NAME="aurora-cdc-demo-${ENVIRONMENT}"
REGION=${AWS_REGION:-us-east-1}

echo "Cleaning up infrastructure for environment: ${ENVIRONMENT}"
echo "Stack name: ${STACK_NAME}"
echo "Region: ${REGION}"

# Confirm deletion
read -p "Are you sure you want to delete all resources? (y/N) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]
then
    echo "Cleanup cancelled"
    exit 1
fi

# Delete Aurora stack
echo "Deleting Aurora stack..."
aws cloudformation delete-stack \
  --stack-name "${STACK_NAME}-aurora" \
  --region "${REGION}"

echo "Waiting for Aurora stack deletion..."
aws cloudformation wait stack-delete-complete \
  --stack-name "${STACK_NAME}-aurora" \
  --region "${REGION}" || true

# Delete VPC stack
echo "Deleting VPC stack..."
aws cloudformation delete-stack \
  --stack-name "${STACK_NAME}-vpc" \
  --region "${REGION}"

echo "Waiting for VPC stack deletion..."
aws cloudformation wait stack-delete-complete \
  --stack-name "${STACK_NAME}-vpc" \
  --region "${REGION}" || true

echo "Cleanup completed successfully!"