#!/bin/bash

set -e

ENVIRONMENT=${1:-dev}
STACK_NAME="aurora-cdc-demo-${ENVIRONMENT}"
REGION=${AWS_REGION:-us-east-1}

echo "Deploying infrastructure for environment: ${ENVIRONMENT}"
echo "Stack name: ${STACK_NAME}"
echo "Region: ${REGION}"

# Deploy VPC stack
echo "Deploying VPC stack..."
aws cloudformation deploy \
  --template-file ../cloudformation/vpc-networking.yaml \
  --stack-name "${STACK_NAME}-vpc" \
  --parameter-overrides EnvironmentName="${ENVIRONMENT}" \
  --region "${REGION}" \
  --capabilities CAPABILITY_IAM

# Get VPC outputs
VPC_ID=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}-vpc" \
  --query "Stacks[0].Outputs[?OutputKey=='VPCId'].OutputValue" \
  --output text \
  --region "${REGION}")

DATABASE_SUBNETS=$(aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}-vpc" \
  --query "Stacks[0].Outputs[?OutputKey=='DatabaseSubnets'].OutputValue" \
  --output text \
  --region "${REGION}")

# Deploy Aurora stack
echo "Deploying Aurora stack..."
aws cloudformation deploy \
  --template-file ../cloudformation/aurora-cluster.yaml \
  --stack-name "${STACK_NAME}-aurora" \
  --parameter-overrides \
    file://../cloudformation/parameters/${ENVIRONMENT}.json \
    VPCId="${VPC_ID}" \
    SubnetIds="${DATABASE_SUBNETS}" \
  --region "${REGION}" \
  --capabilities CAPABILITY_IAM

echo "Infrastructure deployment completed successfully!"

# Display outputs
echo "Aurora Cluster Endpoint:"
aws cloudformation describe-stacks \
  --stack-name "${STACK_NAME}-aurora" \
  --query "Stacks[0].Outputs[?OutputKey=='ClusterEndpoint'].OutputValue" \
  --output text \
  --region "${REGION}"