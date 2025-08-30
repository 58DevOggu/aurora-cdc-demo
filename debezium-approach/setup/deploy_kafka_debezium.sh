#!/bin/bash

# Deploy Kafka and Debezium for Aurora CDC
# Can use either local Docker or AWS MSK

set -e

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Configuration
DEPLOYMENT_TYPE="${1:-docker}"  # docker or msk
ENVIRONMENT="${ENVIRONMENT:-demo}"
AWS_REGION="${AWS_REGION:-us-east-1}"

echo -e "${GREEN}=================================================${NC}"
echo -e "${GREEN}     Kafka & Debezium Deployment for CDC        ${NC}"
echo -e "${GREEN}=================================================${NC}"

# Function to deploy with Docker
deploy_docker() {
    echo -e "\n${YELLOW}Deploying Kafka and Debezium using Docker...${NC}"
    
    # Check Docker
    if ! command -v docker &> /dev/null; then
        echo -e "${RED}Docker is not installed${NC}"
        exit 1
    fi
    
    if ! command -v docker-compose &> /dev/null; then
        echo -e "${RED}docker-compose is not installed${NC}"
        exit 1
    fi
    
    # Start services
    cd ../docker
    
    echo -e "${YELLOW}Starting Docker containers...${NC}"
    docker-compose up -d
    
    # Wait for services to be ready
    echo -e "${YELLOW}Waiting for services to be ready...${NC}"
    
    # Wait for Kafka
    echo -n "Waiting for Kafka..."
    while ! docker exec kafka kafka-broker-api-versions --bootstrap-server localhost:9092 &>/dev/null; do
        echo -n "."
        sleep 2
    done
    echo -e " ${GREEN}Ready${NC}"
    
    # Wait for Schema Registry
    echo -n "Waiting for Schema Registry..."
    while ! curl -s http://localhost:8081/subjects &>/dev/null; do
        echo -n "."
        sleep 2
    done
    echo -e " ${GREEN}Ready${NC}"
    
    # Wait for Kafka Connect
    echo -n "Waiting for Kafka Connect..."
    while ! curl -s http://localhost:8083/connectors &>/dev/null; do
        echo -n "."
        sleep 2
    done
    echo -e " ${GREEN}Ready${NC}"
    
    # Create topics for CDC
    echo -e "\n${YELLOW}Creating Kafka topics...${NC}"
    
    docker exec kafka kafka-topics --create \
        --topic cdc.tpch.customer \
        --bootstrap-server localhost:9092 \
        --partitions 3 \
        --replication-factor 1 \
        --if-not-exists
    
    docker exec kafka kafka-topics --create \
        --topic cdc.tpch.orders \
        --bootstrap-server localhost:9092 \
        --partitions 6 \
        --replication-factor 1 \
        --if-not-exists
    
    docker exec kafka kafka-topics --create \
        --topic cdc.tpch.lineitem \
        --bootstrap-server localhost:9092 \
        --partitions 12 \
        --replication-factor 1 \
        --if-not-exists
    
    # Create schema changes topic
    docker exec kafka kafka-topics --create \
        --topic schema-changes.aurora-cdc \
        --bootstrap-server localhost:9092 \
        --partitions 1 \
        --replication-factor 1 \
        --config cleanup.policy=compact \
        --if-not-exists
    
    echo -e "${GREEN}✓ Docker deployment completed${NC}"
    
    # Display endpoints
    echo -e "\n${YELLOW}Service Endpoints:${NC}"
    echo "  Kafka Bootstrap: localhost:9092"
    echo "  Schema Registry: http://localhost:8081"
    echo "  Kafka Connect: http://localhost:8083"
    echo "  Kafka UI: http://localhost:8080"
    echo "  Debezium UI: http://localhost:8090"
}

# Function to deploy with MSK
deploy_msk() {
    echo -e "\n${YELLOW}Deploying Kafka using AWS MSK...${NC}"
    
    # Check AWS CLI
    if ! command -v aws &> /dev/null; then
        echo -e "${RED}AWS CLI is not installed${NC}"
        exit 1
    fi
    
    # CloudFormation template for MSK
    cat > msk-cluster.yaml <<EOF
AWSTemplateFormatVersion: '2010-09-09'
Description: 'MSK Cluster for Aurora CDC with Debezium'

Parameters:
  EnvironmentName:
    Type: String
    Default: ${ENVIRONMENT}
  
  VpcId:
    Type: String
    Description: VPC ID for MSK cluster
  
  SubnetIds:
    Type: CommaDelimitedList
    Description: Subnet IDs for MSK cluster

Resources:
  MSKSecurityGroup:
    Type: AWS::EC2::SecurityGroup
    Properties:
      GroupDescription: Security group for MSK cluster
      VpcId: !Ref VpcId
      SecurityGroupIngress:
        - IpProtocol: tcp
          FromPort: 9092
          ToPort: 9092
          CidrIp: 10.0.0.0/8
        - IpProtocol: tcp
          FromPort: 2181
          ToPort: 2181
          CidrIp: 10.0.0.0/8
        - IpProtocol: tcp
          FromPort: 9094
          ToPort: 9094
          CidrIp: 10.0.0.0/8

  MSKCluster:
    Type: AWS::MSK::Cluster
    Properties:
      ClusterName: !Sub '\${EnvironmentName}-aurora-cdc-msk'
      KafkaVersion: '3.5.1'
      NumberOfBrokerNodes: 3
      BrokerNodeGroupInfo:
        InstanceType: kafka.m5.large
        ClientSubnets: !Ref SubnetIds
        SecurityGroups:
          - !Ref MSKSecurityGroup
        StorageInfo:
          EBSStorageInfo:
            VolumeSize: 100
      ClientAuthentication:
        Sasl:
          Iam:
            Enabled: true
      EncryptionInfo:
        EncryptionInTransit:
          ClientBroker: TLS
          InCluster: true
      EnhancedMonitoring: PER_TOPIC_PER_BROKER
      LoggingInfo:
        BrokerLogs:
          CloudWatchLogs:
            Enabled: true
            LogGroup: !Ref MSKLogGroup

  MSKLogGroup:
    Type: AWS::Logs::LogGroup
    Properties:
      LogGroupName: !Sub '/aws/msk/\${EnvironmentName}-aurora-cdc'
      RetentionInDays: 7

Outputs:
  MSKClusterArn:
    Description: MSK Cluster ARN
    Value: !Ref MSKCluster
  
  BootstrapServers:
    Description: MSK Bootstrap servers
    Value: !GetAtt MSKCluster.BootstrapBrokerStringTls
EOF
    
    # Deploy MSK cluster
    echo -e "${YELLOW}Creating MSK cluster...${NC}"
    
    # Get VPC and Subnet information
    read -p "Enter VPC ID for MSK: " VPC_ID
    read -p "Enter comma-separated Subnet IDs: " SUBNET_IDS
    
    aws cloudformation deploy \
        --template-file msk-cluster.yaml \
        --stack-name ${ENVIRONMENT}-msk-cdc \
        --parameter-overrides \
            EnvironmentName=${ENVIRONMENT} \
            VpcId=${VPC_ID} \
            SubnetIds=${SUBNET_IDS} \
        --capabilities CAPABILITY_IAM \
        --region ${AWS_REGION}
    
    # Get MSK endpoints
    BOOTSTRAP_SERVERS=$(aws cloudformation describe-stacks \
        --stack-name ${ENVIRONMENT}-msk-cdc \
        --query "Stacks[0].Outputs[?OutputKey=='BootstrapServers'].OutputValue" \
        --output text \
        --region ${AWS_REGION})
    
    echo -e "${GREEN}✓ MSK cluster created${NC}"
    echo -e "Bootstrap servers: ${BOOTSTRAP_SERVERS}"
    
    # Deploy Kafka Connect on ECS/EKS (simplified)
    echo -e "\n${YELLOW}Note: Deploy Kafka Connect with Debezium on ECS/EKS${NC}"
    echo "Use the provided ECS task definition or Kubernetes manifests"
}

# Function to verify deployment
verify_deployment() {
    echo -e "\n${YELLOW}Verifying deployment...${NC}"
    
    if [ "$DEPLOYMENT_TYPE" == "docker" ]; then
        # Check Docker containers
        echo -e "\n${YELLOW}Container Status:${NC}"
        docker-compose ps
        
        # Test Kafka
        echo -e "\n${YELLOW}Testing Kafka...${NC}"
        docker exec kafka kafka-topics --list --bootstrap-server localhost:9092
        
        # Test Schema Registry
        echo -e "\n${YELLOW}Testing Schema Registry...${NC}"
        curl -s http://localhost:8081/subjects | jq '.'
        
        # Test Kafka Connect
        echo -e "\n${YELLOW}Testing Kafka Connect...${NC}"
        curl -s http://localhost:8083/connector-plugins | jq '.[].class' | grep -i debezium
        
    else
        echo "Verify MSK cluster in AWS Console"
        echo "Cluster: ${ENVIRONMENT}-msk-cdc"
    fi
    
    echo -e "\n${GREEN}✓ Deployment verification completed${NC}"
}

# Function to display next steps
show_next_steps() {
    echo -e "\n${GREEN}=================================================${NC}"
    echo -e "${GREEN}        Kafka & Debezium Ready for CDC          ${NC}"
    echo -e "${GREEN}=================================================${NC}"
    
    echo -e "\n${YELLOW}Next Steps:${NC}"
    echo "1. Configure Aurora connector:"
    echo "   ./configure_aurora_connector.sh"
    echo ""
    echo "2. Create DLT pipeline in Databricks:"
    echo "   ./create_dlt_pipeline.sh"
    echo ""
    echo "3. Monitor CDC flow:"
    if [ "$DEPLOYMENT_TYPE" == "docker" ]; then
        echo "   Kafka UI: http://localhost:8080"
        echo "   Debezium UI: http://localhost:8090"
    else
        echo "   CloudWatch: AWS MSK Metrics"
    fi
    echo ""
    echo "4. Run tests:"
    echo "   cd ../tests && python test_debezium_integration.py"
}

# Main execution
main() {
    case "$DEPLOYMENT_TYPE" in
        docker)
            deploy_docker
            ;;
        msk)
            deploy_msk
            ;;
        *)
            echo -e "${RED}Invalid deployment type. Use 'docker' or 'msk'${NC}"
            exit 1
            ;;
    esac
    
    # Verify deployment
    verify_deployment
    
    # Show next steps
    show_next_steps
}

# Run main
main