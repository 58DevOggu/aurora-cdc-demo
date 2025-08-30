# Aurora CDC Demo

A comprehensive demonstration of Change Data Capture (CDC) from AWS Aurora MySQL to Databricks using custom DataSource V2 API implementation.

## Overview

This project implements a custom Spark DataSource for streaming CDC events from Aurora MySQL databases to Databricks, enabling real-time data synchronization and analytics.

## Features

- **Custom DataSource V2 Implementation**: Native Spark integration for optimal performance
- **Real-time CDC Streaming**: Capture and process database changes in real-time
- **Aurora MySQL Optimization**: Configured for efficient binlog-based CDC
- **Production-Ready**: Includes monitoring, alerting, and performance tuning
- **Comprehensive Testing**: Unit, integration, and performance test suites
- **Infrastructure as Code**: CloudFormation and Terraform templates included

## Architecture

```
Aurora MySQL → Binlog → Custom DataSource → Spark Structured Streaming → Delta Lake
```

## Quick Start

### Prerequisites

- Python 3.8+
- AWS Account with Aurora MySQL
- Databricks Workspace
- Docker (for local development)

### Installation

1. Clone the repository:
```bash
git clone https://github.com/yourusername/aurora-cdc-demo.git
cd aurora-cdc-demo
```

2. Create virtual environment:
```bash
python -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements/development.txt
```

4. Set up environment variables:
```bash
cp .env.example .env
# Edit .env with your configuration
```

### Deploy Infrastructure

Using CloudFormation:
```bash
cd infrastructure/scripts
./deploy-infrastructure.sh dev
```

Using Terraform:
```bash
cd infrastructure/terraform
terraform init
terraform plan
terraform apply
```

### Run the Demo

1. Set up the database:
```bash
./infrastructure/scripts/setup-database.sh <host> <user> <password>
```

2. Generate sample data:
```bash
python scripts/data/generate_sample_data.py
```

3. Start CDC streaming:
```bash
python src/notebooks/demo/02_basic_cdc_streaming.py
```

## Project Structure

```
aurora-cdc-demo/
├── src/aurora_cdc/         # Main package
│   ├── datasource/         # DataSource implementation
│   ├── config/            # Configuration management
│   ├── monitoring/        # Monitoring and metrics
│   └── utils/            # Utilities
├── infrastructure/        # AWS infrastructure
├── tests/                # Test suites
├── docs/                 # Documentation
└── examples/             # Usage examples
```

## Configuration

Edit `configs/development.yaml`:

```yaml
aurora:
  host: your-aurora-cluster.cluster-xxx.us-east-1.rds.amazonaws.com
  port: 3306
  database: aurora_cdc_demo
  username: admin
  password: ${AURORA_PASSWORD}

databricks:
  workspace_url: https://your-workspace.cloud.databricks.com
  token: ${DATABRICKS_TOKEN}
  
streaming:
  checkpoint_location: /tmp/aurora-cdc-checkpoint
  trigger_interval: 10 seconds
  max_files_per_trigger: 100
```

## Testing

Run all tests:
```bash
pytest tests/
```

Run specific test suite:
```bash
pytest tests/unit/
pytest tests/integration/
pytest tests/performance/
```

## Performance Tuning

See [PERFORMANCE_TUNING.md](docs/PERFORMANCE_TUNING.md) for detailed performance optimization guidelines.

## Monitoring

The project includes comprehensive monitoring:
- CloudWatch metrics and dashboards
- Custom application metrics
- Performance benchmarking tools

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Support

For issues and questions:
- GitHub Issues: [aurora-cdc-demo/issues](https://github.com/yourusername/aurora-cdc-demo/issues)
- Documentation: [docs/](docs/)

## Acknowledgments

- Apache Spark team for the DataSource V2 API
- AWS Aurora team for CDC capabilities
- Databricks for the unified analytics platform