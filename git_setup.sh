#!/bin/bash

# Git Setup Script for Aurora CDC Demo
# This script initializes git and prepares the project for pushing to GitHub

set -e

# Color codes
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=================================================${NC}"
echo -e "${BLUE}    Git Setup for Aurora CDC Demo Project       ${NC}"
echo -e "${BLUE}=================================================${NC}"

# Function to check if we're in the right directory
check_directory() {
    if [ ! -f "README.md" ] || [ ! -d "src/aurora_cdc" ]; then
        echo -e "${YELLOW}Warning: Not in aurora-cdc-demo directory${NC}"
        echo "Please run this script from the aurora-cdc-demo directory"
        exit 1
    fi
}

# Function to initialize git
init_git() {
    echo -e "\n${YELLOW}Initializing Git repository...${NC}"
    
    if [ -d ".git" ]; then
        echo "Git repository already initialized"
    else
        git init
        echo -e "${GREEN}✓ Git repository initialized${NC}"
    fi
}

# Function to create comprehensive .gitignore
create_gitignore() {
    echo -e "\n${YELLOW}Creating .gitignore file...${NC}"
    
    # .gitignore is already created, but let's ensure it has all necessary entries
    cat > .gitignore <<'EOF'
# Byte-compiled / optimized / DLL files
__pycache__/
*.py[cod]
*$py.class

# C extensions
*.so

# Distribution / packaging
.Python
build/
develop-eggs/
dist/
downloads/
eggs/
.eggs/
lib/
lib64/
parts/
sdist/
var/
wheels/
share/python-wheels/
*.egg-info/
.installed.cfg
*.egg
MANIFEST

# PyInstaller
*.manifest
*.spec

# Installer logs
pip-log.txt
pip-delete-this-directory.txt

# Unit test / coverage reports
htmlcov/
.tox/
.nox/
.coverage
.coverage.*
.cache
nosetests.xml
coverage.xml
*.cover
*.py,cover
.hypothesis/
.pytest_cache/
cover/

# Virtual environments
.env
.venv
env/
venv/
ENV/
env.bak/
venv.bak/

# IDEs
.idea/
.vscode/
*.swp
*.swo
*~
.DS_Store

# Jupyter Notebook
.ipynb_checkpoints

# IPython
profile_default/
ipython_config.py

# mypy
.mypy_cache/
.dmypy.json
dmypy.json

# Pyre type checker
.pyre/

# pytype static type analyzer
.pytype/

# Terraform
*.tfstate
*.tfstate.*
.terraform/
.terraform.lock.hcl
terraform.tfvars

# AWS
.aws-sam/

# Spark
spark-warehouse/
metastore_db/
derby.log

# Databricks
.databricks/

# Application specific
logs/
data/
checkpoint/
tmp/
*.parquet
*.csv
*.json.gz

# Demo specific
demo/setup/config/
demo/setup/cdc_generator.pid
demo/setup/cdc_generator.log
demo/setup/logs/

# Credentials - NEVER commit these
*.pem
*.key
*.crt
credentials.json
service-account.json
*_credentials.json
*_secrets.yaml
*_secrets.json

# Local configuration
config/local/
config/databricks_vpc.json
config/databricks_connection.json

# Temporary files
*.tmp
*.bak
*.backup
*.old
EOF
    
    echo -e "${GREEN}✓ .gitignore created/updated${NC}"
}

# Function to add all files
add_files() {
    echo -e "\n${YELLOW}Adding files to Git...${NC}"
    
    # Add all files respecting .gitignore
    git add .
    
    # Show status
    echo -e "\n${YELLOW}Git status:${NC}"
    git status --short
    
    # Count files
    file_count=$(git ls-files | wc -l)
    echo -e "\n${GREEN}✓ Added ${file_count} files to Git${NC}"
}

# Function to create initial commit
create_commit() {
    echo -e "\n${YELLOW}Creating initial commit...${NC}"
    
    # Check if there are changes to commit
    if git diff --cached --quiet; then
        echo "No changes to commit"
    else
        git commit -m "Initial commit: Aurora CDC Demo for Databricks

- Complete CDC solution for 500+ tables from Aurora MySQL to Databricks
- Custom PySpark DataSource V2 with Spark 4 features
- Unity Catalog integration with Delta Lake
- TPC-H test data with continuous CDC generator
- Cross-VPC networking setup
- Comprehensive test suite
- One-click demo deployment
- Real-time streaming with checkpointing"
        
        echo -e "${GREEN}✓ Initial commit created${NC}"
    fi
}

# Function to create README for GitHub
create_github_readme() {
    echo -e "\n${YELLOW}Updating main README for GitHub...${NC}"
    
    # The main README already exists, but let's add a quick start section at the top
    if [ -f "README.md" ]; then
        echo -e "${GREEN}✓ README.md already exists${NC}"
    fi
}

# Function to display next steps
show_next_steps() {
    echo -e "\n${BLUE}=================================================${NC}"
    echo -e "${BLUE}              Git Setup Complete!                ${NC}"
    echo -e "${BLUE}=================================================${NC}"
    
    echo -e "\n${YELLOW}Next steps to push to GitHub:${NC}"
    echo ""
    echo "1. Create a new repository on GitHub:"
    echo "   - Go to https://github.com/new"
    echo "   - Name: aurora-cdc-demo"
    echo "   - Description: Enterprise-scale CDC from Aurora MySQL to Databricks using custom PySpark DataSource"
    echo "   - Keep it public or private as needed"
    echo "   - DO NOT initialize with README, .gitignore, or license"
    echo ""
    echo "2. Add GitHub remote and push:"
    echo -e "${GREEN}   git remote add origin https://github.com/YOUR_USERNAME/aurora-cdc-demo.git${NC}"
    echo -e "${GREEN}   git branch -M main${NC}"
    echo -e "${GREEN}   git push -u origin main${NC}"
    echo ""
    echo "3. (Optional) Add additional remotes:"
    echo -e "${GREEN}   git remote add upstream https://github.com/databricks/aurora-cdc-demo.git${NC}"
    echo ""
    echo -e "${YELLOW}Repository Statistics:${NC}"
    echo "   Files: $(git ls-files | wc -l)"
    echo "   Directories: $(find . -type d -not -path './.git*' | wc -l)"
    echo "   Python files: $(find . -name "*.py" -not -path './.git*' | wc -l)"
    echo "   Shell scripts: $(find . -name "*.sh" -not -path './.git*' | wc -l)"
    echo ""
    echo -e "${YELLOW}Key Features Ready:${NC}"
    echo "   ✓ Custom PySpark DataSource V2"
    echo "   ✓ 500+ table CDC support"
    echo "   ✓ Unity Catalog integration"
    echo "   ✓ Cross-VPC networking"
    echo "   ✓ TPC-H data generator"
    echo "   ✓ Continuous CDC simulator"
    echo "   ✓ Comprehensive test suite"
    echo "   ✓ One-click demo setup"
}

# Function to create a branch for development
create_dev_branch() {
    echo -e "\n${YELLOW}Creating development branch...${NC}"
    
    git branch develop
    echo -e "${GREEN}✓ Created 'develop' branch${NC}"
    echo "   To switch to develop branch: git checkout develop"
}

# Main execution
main() {
    echo -e "\n${YELLOW}Starting Git setup...${NC}"
    
    # Check we're in the right directory
    check_directory
    
    # Initialize git
    init_git
    
    # Create/update .gitignore
    create_gitignore
    
    # Add files
    add_files
    
    # Create initial commit
    create_commit
    
    # Create development branch
    create_dev_branch
    
    # Update README
    create_github_readme
    
    # Show next steps
    show_next_steps
}

# Run main function
main