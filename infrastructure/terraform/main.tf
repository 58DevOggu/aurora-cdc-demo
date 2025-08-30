terraform {
  required_version = ">= 1.0"
  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 5.0"
    }
  }
}

provider "aws" {
  region = var.aws_region
}

module "vpc" {
  source = "./modules/vpc"
  
  environment_name = var.environment_name
  vpc_cidr        = var.vpc_cidr
  azs             = var.availability_zones
}

module "aurora" {
  source = "./modules/aurora"
  
  environment_name    = var.environment_name
  vpc_id             = module.vpc.vpc_id
  subnet_ids         = module.vpc.database_subnet_ids
  master_username    = var.db_master_username
  master_password    = var.db_master_password
  instance_class     = var.db_instance_class
  backup_retention   = var.backup_retention_period
}

module "monitoring" {
  source = "./modules/monitoring"
  
  environment_name = var.environment_name
  cluster_id      = module.aurora.cluster_id
  alarm_email     = var.alarm_email
}