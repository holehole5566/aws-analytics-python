# Kafka Connector Configurations

This directory contains example configurations for Kafka Connect connectors.

## Available Connectors

### MongoDB Connector (`mongo_connector.config`)
Configuration for connecting Kafka to MongoDB for data synchronization.

### PostgreSQL Connector (`postgre_connector.config`)  
Configuration for connecting Kafka to PostgreSQL databases.

## Usage

1. Copy the desired configuration file
2. Modify the connection parameters for your environment
3. Deploy using Kafka Connect REST API or your connector management tool

## Configuration Parameters

Each connector config includes:
- Connection details (host, port, credentials)
- Topic mapping
- Data transformation settings
- Error handling configuration

Refer to the specific connector documentation for detailed parameter descriptions.