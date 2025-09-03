# AWS Analytics Scripts

A collection of Python scripts for AWS analytics services including MSK, OpenSearch, Kinesis, and more.

## Project Structure

```
├── src/aws_analytics/          # Main package
│   ├── config/                 # Configuration management
│   ├── services/               # Service classes
│   └── utils/                  # Utility functions
├── scripts/                    # Executable scripts
├── msk/                        # Legacy MSK scripts
├── opensearch/                 # Legacy OpenSearch scripts
├── kinesis/                    # Legacy Kinesis scripts
└── .env                        # Environment variables
```

## Setup

1. Install dependencies:
   ```bash
   pip install -e .
   ```

2. Configure environment variables in `.env`:
   ```
   MSK_BOOTSTRAP_SERVERS=your-msk-endpoint:9092
   MSK_TOPIC=your-topic
   OPENSEARCH_ENDPOINT=your-opensearch-endpoint
   HTTP_URL=your-http-endpoint
   ```

## Usage

### MSK Load Testing
```bash
python scripts/msk_load_test.py
```

### HTTP Requests with AWS Auth
```bash
python scripts/http_request.py
```

### Using Services Programmatically
```python
from aws_analytics.services import MSKService
from aws_analytics.config import Settings

msk = MSKService()
msk.send_message({"test": "data"})
```

## Features

- Centralized configuration management
- Structured logging
- AWS authentication utilities
- Service-oriented architecture
- Load testing capabilities