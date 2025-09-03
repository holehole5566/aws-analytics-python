#!/usr/bin/env python3
"""OpenSearch Indexing Script"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aws_analytics.services import OpenSearchService
from aws_analytics.config import Settings
from aws_analytics.utils import get_logger
from datetime import datetime, timezone
import random
import uuid

def generate_aws_documents(num_docs=500):
    """Generate sample AWS service documents"""
    tickers = ["EKS", "S3", "EC2", "RDS", "Lambda"]
    documents = []
    
    for _ in range(num_docs):
        name = random.choice(tickers)
        document = {
            "id": str(uuid.uuid4()),
            "metadata": {
                "product": {
                    "name": name
                }
            },
            "message": "ResponseComplete",
            "time": int(datetime.now(timezone.utc).timestamp() * 1000),
            "severity": "Informational",
            "activity_name": "Update",
            "@timestamp": datetime.now(timezone.utc).isoformat()
        }
        documents.append(document)
    
    return documents

def main():
    logger = get_logger(__name__)
    
    try:
        opensearch = OpenSearchService()
        index_name = Settings.OPENSEARCH_INDEX or "test_aws_service"
        
        # Generate documents
        documents = generate_aws_documents(500)
        logger.info(f"Generated {len(documents)} documents")
        
        # Bulk index
        opensearch.bulk_index(index_name, documents)
        logger.info(f"Successfully indexed {len(documents)} documents to {index_name}")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()