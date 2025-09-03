#!/usr/bin/env python3
"""HTTP Request Script with AWS Auth"""

import sys
import requests
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aws_analytics.config import Settings
from aws_analytics.utils import get_aws4auth, get_logger

def main():
    logger = get_logger(__name__)
    
    if not Settings.HTTP_URL:
        logger.error("HTTP_URL not configured")
        sys.exit(1)
    
    try:
        awsauth = get_aws4auth()
        
        payload = {"size": 1}
        headers = {"Content-Type": "application/json"}
        
        response = requests.get(
            Settings.HTTP_URL,
            auth=awsauth,
            headers=headers,
            json=payload,
            timeout=30
        )
        response.raise_for_status()
        
        logger.info(f"Status: {response.status_code}")
        logger.info(f"Response: {response.json()}")
        
    except Exception as e:
        logger.error(f"Request failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()