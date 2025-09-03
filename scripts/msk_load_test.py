#!/usr/bin/env python3
"""MSK Load Testing Script"""

import sys
import time
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aws_analytics.services import MSKService
from aws_analytics.config import Settings
from aws_analytics.utils import get_logger

def main():
    logger = get_logger(__name__)
    
    try:
        Settings.validate()
        msk = MSKService()
        
        logger.info("Starting MSK load test...")
        threads = msk.generate_load(num_threads=5, messages_per_sec=500)
        
        # Keep running until interrupted
        try:
            while True:
                time.sleep(10)
                logger.info(f"Load test running with {len(threads)} threads")
        except KeyboardInterrupt:
            logger.info("Stopping load test...")
            
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()