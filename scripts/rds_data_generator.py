#!/usr/bin/env python3
"""RDS Data Generator Script"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aws_analytics.services import RDSService
from aws_analytics.utils import get_logger

def main():
    logger = get_logger(__name__)
    
    try:
        # Use RDS configuration from environment
        rds = RDSService()
        
        # Create table
        logger.info("Creating books table...")
        rds.create_books_table()
        
        # Insert data
        num_records = int(input("Number of records to insert (default 1000): ").strip() or 1000)
        rds.insert_random_books(num_records)
        
        # Show sample data
        logger.info("Sample records:")
        books = rds.get_books(5)
        for book in books:
            logger.info(f"  {book['title']} by {book['author']} ({book['publication_year']})")
        
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()