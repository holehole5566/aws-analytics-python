#!/usr/bin/env python3
"""Lake Formation IAM Migration Script"""

import sys
import argparse
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aws_analytics.services import LakeFormationService
from aws_analytics.utils import get_logger

def main():
    parser = argparse.ArgumentParser(description='Migrate Lake Formation to IAM access control')
    parser.add_argument('-d', '--databases', type=str, 
                       help='Comma separated list of target database names (default: all databases)')
    parser.add_argument('--skip-errors', action='store_true',
                       help='Skip errors and continue execution (default: false)')
    parser.add_argument('--dryrun', action='store_true',
                       help='Display operations without executing (default: false)')
    parser.add_argument('--no-global', action='store_true',
                       help='Skip global configuration updates (default: false)')
    parser.add_argument('-v', '--verbose', action='store_true',
                       help='Display verbose logging (default: false)')
    
    args = parser.parse_args()
    
    # Setup logging
    log_level = "DEBUG" if args.verbose else "INFO"
    logger = get_logger(__name__, log_level)
    
    # Parse target databases
    target_databases = None
    if args.databases:
        target_databases = [db.strip() for db in args.databases.split(',')]
        logger.info(f"Target databases: {target_databases}")
    
    # Confirmation prompt
    if not args.dryrun:
        response = input("Are you sure you want to migrate Lake Formation to IAM access control? (y/N): ")
        if response.lower() not in ['y', 'yes']:
            logger.info("Migration cancelled")
            sys.exit(0)
    else:
        logger.info("Running in dry run mode - no changes will be made")
        
    try:
        lf = LakeFormationService()
        
        if args.dryrun:
            logger.info("DRY RUN: Would perform the following operations:")
            logger.info("1. Update data lake settings to use IAM controls")
            logger.info("2. De-register all data lake locations")
            logger.info("3. Grant CREATE_DATABASE to IAM_ALLOWED_PRINCIPALS")
            logger.info("4. Grant ALL permissions to IAM_ALLOWED_PRINCIPALS for databases/tables")
            logger.info("5. Revoke all other Lake Formation permissions")
            
            if target_databases:
                logger.info(f"   - Limited to databases: {target_databases}")
            if args.no_global:
                logger.info("   - Skipping global configuration updates")
                
        else:
            # Perform actual migration
            lf.migrate_to_iam_control(
                target_databases=target_databases,
                skip_errors=args.skip_errors,
                global_config=not args.no_global
            )
            
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()