import boto3
import sys
from ..config import Settings
from ..utils import get_logger

class LakeFormationService:
    """Lake Formation service for data lake permissions management"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.lakeformation = boto3.client('lakeformation', region_name=Settings.AWS_REGION)
        self.glue = boto3.client('glue', region_name=Settings.AWS_REGION)
        self.sts = boto3.client('sts', region_name=Settings.AWS_REGION)
        self.account_id = self.sts.get_caller_identity().get('Account')
        self.iam_allowed_principal = {'DataLakePrincipalIdentifier': 'IAM_ALLOWED_PRINCIPALS'}
        
    def update_data_lake_settings(self):
        """Update data lake settings to use IAM access control only"""
        self.logger.info('Modifying Data Lake Settings to use IAM Controls only...')
        
        data_lake_setting = self.lakeformation.get_data_lake_settings()['DataLakeSettings']
        data_lake_setting['CreateDatabaseDefaultPermissions'] = [
            {'Principal': self.iam_allowed_principal, 'Permissions': ['ALL']}
        ]
        data_lake_setting['CreateTableDefaultPermissions'] = [
            {'Principal': self.iam_allowed_principal, 'Permissions': ['ALL']}
        ]
        
        self.lakeformation.put_data_lake_settings(DataLakeSettings=data_lake_setting)
        self.logger.info('Data Lake Settings updated')
        
    def deregister_data_lake_locations(self):
        """De-register all data lake locations"""
        self.logger.info('De-registering all data lake locations...')
        
        res = self.lakeformation.list_resources()
        resources = res['ResourceInfoList']
        
        while 'NextToken' in res:
            res = self.lakeformation.list_resources(NextToken=res['NextToken'])
            resources.extend(res['ResourceInfoList'])
            
        for r in resources:
            self.logger.info(f"De-registering {r['ResourceArn']}")
            self.lakeformation.deregister_resource(ResourceArn=r['ResourceArn'])
            
        self.logger.info('All data lake locations de-registered')
        
    def grant_catalog_permissions(self):
        """Grant CREATE_DATABASE to IAM_ALLOWED_PRINCIPALS for catalog"""
        self.logger.info('Granting CREATE_DATABASE to IAM_ALLOWED_PRINCIPALS for catalog...')
        
        catalog_resource = {'Catalog': {}}
        
        try:
            self.lakeformation.grant_permissions(
                Principal=self.iam_allowed_principal,
                Resource=catalog_resource,
                Permissions=['CREATE_DATABASE'],
                PermissionsWithGrantOption=[]
            )
            self.logger.info('Catalog permissions granted')
        except Exception as e:
            self.logger.error(f"Error granting catalog permissions: {e}")
            raise
            
    def grant_database_table_permissions(self, target_databases=None):
        """Grant ALL permissions to IAM_ALLOWED_PRINCIPALS for databases and tables"""
        self.logger.info('Granting ALL to IAM_ALLOWED_PRINCIPALS for databases and tables...')
        
        # Get all databases
        databases = []
        paginator = self.glue.get_paginator('get_databases')
        for page in paginator.paginate():
            databases.extend(page['DatabaseList'])
            
        for db in databases:
            database_name = db['Name']
            
            # Skip resource links
            if 'TargetDatabase' in db:
                self.logger.debug(f"Skipping resource link: {database_name}")
                continue
                
            # Skip if not in target databases
            if target_databases and database_name not in target_databases:
                self.logger.debug(f"Skipping database not in targets: {database_name}")
                continue
                
            # Grant database permissions
            self.logger.info(f"Granting permissions on database: {database_name}")
            database_resource = {'Database': {'Name': database_name}}
            
            try:
                self.lakeformation.grant_permissions(
                    Principal=self.iam_allowed_principal,
                    Resource=database_resource,
                    Permissions=['ALL'],
                    PermissionsWithGrantOption=[]
                )
            except Exception as e:
                self.logger.error(f"Error granting database permissions for {database_name}: {e}")
                
            # Update database default permissions
            self._update_database_default_permissions(db, database_name)
            
            # Grant table permissions
            self._grant_table_permissions(database_name)
            
    def _update_database_default_permissions(self, db, database_name):
        """Update database default table permissions"""
        self.logger.info(f"Updating default permissions for database: {database_name}")
        
        database_input = {
            'Name': database_name,
            'Description': db.get('Description', ''),
            'Parameters': db.get('Parameters', {}),
            'CreateTableDefaultPermissions': [
                {
                    'Principal': self.iam_allowed_principal,
                    'Permissions': ['ALL']
                }
            ]
        }
        
        if db.get('LocationUri'):
            database_input['LocationUri'] = db['LocationUri']
            
        self.glue.update_database(Name=database_name, DatabaseInput=database_input)
        
    def _grant_table_permissions(self, database_name):
        """Grant permissions for all tables in database"""
        tables = []
        paginator = self.glue.get_paginator('get_tables')
        for page in paginator.paginate(DatabaseName=database_name):
            tables.extend(page['TableList'])
            
        for table in tables:
            table_name = table['Name']
            
            # Skip resource links
            if 'TargetTable' in table:
                self.logger.debug(f"Skipping table resource link: {database_name}.{table_name}")
                continue
                
            self.logger.info(f"Granting permissions on table: {database_name}.{table_name}")
            table_resource = {'Table': {'DatabaseName': database_name, 'Name': table_name}}
            
            try:
                self.lakeformation.grant_permissions(
                    Principal=self.iam_allowed_principal,
                    Resource=table_resource,
                    Permissions=['ALL'],
                    PermissionsWithGrantOption=[]
                )
            except Exception as e:
                self.logger.error(f"Error granting table permissions for {database_name}.{table_name}: {e}")
                
    def revoke_all_permissions(self, target_databases=None, skip_errors=False):
        """Revoke all Lake Formation permissions except IAM_ALLOWED_PRINCIPALS"""
        self.logger.info('Revoking all permissions except IAM_ALLOWED_PRINCIPALS...')
        
        res = self.lakeformation.list_permissions()
        permissions = res['PrincipalResourcePermissions']
        
        while 'NextToken' in res:
            res = self.lakeformation.list_permissions(NextToken=res['NextToken'])
            permissions.extend(res['PrincipalResourcePermissions'])
            
        for perm in permissions:
            principal_id = perm['Principal']['DataLakePrincipalIdentifier']
            
            if principal_id == 'IAM_ALLOWED_PRINCIPALS':
                continue
                
            resource = perm['Resource']
            resource_type, resource_name, resource_db = self._get_resource_info(resource)
            
            # Skip if not owned by this account
            catalog_id = self._get_catalog_id(resource)
            if catalog_id != self.account_id:
                continue
                
            # Skip if not in target databases
            if target_databases and resource_db and resource_db not in target_databases:
                continue
                
            self.logger.info(f"Revoking permissions for {principal_id} on {resource_type}: {resource_name}")
            
            # Handle special cases for revoke
            self._prepare_resource_for_revoke(resource, resource_type)
            
            try:
                self.lakeformation.revoke_permissions(
                    Principal=perm['Principal'],
                    Resource=resource,
                    Permissions=perm['Permissions'],
                    PermissionsWithGrantOption=perm['PermissionsWithGrantOption']
                )
            except Exception as e:
                self.logger.error(f"Error revoking permissions: {e}")
                if not skip_errors:
                    raise
                    
    def _get_resource_info(self, resource):
        """Get resource type, name and database from resource"""
        if "Catalog" in resource:
            return "Catalog", "catalog", ""
        elif "Database" in resource:
            name = resource["Database"]['Name']
            return "Database", name, name
        elif "Table" in resource:
            db_name = resource["Table"]['DatabaseName']
            table_name = resource["Table"]['Name']
            return "Table", f"{db_name}.{table_name}", db_name
        elif "TableWithColumns" in resource:
            db_name = resource["TableWithColumns"]['DatabaseName']
            table_name = resource["TableWithColumns"]['Name']
            return "TableWithColumns", f"{db_name}.{table_name}.columns", db_name
        else:
            return "Unknown", "unknown", ""
            
    def _get_catalog_id(self, resource):
        """Get catalog ID from resource"""
        for key, value in resource.items():
            if isinstance(value, dict):
                if 'CatalogId' in value:
                    return value['CatalogId']
                return self._get_catalog_id(value)
        return self.account_id
        
    def _prepare_resource_for_revoke(self, resource, resource_type):
        """Prepare resource for revoke operation"""
        # Handle TableWildcard case
        if resource_type == "Table" and 'TableWildcard' in resource.get('Table', {}):
            if 'Name' in resource['Table']:
                del resource['Table']['Name']
                
        # Handle ColumnWildcard for ALL_TABLES
        if (resource_type == "TableWithColumns" and 
            'ColumnWildcard' in resource.get('TableWithColumns', {}) and
            resource.get('TableWithColumns', {}).get('Name') == "ALL_TABLES"):
            
            table_with_cols = resource['TableWithColumns']
            resource['Table'] = {
                'CatalogId': table_with_cols.get('CatalogId'),
                'DatabaseName': table_with_cols['DatabaseName'],
                'TableWildcard': {}
            }
            del resource['TableWithColumns']
            
    def migrate_to_iam_control(self, target_databases=None, skip_errors=False, global_config=True):
        """Complete migration to IAM access control"""
        self.logger.info("Starting Lake Formation migration to IAM access control...")
        
        try:
            if global_config:
                self.update_data_lake_settings()
                self.deregister_data_lake_locations()
                self.grant_catalog_permissions()
            else:
                self.logger.info("Skipping global configuration updates")
                
            self.grant_database_table_permissions(target_databases)
            self.revoke_all_permissions(target_databases, skip_errors)
            
            self.logger.info("Lake Formation migration completed successfully!")
            
        except Exception as e:
            self.logger.error(f"Migration failed: {e}")
            raise