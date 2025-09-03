import boto3
from requests_aws4auth import AWS4Auth
from ..config import Settings

def get_aws_credentials():
    """Get AWS credentials for authentication"""
    session = boto3.Session()
    credentials = session.get_credentials()
    
    if not credentials:
        raise ValueError("AWS credentials not found")
    
    return credentials

def get_aws4auth(service: str = 'es'):
    """Get AWS4Auth for HTTP requests"""
    credentials = get_aws_credentials()
    
    return AWS4Auth(
        credentials.access_key,
        credentials.secret_key,
        Settings.AWS_REGION,
        service,
        session_token=credentials.token
    )