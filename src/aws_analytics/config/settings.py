import os
from dotenv import load_dotenv

load_dotenv()

class Settings:
    """Centralized configuration management"""
    
    # OpenSearch
    OPENSEARCH_ENDPOINT = os.getenv("OPENSEARCH_ENDPOINT")
    OPENSEARCH_USER = os.getenv("OPENSEARCH_USER")
    OPENSEARCH_PWD = os.getenv("OPENSEARCH_PWD")
    OPENSEARCH_INDEX = os.getenv("OPENSEARCH_INDEX")
    
    # MSK
    MSK_BS_IAM = os.getenv("MSK_BS_IAM")
    MSK_BOOTSTRAP_SERVERS = os.getenv("MSK_BOOTSTRAP_SERVERS")
    MSK_TOPIC = os.getenv("MSK_TOPIC")
    
    # Kinesis
    KDS_NAME = os.getenv("KDS_NAME")
    
    # RDS
    DB_NAME = os.getenv("DB_NAME")
    DB_USER = os.getenv("DB_USER")
    DB_PASSWORD = os.getenv("DB_PASSWORD")
    DB_HOST = os.getenv("DB_HOST")
    DB_PORT = os.getenv("DB_PORT", "5432")
    
    # HTTP
    HTTP_URL = os.getenv("HTTP_URL")
    
    # Neptune
    NEPTUNE_ENDPOINT = os.getenv("NEPTUNE_ENDPOINT")
    
    # AWS
    AWS_REGION = os.getenv("AWS_REGION", "us-east-1")
    
    @classmethod
    def validate(cls):
        """Validate required settings"""
        required = {
            "OPENSEARCH_ENDPOINT": cls.OPENSEARCH_ENDPOINT,
            "MSK_BOOTSTRAP_SERVERS": cls.MSK_BOOTSTRAP_SERVERS,
        }
        
        missing = [k for k, v in required.items() if not v]
        if missing:
            raise ValueError(f"Missing required settings: {', '.join(missing)}")
        
        return True