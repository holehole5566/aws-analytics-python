import boto3
import json
import requests
import os
import sys
from requests_aws4auth import AWS4Auth
from dotenv import load_dotenv

load_dotenv()

def make_request():
    # Validate required environment variables
    url = os.environ.get("HTTP_URL")
    if not url:
        raise ValueError("HTTP_URL environment variable is required")
    
    # Setup AWS authentication
    region = os.environ.get("AWS_REGION", "us-east-1")
    service = 'es'

    url += "/_snapshot/poyenc/"
    
    try:
        credentials = boto3.Session().get_credentials()
        if not credentials:
            raise ValueError("AWS credentials not found")
            
        awsauth = AWS4Auth(
            credentials.access_key, 
            credentials.secret_key,
            region, 
            service, 
            session_token=credentials.token
        )
    except Exception as e:
        raise RuntimeError(f"Failed to setup AWS authentication: {e}")
    
    # Prepare request
    payload = {
  "type": "s3",
  "settings": {
    "bucket": "opensearch-snapshot-poyenc",
    "base_path": "opensearch-snapshots",
    "region": "us-east-1",
    "role_arn": "arn:aws:iam::533267144878:role/TheSnapshotRole"
  }
}

    headers = {"Content-Type": "application/json"}
    
    try:
        # Make the signed HTTP request
        response = requests.put(
            url, 
            auth=awsauth, 
            headers=headers, 
            json=payload,  # Use json parameter instead of data
            timeout=30
        )
        #response.raise_for_status()
        
        print(f"Status: {response.status_code}")
        print(f"Response: {response.json()}")
        return response.json()
        
    except requests.exceptions.RequestException as e:
        print(f"Request failed: {e}")
        sys.exit(1)
    except json.JSONDecodeError as e:
        print(f"Invalid JSON response: {e}")
        sys.exit(1)

if __name__ == "__main__":
    make_request()
