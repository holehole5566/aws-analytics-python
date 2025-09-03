import boto3
import json
import time
from ..config import Settings
from ..utils import get_logger

class KinesisService:
    """Kinesis Data Streams service"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.client = boto3.client('kinesis', region_name=Settings.AWS_REGION)
        self.stream_name = Settings.KDS_NAME
        
    def put_record(self, data, partition_key=None):
        """Put single record to stream"""
        if not partition_key:
            partition_key = str(int(time.time()))
            
        try:
            response = self.client.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(data),
                PartitionKey=partition_key
            )
            self.logger.info(f"Record sent: {response['SequenceNumber']}")
            return response
        except Exception as e:
            self.logger.error(f"Failed to put record: {e}")
            raise
    
    def put_records(self, records):
        """Put multiple records to stream"""
        kinesis_records = []
        
        for i, record in enumerate(records):
            kinesis_records.append({
                'Data': json.dumps(record),
                'PartitionKey': str(i)
            })
        
        try:
            response = self.client.put_records(
                Records=kinesis_records,
                StreamName=self.stream_name
            )
            
            failed_count = response['FailedRecordCount']
            success_count = len(records) - failed_count
            
            self.logger.info(f"Sent {success_count} records, {failed_count} failed")
            return response
            
        except Exception as e:
            self.logger.error(f"Failed to put records: {e}")
            raise
    
    def describe_stream(self):
        """Get stream information"""
        try:
            response = self.client.describe_stream(StreamName=self.stream_name)
            return response['StreamDescription']
        except Exception as e:
            self.logger.error(f"Failed to describe stream: {e}")
            raise
    
    def list_shards(self):
        """List stream shards"""
        try:
            response = self.client.list_shards(StreamName=self.stream_name)
            return response['Shards']
        except Exception as e:
            self.logger.error(f"Failed to list shards: {e}")
            raise