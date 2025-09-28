import boto3
import json
from datetime import datetime
import uuid

# Initialize Firehose client
firehose_client = boto3.client('firehose', region_name='us-east-1')  # Change region as needed

def create_compliance_event_record():
    """Create a sample compliance event record"""
    event_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().isoformat() + 'Z'
    
    data = {
        "event_id": event_id,
        "schema_version": "1.0",
        "role": "advisor",
        "operator_id": "user_456",
        "operator_name": "John Doe",
        "operator_email": "john.doe@example.com",
        "broker_dealer_id": "123",
        "broker_dealer_name": "ABC Securities",
        "organization_id": "123",
        "organization_name": "Investment Corp",
        "client_id": "client_001",
        "client_name": "Jane Smith",
        "action": "modify",
        "target_type": "portfolio_allocation",
        "ip": "203.0.113.7",
        "timestamp": timestamp,
        "context": {
            "diff": {
                "before": {
                    "equities": 0.6,
                    "bonds": 0.35,
                    "cash": 0.05
                },
                "after": {
                    "equities": 0.55,
                    "bonds": 0.4,
                    "cash": 0.05
                }
            },
            "artifact_ref": {
                "artifact_id": "art_9d2f",
                "artifact_hash": "",
                "s3_uri": "s3://compliance-worm/artifacts/2025/09/19/org_123/plan_789_v12.pdf",
                "type": "pdf"
            }
        },
        "event_hash": "b5b1...8a2"
    }
    
    return data

def send_to_firehose(delivery_stream_name, record_data):
    """Send JSON record to Kinesis Data Firehose"""
    try:
        # Convert dict to JSON string
        json_data = json.dumps(record_data)
        
        # Add newline for proper record separation in Firehose
        json_data_with_newline = json_data + '\n'
        
        # Send record to Firehose
        response = firehose_client.put_record(
            DeliveryStreamName=delivery_stream_name,
            Record={
                'Data': json_data_with_newline.encode('utf-8')
            }
        )
        
        print(f"Successfully sent record to Firehose")
        print(f"Record ID: {response['RecordId']}")
        print(f"Response: {response}")
        
        return response
        
    except Exception as e:
        print(f"Error sending record to Firehose: {str(e)}")
        raise

def send_batch_to_firehose(delivery_stream_name, records_list):
    """Send multiple JSON records to Kinesis Data Firehose in batch"""
    try:
        # Prepare records for batch sending
        firehose_records = []
        for record_data in records_list:
            json_data = json.dumps(record_data) + '\n'
            firehose_records.append({
                'Data': json_data.encode('utf-8')
            })
        
        # Send batch records to Firehose (max 500 records per batch)
        response = firehose_client.put_record_batch(
            DeliveryStreamName=delivery_stream_name,
            Records=firehose_records
        )
        
        print(f"Successfully sent {len(firehose_records)} records to Firehose")
        print(f"Failed record count: {response['FailedPutCount']}")
        
        # Check for any failed records
        if response['FailedPutCount'] > 0:
            print("Some records failed:")
            for i, record_result in enumerate(response['RequestResponses']):
                if 'ErrorCode' in record_result:
                    print(f"Record {i}: {record_result['ErrorCode']} - {record_result['ErrorMessage']}")
        
        return response
        
    except Exception as e:
        print(f"Error sending batch records to Firehose: {str(e)}")
        raise

# Main execution
if __name__ == "__main__":
    # Configuration
    DELIVERY_STREAM_NAME = "test-parquet-3"  # Replace with your Firehose delivery stream name
    
    # Example 1: Send single record
    print("=== Sending Single Record ===")
    record = create_compliance_event_record()
    send_to_firehose(DELIVERY_STREAM_NAME, record)
    
    # Example 2: Send multiple records in batch
    print("\n=== Sending Batch Records ===")
    batch_records = [create_compliance_event_record() for _ in range(100)]
    send_batch_to_firehose(DELIVERY_STREAM_NAME, batch_records)