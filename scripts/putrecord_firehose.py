import boto3
import json
from datetime import datetime
import uuid
import random

# Initialize Firehose client
firehose_client = boto3.client('firehose', region_name='us-east-1')  # Change region as needed

def create_compliance_event_record():
    """Create a simple compliance event record"""
    event_id = str(uuid.uuid4())
    timestamp = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
    
    data = {
        "event_id": event_id,
        "operator_id": f"user_{random.randint(100, 999)}",
        "operator_name": random.choice(["John Doe", "Jane Smith", "Bob Johnson", "Alice Brown"]),
        "client_id": f"client_{random.randint(1, 100):03d}",
        "action": random.choice(["create", "modify", "delete", "view"]),
        "target_type": random.choice(["portfolio", "account", "transaction", "report"]),
        "timestamp": timestamp,
        "amount": round(random.uniform(1000, 50000), 2)
    }
    return data


def send_to_firehose(delivery_stream_name, record_data):
    """Send JSON record to Kinesis Data Firehose"""
    try:
        # Convert dict to JSON string
        json_data = json.dumps(record_data)
        
        # Add newline for proper record separation in Firehose
        json_data_with_newline = json_data + '\n'
        
        start_time = datetime.utcnow()
        # Send record to Firehose
        response = firehose_client.put_record(
            DeliveryStreamName=delivery_stream_name,
            Record={
                'Data': json_data_with_newline.encode('utf-8')
            }
        )

        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds() * 1000  # duration in milliseconds
        print(f"Time taken to send record: {duration:.2f}")
        
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
        
        start_time = datetime.utcnow()
        # Send batch records to Firehose (max 500 records per batch)
        response = firehose_client.put_record_batch(
            DeliveryStreamName=delivery_stream_name,
            Records=firehose_records
        )
        end_time = datetime.utcnow()
        duration = (end_time - start_time).total_seconds() * 1000  #
        print(f"Time taken to send batch records: {duration:.2f} ms")
        
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
    DELIVERY_STREAM_NAME = "iceberg"  # Replace with your Firehose delivery stream name
    
    # Example 1: Send single record
    #print("=== Sending Single Record ===")
    #record = create_compliance_event_record()
    #send_to_firehose(DELIVERY_STREAM_NAME, record)
    
    # Example 2: Send multiple records in batch
    print("\n=== Sending Batch Records ===")
    batch_records = [create_compliance_event_record() for _ in range(500)]
    send_batch_to_firehose(DELIVERY_STREAM_NAME, batch_records)