import boto3
import json
import time
import random
import uuid
from datetime import datetime

kinesis = boto3.client('kinesis', region_name='us-east-1')
stream_name = 'test-source'

while True:
    data = {
        'id': str(uuid.uuid4()),
        'timestamp': datetime.now().isoformat(),
        'value': random.randint(1, 1000),
        'temperature': round(random.uniform(15.0, 35.0), 2),
        'status': random.choice(['active', 'inactive', 'pending']),
        'count': random.randint(0, 100)
    }
    
    partition_key = str(uuid.uuid4())
    
    kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey=partition_key
    )
    
    print(f"Sent: {data}")
    time.sleep(0.5)
