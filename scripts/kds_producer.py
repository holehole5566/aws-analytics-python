import boto3
import json
import time
import random
import uuid
from datetime import datetime

kinesis = boto3.client('kinesis', region_name='us-east-1')
stream_name = 'test-source'

product_categories = ['Electronics', 'Clothing', 'Books', 'Home', 'Sports']

while True:
    data = {
        'orderId': str(uuid.uuid4()),
        'customerId': f'CUST-{random.randint(1000, 9999)}',
        'productCategory': random.choice(product_categories),
        'price': round(random.randint(10, 500) + random.randint(10, 99) / 100, 2),
        'quantity': random.randint(1, 10),
        'timestamp': datetime.now().isoformat()
    }
    
    partition_key = str(uuid.uuid4())
    
    kinesis.put_record(
        StreamName=stream_name,
        Data=json.dumps(data),
        PartitionKey=partition_key
    )
    
    print(f"Sent: {data}")
    time.sleep(0.5)
