import boto3
import json
import time

kinesis = boto3.client('kinesis', region_name='us-east-1')
stream_name = 'test-sink'

# Get all shards
response = kinesis.describe_stream(StreamName=stream_name)
shards = response['StreamDescription']['Shards']

# Get iterator for each shard
shard_iterators = {}
for shard in shards:
    shard_id = shard['ShardId']
    shard_iterators[shard_id] = kinesis.get_shard_iterator(
        StreamName=stream_name,
        ShardId=shard_id,
        ShardIteratorType='LATEST'
    )['ShardIterator']

# Consume from all shards
while True:
    for shard_id, iterator in list(shard_iterators.items()):
        response = kinesis.get_records(ShardIterator=iterator, Limit=100)
        
        for record in response['Records']:
            data = json.loads(record['Data'])
            print(f"Shard {shard_id}: {data}")
        
        shard_iterators[shard_id] = response['NextShardIterator']
    
    time.sleep(1)
