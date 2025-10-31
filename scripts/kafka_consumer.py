from kafka import KafkaConsumer
from dotenv import load_dotenv
import os
import json
from datetime import datetime

load_dotenv()

def safe_deserialize(x):
    """Safely deserialize message value"""
    if not x:
        return None
    try:
        return json.loads(x.decode('utf-8'))
    except (json.JSONDecodeError, UnicodeDecodeError):
        # Return raw bytes as string if not JSON
        return x.decode('utf-8', errors='ignore')

# Kafka consumer configuration
consumer = KafkaConsumer(
    'dev-mysql.dev.users',
    bootstrap_servers=os.getenv('MSK_BOOTSTRAP_SERVERS').split(','),
    auto_offset_reset='earliest',  # Start from beginning
    enable_auto_commit=True,
    value_deserializer=safe_deserialize
)

print("Starting to consume messages from topic: dev-mysql.dev.users")
print("Press Ctrl+C to stop\n")

messages = []

try:
    for message in consumer:
        msg_data = {
            'offset': message.offset,
            'partition': message.partition,
            'timestamp': message.timestamp,
            'key': message.key.decode('utf-8') if message.key else None,
            'value': message.value
        }
        messages.append(msg_data)
        
        print(f"Offset: {message.offset}")
        print(f"Key: {message.key}")
        print(f"Value: {json.dumps(message.value, indent=2, default=str)}")
        print("-" * 80)
except KeyboardInterrupt:
    print("\nStopping consumer...")
finally:
    consumer.close()
    
    # Save to JSON file
    if messages:
        filename = f"kafka_messages_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(messages, f, indent=2, ensure_ascii=False, default=str)
        print(f"\nSaved {len(messages)} messages to {filename}")
    else:
        print("\nNo messages consumed")
