from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.sasl.oauth import AbstractTokenProvider
import socket
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import os
from dotenv import load_dotenv # type: ignore
import json
import uuid
from datetime import datetime
import random
import time

load_dotenv()

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-east-1')
        return token

tp = MSKTokenProvider()

producer = KafkaProducer(
    bootstrap_servers=os.getenv("MSK_BS_IAM"),
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=tp,
    client_id=socket.gethostname(),
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 隨機數據生成的範例資料
sample_users = ["Alice", "Bob", "Charlie", "Diana", "Eve", "Frank", "Grace", "Henry","Jason"]
sample_products = ["Laptop", "Phone", "Tablet", "Headphones", "Mouse", "Keyboard", "Monitor", "Speaker"]
sample_actions = ["purchase", "view", "add_to_cart", "remove_from_cart", "login", "logout", "search"]
sample_categories = ["Electronics", "Books", "Clothing", "Home", "Sports", "Beauty", "Automotive"]

topic = os.getenv("MSK_TOPIC")

# 生成50筆隨機數據
for i in range(50):
    try:
        # 隨機生成數據
        message_data = {
            "id": str(uuid.uuid4()),
            "user_id": random.choice(sample_users),
            "product": random.choice(sample_products),
            "action": random.choice(sample_actions),
            "category": random.choice(sample_categories),
            "price": round(random.uniform(10.0, 1000.0), 2),
            "quantity": random.randint(1, 5),
            "timestamp": datetime.now().isoformat(),
            "source": socket.gethostname(),
            "session_id": str(uuid.uuid4())[:8],  # 短的 session ID
            "ip_address": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}"
        }
        
        producer.send(topic, message_data)
        print(f"Sent message {i+1}/50: {message_data['user_id']} - {message_data['action']} - {message_data['product']}")
        
        # 每發送一筆資料後稍微延遲，避免過於密集
        time.sleep(0.1)
        
    except Exception as e:
        print(f"Failed to send message {i+1}: {e}")

# 確保所有訊息都發送完成
producer.flush()
print("All 50 messages sent successfully!")
producer.close()