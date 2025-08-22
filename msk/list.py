from kafka.admin import KafkaAdminClient
from kafka.sasl.oauth import AbstractTokenProvider
import socket
from aws_msk_iam_sasl_signer import MSKAuthTokenProvider
import os
from dotenv import load_dotenv

load_dotenv()

class MSKTokenProvider(AbstractTokenProvider):
    def token(self):
        token, _ = MSKAuthTokenProvider.generate_auth_token('us-east-1')
        return token

tp = MSKTokenProvider()

admin_client = KafkaAdminClient(
    bootstrap_servers=os.getenv("MSK_BS_IAM"),
    security_protocol='SASL_SSL',
    sasl_mechanism='OAUTHBEARER',
    sasl_oauth_token_provider=tp,
    client_id=socket.gethostname(),
)

# 簡單列出 topics
print("Topics:")
topics = admin_client.list_topics()
for topic in topics:
    if not topic.startswith('__'):
        print(f"  - {topic}")

print("\nConsumer Groups:")
groups = admin_client.list_consumer_groups()
for group in groups:
    print(group)

admin_client.close()