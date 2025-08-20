from opensearchpy import OpenSearch, helpers
import os
from datetime import datetime, timezone
from generate import generate_document_aws

from dotenv import load_dotenv # type: ignore
load_dotenv()

# OpenSearch 連接設定
host = os.getenv("OPENSEARCH_ENDPOINT")
port = 443
auth = (os.getenv('OPENSEARCH_USER'), os.getenv('OPENSEARCH_PWD'))  # 預設帳號密碼

# 建立 OpenSearch 客戶端
client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_auth=auth,
    use_ssl=True,
    verify_certs=False
)

# 批量寫入文件
def bulk_insert_documents(client, index_name, documents):
    actions = [
        {
            "_index": index_name,
            "_source": doc,
            "_id": doc.get('id')  # 使用生成的 UUID 作為文件 ID
        } for doc in documents
    ]
    
    helpers.bulk(client, actions)

# 生成並插入 500 個隨機文件
random_documents = generate_document_aws(500)
bulk_insert_documents(client, "test_aws_service", random_documents)

print(f"Inserted {len(random_documents)} random documents successfully!")

