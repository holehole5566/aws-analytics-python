from opensearchpy import OpenSearch
import numpy as np

# OpenSearch 連接設定
host = ''
port = 443
auth = ('', '')  # 預設帳號密碼

# 建立 OpenSearch 客戶端
client = OpenSearch(
    hosts=[{'host': host, 'port': port}],
    http_auth=auth,
    use_ssl=True,
    verify_certs=False
)

# 建立索引設定（如果索引不存在）
index_name = 'test-index-dynamic-knn'
index_body = {
    'settings': {
        'index': {
            'knn': True
        }
    },
    'mappings': {
        'properties': {
            'my_vector': {
                'type': 'knn_vector',
                'dimension': 3
            }
        }
    }
}

# 創建索引
if not client.indices.exists(index=index_name):
    client.indices.create(index=index_name, body=index_body)

# 生成 1536 維向量並確保不是 null
vector = np.random.rand(3).astype(float).tolist()

# 驗證向量
print(f"向量長度: {len(vector)}")
print(f"向量前5個值: {vector[:5]}")
print(f"向量是否為 None: {vector is None}")

test_vector = [0.1] * 3

# 確保向量不包含 NaN 或 inf
vector = [float(x) if not (np.isnan(x) or np.isinf(x)) else 0.0 for x in vector]

# 文件範例
document = {
    'my_vector': vector,
    'title': 'Example Document'
}

# 寫入文件
client.index(
    index=index_name,
    body=document
)

print("文件已成功寫入")

knn_query = {}
q = {}
vector_column_name = "my_vector"

knn_query[vector_column_name] = {}
knn_query[vector_column_name]["vector"] = test_vector
knn_query[vector_column_name]["k"] = 2

q["query"] = {"knn": knn_query}

res = client.search(index=index_name,
                                     body=q,
                                     timeout=600,
                                     # search_type="dfs_query_then_fetch",
                                     track_total_hits=True,
                                     _source=True)
print("搜尋結果:")
for hit in res['hits']['hits']:
    print(f"ID: {hit['_id']}, Score: {hit['_score']}, Source: {hit['_source']}")