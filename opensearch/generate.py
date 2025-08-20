import random
import uuid
from datetime import datetime, timezone


TICKERS = ["EKS", "S3", "EC2"]

# 定義可能的使用者名稱
users = ['Alice', 'Bob', 'Charlie', 'David', 'Eve', 'Frank', 'Grace', 'Henry']

# 定義可能的 HTTP 方法
methods = ['GET', 'POST', 'PUT', 'DELETE', 'PATCH']

# 定義可能的 API 路徑
api_paths = [
    '/api/v1/users',
    '/api/v1/products', 
    '/api/v1/orders',
    '/api/v1/auth/login',
    '/api/v1/auth/logout',
    '/api/v1/profile',
    '/api/v1/settings',
    '/api/v1/dashboard',
    '/api/v1/reports',
    '/api/v1/notifications'
]

def generate_document_aws(num_docs=1):
    documents = []
    
    for _ in range(num_docs):
        name = random.choice(TICKERS)
        document =  {
        "metadata": {
            "product": {
                "name": name
            }
        },
        "message": "ResponseComplete",
        "time": int(datetime.now(timezone.utc).timestamp() * 1000),
        "severity": "Informational",
        "activity_name": "Update",
        "@timestamp": datetime.now(timezone.utc).isoformat()
    }
        documents.append(document)
    return documents

# 生成隨機文件的函數
def generate_random_request(num_docs):
    documents = []
    
    for _ in range(num_docs):
        # 隨機選擇使用者
        user = random.choice(users)
        
        # 隨機選擇 HTTP 方法
        method = random.choice(methods)
        
        # 隨機選擇 API 路徑
        path = random.choice(api_paths)
        
        # 隨機生成響應大小 (1KB 到 1MB)
        response_size = random.randint(1, 10)
        
        # 隨機選擇響應類型和對應的狀態碼
        response_types = [
            {"type": "ok", "codes": [200, 201, 202, 204]},
            {"type": "client error", "codes": [400, 401, 403, 404, 422]},
            {"type": "server error", "codes": [500, 502, 503, 504]}
    ]
        
        response_type_data = random.choice(response_types)
        status_code = random.choice(response_type_data["codes"])
        
        # 生成當前時間戳 (加上一些隨機秒數來模擬不同時間)
        current_time = datetime.now()
        random_seconds = random.randint(-3600, 0)  # 過去一小時內的隨機時間
        timestamp = current_time.timestamp() + random_seconds
        
        # 建立文件
        document = {
            "id": str(uuid.uuid4()),  # 唯一識別碼
            "user": user,
            "method": method,
            "path": path,
            "response_size": response_size,
            "status_code": status_code,
            "response_type": response_type_data["type"],
            "timestamp": timestamp,
            "timestamp_iso": datetime.fromtimestamp(timestamp).isoformat(),  # ISO 格式時間
            "ip_address": f"{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}.{random.randint(1,255)}",
            "additional_info": {
                "device": random.choice(["mobile", "desktop", "tablet"]),
                "browser": random.choice(["Chrome", "Firefox", "Safari", "Edge"]),
                "user_agent": f"Mozilla/5.0 ({random.choice(['Windows NT 10.0', 'Macintosh', 'X11; Linux x86_64'])})"
            }
        }
        
        documents.append(document)
    
    return documents


