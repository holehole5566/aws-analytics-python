import psycopg2
from faker import Faker
import random

# 資料庫連線資訊
DB_NAME = ""
DB_USER = ""
DB_PASS = ""
DB_HOST = ""  
DB_PORT = "5432"

# 初始化 Faker
fake = Faker('zh_TW') # 你也可以使用其他地區，例如 'en_US'

def insert_random_books(num_rows):
    """
    產生指定數量的隨機書籍數據並插入到資料庫中。
    """
    conn = None
    try:
        # 建立資料庫連線
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS,
            host=DB_HOST,
            port=DB_PORT
        )
        cur = conn.cursor()

        # 準備插入資料的 SQL 語句
        sql = """INSERT INTO books (title, author, publication_year, price)
                 VALUES (%s, %s, %s, %s);"""

        print(f"開始插入 {num_rows} 筆隨機數據...")

        for _ in range(num_rows):
            # 使用 Faker 產生隨機數據
            title = fake.sentence(nb_words=random.randint(3, 8))
            author = fake.name()
            publication_year = random.randint(1900, 2025)
            price = round(random.uniform(100.00, 2000.00), 2)

            # 執行 SQL 語句
            cur.execute(sql, (title, author, publication_year, price))

        # 提交 (commit) 事務，將變更寫入資料庫
        conn.commit()
        print(f"成功插入 {num_rows} 筆數據。")

    except (Exception, psycopg2.DatabaseError) as error:
        print(f"發生錯誤: {error}")
        if conn:
            conn.rollback() # 如果發生錯誤，回滾事務

    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

if __name__ == '__main__':
    # 你可以修改這個數字來決定要插入多少筆數據
    insert_random_books(100000)