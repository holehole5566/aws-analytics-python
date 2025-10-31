import pymysql
from faker import Faker
from dotenv import load_dotenv
import os

load_dotenv()
fake = Faker()

def get_connection():
    """Create database connection"""
    return pymysql.connect(
        host=os.getenv('MYSQL_HOST'),
        user=os.getenv('MYSQL_USER'),
        password=os.getenv('MYSQL_PASSWORD'),
        database=os.getenv('MYSQL_DATABASE')
    )

def test_connection(cursor):
    """Test database connection"""
    print("Testing database connection...")
    cursor.execute("SELECT DATABASE(), VERSION(), NOW()")
    result = cursor.fetchone()
    print(f"Connected to database: {result[0]}")
    print(f"MySQL version: {result[1]}")
    print(f"Server time: {result[2]}")
    
    cursor.execute("SHOW TABLES LIKE 'users'")
    if cursor.fetchone():
        print("Table 'users' found")
        cursor.execute("SELECT COUNT(*) FROM users")
        count = cursor.fetchone()[0]
        print(f"Current records in users table: {count}")
    else:
        print("Warning: Table 'users' not found")

def insert_test_data(cursor, num_records=100):
    """Insert test data"""
    print(f"\nInserting {num_records} test records...")
    inserted = 0
    for i in range(num_records):
        name = fake.name()
        email = fake.email()
        
        sql = "INSERT INTO users (name, email) VALUES (%s, %s)"
        try:
            cursor.execute(sql, (name, email))
            inserted += 1
        except pymysql.IntegrityError:
            continue
    
    print(f"Inserted {inserted} records")
    return inserted

if __name__ == "__main__":
    conn = get_connection()
    cursor = conn.cursor()
    
    # Test connection
    test_connection(cursor)
    
    # Control whether to insert
    DO_INSERT = True  # Change to False to skip insert
    
    if DO_INSERT:
        insert_test_data(cursor, num_records=10)
        conn.commit()
    
    cursor.close()
    conn.close()
