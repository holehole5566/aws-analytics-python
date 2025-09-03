import psycopg2
from psycopg2.extras import RealDictCursor
from faker import Faker
import random
from ..config import Settings
from ..utils import get_logger

class RDSService:
    """RDS PostgreSQL service for database operations"""
    
    def __init__(self, db_config=None):
        self.logger = get_logger(__name__)
        self.db_config = db_config or self._get_db_config()
        self.fake = Faker('en_US')
        
    def _get_db_config(self):
        """Get database configuration from environment"""
        if not all([Settings.DB_NAME, Settings.DB_USER, Settings.DB_PASSWORD, Settings.DB_HOST]):
            raise ValueError("Database configuration incomplete. Check DB_NAME, DB_USER, DB_PASSWORD, DB_HOST in .env")
            
        return {
            'dbname': Settings.DB_NAME,
            'user': Settings.DB_USER,
            'password': Settings.DB_PASSWORD,
            'host': Settings.DB_HOST,
            'port': Settings.DB_PORT or '5432'
        }
    
    def get_connection(self):
        """Get database connection"""
        try:
            conn = psycopg2.connect(**self.db_config)
            return conn
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise
    
    def execute_query(self, query, params=None, fetch=False):
        """Execute SQL query"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute(query, params)
                
                if fetch:
                    return cur.fetchall()
                
                conn.commit()
                return cur.rowcount
                
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Query execution failed: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def insert_random_books(self, num_rows=1000):
        """Insert random book data"""
        conn = None
        try:
            conn = self.get_connection()
            with conn.cursor() as cur:
                sql = """INSERT INTO books (title, author, publication_year, price)
                         VALUES (%s, %s, %s, %s)"""
                
                self.logger.info(f"Inserting {num_rows} random books...")
                
                for i in range(num_rows):
                    title = self.fake.sentence(nb_words=random.randint(3, 8))
                    author = self.fake.name()
                    publication_year = random.randint(1900, 2025)
                    price = round(random.uniform(100.00, 2000.00), 2)
                    
                    cur.execute(sql, (title, author, publication_year, price))
                    
                    if (i + 1) % 1000 == 0:
                        self.logger.info(f"Inserted {i + 1} records...")
                
                conn.commit()
                self.logger.info(f"Successfully inserted {num_rows} books")
                
        except Exception as e:
            if conn:
                conn.rollback()
            self.logger.error(f"Bulk insert failed: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def create_books_table(self):
        """Create books table if not exists"""
        sql = """
        CREATE TABLE IF NOT EXISTS books (
            id SERIAL PRIMARY KEY,
            title VARCHAR(255) NOT NULL,
            author VARCHAR(100) NOT NULL,
            publication_year INTEGER,
            price DECIMAL(10,2),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        
        try:
            self.execute_query(sql)
            self.logger.info("Books table created/verified")
        except Exception as e:
            self.logger.error(f"Failed to create table: {e}")
            raise
    
    def get_books(self, limit=10):
        """Get books from database"""
        sql = "SELECT * FROM books ORDER BY title LIMIT %s"
        return self.execute_query(sql, (limit,), fetch=True)