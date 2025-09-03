import json
import threading
import time
from kafka import KafkaProducer
from ..config import Settings
from ..utils import get_logger

class MSKService:
    """MSK service for Kafka operations"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.bootstrap_servers = Settings.MSK_BOOTSTRAP_SERVERS
        self.topic = Settings.MSK_TOPIC
        
    def create_producer(self, **kwargs):
        """Create Kafka producer with default config"""
        config = {
            'bootstrap_servers': self.bootstrap_servers,
            'value_serializer': lambda v: json.dumps(v).encode('utf-8'),
            'batch_size': 32768,
            'linger_ms': 5,
            'compression_type': 'gzip',
            'acks': 'all'
        }
        config.update(kwargs)
        
        return KafkaProducer(**config)
    
    def send_message(self, message, topic=None):
        """Send single message"""
        producer = self.create_producer()
        try:
            future = producer.send(topic or self.topic, value=message)
            producer.flush()
            return future.get(timeout=10)
        finally:
            producer.close()
    
    def generate_load(self, num_threads=3, messages_per_sec=200, duration=None):
        """Generate load for testing"""
        self.logger.info(f"Starting load test: {num_threads} threads, {messages_per_sec} msg/sec")
        
        def worker(thread_id):
            producer = self.create_producer()
            count = 0
            start_time = time.time()
            
            while True:
                if duration and (time.time() - start_time) > duration:
                    break
                    
                message = {
                    'timestamp': time.time(),
                    'thread_id': thread_id,
                    'count': count
                }
                
                producer.send(self.topic, value=message)
                count += 1
                
                if count % 100 == 0:
                    producer.flush()
                    self.logger.info(f"Thread {thread_id}: {count} messages")
                
                time.sleep(1.0 / messages_per_sec)
            
            producer.close()
        
        threads = []
        for i in range(num_threads):
            thread = threading.Thread(target=worker, args=(i,))
            thread.start()
            threads.append(thread)
        
        return threads