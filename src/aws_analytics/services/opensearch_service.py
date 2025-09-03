from opensearchpy import OpenSearch, helpers
from ..config import Settings
from ..utils import get_logger

class OpenSearchService:
    """OpenSearch service for indexing and search operations"""
    
    def __init__(self):
        self.logger = get_logger(__name__)
        self.client = self._create_client()
        
    def _create_client(self):
        """Create OpenSearch client"""
        if not all([Settings.OPENSEARCH_ENDPOINT, Settings.OPENSEARCH_USER, Settings.OPENSEARCH_PWD]):
            raise ValueError("OpenSearch credentials not configured")
            
        return OpenSearch(
            hosts=[{'host': Settings.OPENSEARCH_ENDPOINT, 'port': 443}],
            http_auth=(Settings.OPENSEARCH_USER, Settings.OPENSEARCH_PWD),
            use_ssl=True,
            verify_certs=False
        )
    
    def index_document(self, index_name, document, doc_id=None):
        """Index single document"""
        try:
            response = self.client.index(
                index=index_name,
                body=document,
                id=doc_id
            )
            self.logger.info(f"Document indexed: {response['_id']}")
            return response
        except Exception as e:
            self.logger.error(f"Failed to index document: {e}")
            raise
    
    def bulk_index(self, index_name, documents):
        """Bulk index documents"""
        actions = [
            {
                "_index": index_name,
                "_source": doc,
                "_id": doc.get('id')
            } for doc in documents
        ]
        
        try:
            response = helpers.bulk(self.client, actions)
            self.logger.info(f"Bulk indexed {len(documents)} documents")
            return response
        except Exception as e:
            self.logger.error(f"Bulk indexing failed: {e}")
            raise
    
    def search(self, index_name, query, size=10):
        """Search documents"""
        try:
            response = self.client.search(
                index=index_name,
                body=query,
                size=size
            )
            return response
        except Exception as e:
            self.logger.error(f"Search failed: {e}")
            raise
    
    def create_index(self, index_name, mapping=None):
        """Create index with optional mapping"""
        try:
            body = {"mappings": mapping} if mapping else {}
            response = self.client.indices.create(index=index_name, body=body)
            self.logger.info(f"Index created: {index_name}")
            return response
        except Exception as e:
            self.logger.error(f"Failed to create index: {e}")
            raise