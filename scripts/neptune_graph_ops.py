#!/usr/bin/env python3
"""Neptune Graph Operations Script"""

import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from aws_analytics.services import NeptuneService
from aws_analytics.utils import get_logger

def main():
    logger = get_logger(__name__)
    
    endpoint = input("Neptune endpoint (without wss://): ").strip()
    if not endpoint:
        logger.error("Neptune endpoint is required")
        sys.exit(1)
    
    try:
        with NeptuneService(endpoint) as neptune:
            # Get counts
            vertex_count = neptune.vertex_count()
            edge_count = neptune.edge_count()
            
            logger.info(f"Current graph stats - Vertices: {vertex_count}, Edges: {edge_count}")
            
            # Add sample data
            choice = input("Add sample data? (y/n): ").strip().lower()
            if choice == 'y':
                # Add vertices
                person1 = neptune.add_vertex('person', {'name': 'Alice', 'age': 30})
                person2 = neptune.add_vertex('person', {'name': 'Bob', 'age': 25})
                company = neptune.add_vertex('company', {'name': 'TechCorp'})
                
                # Add edges
                neptune.add_edge(person1.id, company.id, 'works_at', {'since': 2020})
                neptune.add_edge(person2.id, company.id, 'works_at', {'since': 2021})
                neptune.add_edge(person1.id, person2.id, 'knows', {'relationship': 'colleague'})
                
                logger.info("Sample data added successfully")
                
                # Query data
                persons = neptune.find_vertices_by_label('person')
                logger.info(f"Found {len(persons)} person vertices")
                
    except Exception as e:
        logger.error(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()