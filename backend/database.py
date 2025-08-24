import pg8000
import asyncio
import os
from typing import List, Dict, Any, Optional
from datetime import datetime
import logging

from dotenv import load_dotenv
load_dotenv()


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class Database:
    def __init__(self):
        self.connection = None
        self.db_config = {
            'host': os.getenv('DB_HOST', 'localhost'),
            'port': int(os.getenv('DB_PORT', '5432')),
            'database': os.getenv('DB_NAME', 'claimsdb'),
            'user': os.getenv('DB_USER', 'claimsuser'),
            'password': os.getenv('DB_PASSWORD', ''),
        }
    
    async def connect(self):
        """Establish database connection"""
        try:
            self.connection = pg8000.connect(**self.db_config)
            self.connection.autocommit = True
            logger.info("Database connection established")
            await self.create_tables()
        except Exception as e:
            logger.error(f"Database connection failed: {e}")
            raise
    
    async def disconnect(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    async def create_tables(self):
        """Create necessary tables if they don't exist"""
        create_claims_table = """
        CREATE TABLE IF NOT EXISTS claims (
            id SERIAL PRIMARY KEY,
            claim_id VARCHAR(100) UNIQUE NOT NULL,
            policy_number VARCHAR(100),
            claim_date DATE,
            claim_amount DECIMAL(15, 2),
            claim_status VARCHAR(50),
            claim_type VARCHAR(100),
            settlement_amount DECIMAL(15, 2),
            processing_days INTEGER,
            diagnosis_code VARCHAR(20),
            provider_id VARCHAR(100),
            file_source VARCHAR(255),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        create_conversations_table = """
        CREATE TABLE IF NOT EXISTS conversations (
            id SERIAL PRIMARY KEY,
            conversation_id VARCHAR(100) UNIQUE NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        create_messages_table = """
        CREATE TABLE IF NOT EXISTS messages (
            id SERIAL PRIMARY KEY,
            conversation_id VARCHAR(100) REFERENCES conversations(conversation_id),
            message_type VARCHAR(20) NOT NULL, -- 'user' or 'assistant'
            content TEXT NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        """
        
        create_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_claims_date ON claims(claim_date);",
            "CREATE INDEX IF NOT EXISTS idx_claims_status ON claims(claim_status);",
            "CREATE INDEX IF NOT EXISTS idx_claims_type ON claims(claim_type);",
        ]
        
        try:
            cursor = self.connection.cursor()
            cursor.execute(create_claims_table)
            cursor.execute(create_conversations_table)
            cursor.execute(create_messages_table)
            
            for index_sql in create_indexes:
                cursor.execute(index_sql)
            
            cursor.close()
            logger.info("Tables and indexes created successfully")
            
        except Exception as e:
            logger.error(f"Error creating tables: {e}")
            raise
    
    async def execute_query(self, query: str, params: Optional[List] = None) -> List[Dict[str, Any]]:
        """Execute a SELECT query and return results as list of dictionaries"""
        try:
            cursor = self.connection.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            columns = [desc[0] for desc in cursor.description]
            results = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            cursor.close()
            return results
            
        except Exception as e:
            logger.error(f"Error executing query: {e}")
            raise

    async def execute_batch_insert(self, query: str, params_list: List[List[Any]]) -> int:
        """Execute batch INSERT queries"""
        try:
            cursor = self.connection.cursor()
            total_affected = 0
            for params in params_list:
                try:
                    cursor.execute(query, params)
                    total_affected += cursor.rowcount
                except Exception as e:
                    logger.warning(f"Skipping record due to error: {e}")
                    self.connection.rollback() # Rollback the single failed transaction
                    continue
            cursor.close()
            return total_affected
        except Exception as e:
            logger.error(f"Error executing batch insert: {e}")
            raise