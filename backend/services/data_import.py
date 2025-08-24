import csv
import io
import asyncio
import logging
from typing import List, Dict, Any
from datetime import datetime
from models import Claim, DatabaseStats, DataImportResult

logger = logging.getLogger(__name__)

class DataImportService:
    """Service for importing and managing claims data"""
    
    def __init__(self, database):
        self.db = database
    
    async def import_csv_data(self, csv_data: str, filename: str) -> int:
        """Import CSV data into the database"""
        try:
            # Parse CSV data
            csv_file = io.StringIO(csv_data)
            reader = csv.DictReader(csv_file)
            
            claims = []
            processed_count = 0
            error_count = 0
            
            # Process CSV rows
            for row_num, row in enumerate(reader, 1):
                try:
                    # Clean column names (remove whitespace, lowercase)
                    clean_row = {k.strip().lower(): v for k, v in row.items()}
                    
                    # Create claim object
                    claim = Claim.from_csv_row(clean_row, filename)
                    
                    if claim.is_valid():
                        claims.append(claim)
                        processed_count += 1
                    else:
                        logger.warning(f"Invalid claim in row {row_num}: missing claim_id")
                        error_count += 1
                        
                    # Process in batches of 1000 for memory efficiency
                    if len(claims) >= 1000:
                        await self._batch_insert_claims(claims)
                        claims = []
                        
                except Exception as e:
                    logger.error(f"Error processing row {row_num}: {e}")
                    error_count += 1
                    continue
            
            # Insert remaining claims
            if claims:
                await self._batch_insert_claims(claims)
            
            logger.info(f"Import completed: {processed_count} processed, {error_count} errors")
            return processed_count
            
        except Exception as e:
            logger.error(f"Error importing CSV data: {e}")
            raise
    
    async def _batch_insert_claims(self, claims: List[Claim]) -> int:
        """Batch insert claims into database"""
        insert_query = """
        INSERT INTO claims (
            claim_id, policy_number, claim_date, claim_amount, claim_status,
            claim_type, settlement_amount, processing_days, diagnosis_code,
            provider_id, file_source
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (claim_id) DO NOTHING
        """
        
        params_list = [claim.to_db_params() for claim in claims]
        return await self.db.execute_batch_insert(insert_query, params_list)
    
    async def get_database_stats(self) -> Dict[str, Any]:
        """Get comprehensive database statistics"""
        try:
            summary = await self.db.get_claims_summary()
            
            # Format the response
            stats = {
                "total_claims": summary.get('total_claims', {}).get('count', 0),
                "total_amount": float(summary.get('total_amount', {}).get('sum', 0) or 0),
                "avg_amount": float(summary.get('avg_amount', {}).get('avg', 0) or 0),
                "avg_processing_days": float(summary.get('avg_processing_days', {}).get('avg', 0) or 0),
                "recent_claims_30_days": summary.get('recent_claims', {}).get('count', 0),
                "status_breakdown": summary.get('status_breakdown', []),
                "type_breakdown": summary.get('type_breakdown', []),
                "last_updated": datetime.now().isoformat()
            }
            
            return stats
            
        except Exception as e:
            logger.error(f"Error getting database stats: {e}")
            raise
    
    async def clear_all_data(self):
        """Clear all claims data from the database"""
        try:
            await self.db.execute_query("DELETE FROM messages")
            await self.db.execute_query("DELETE FROM conversations")  
            await self.db.execute_query("DELETE FROM claims")
            logger.info("All data cleared successfully")
        except Exception as e:
            logger.error(f"Error clearing data: {e}")
            raise
    
    async def get_claims_by_filter(self, filters: Dict[str, Any], limit: int = 100) -> List[Dict[str, Any]]:
        """Get claims based on filters"""
        try:
            where_conditions = []
            params = []
            param_count = 1
            
            if filters.get('claim_status'):
                where_conditions.append(f"claim_status = ${param_count}")
                params.append(filters['claim_status'])
                param_count += 1
            
            if filters.get('claim_type'):
                where_conditions.append(f"claim_type = ${param_count}")
                params.append(filters['claim_type'])
                param_count += 1
            
            if filters.get('min_amount'):
                where_conditions.append(f"claim_amount >= ${param_count}")
                params.append(filters['min_amount'])
                param_count += 1
            
            if filters.get('max_amount'):
                where_conditions.append(f"claim_amount <= ${param_count}")
                params.append(filters['max_amount'])
                param_count += 1
            
            if filters.get('start_date'):
                where_conditions.append(f"claim_date >= ${param_count}")
                params.append(filters['start_date'])
                param_count += 1
            
            if filters.get('end_date'):
                where_conditions.append(f"claim_date <= ${param_count}")
                params.append(filters['end_date'])
                param_count += 1
            
            where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"""
            SELECT * FROM claims 
            {where_clause}
            ORDER BY claim_date DESC 
            LIMIT {limit}
            """
            
            # Convert PostgreSQL parameter format
            pg_query = query
            for i in range(len(params)):
                pg_query = pg_query.replace(f"${i+1}", "%s")
            
            return await self.db.execute_query(pg_query, params)
            
        except Exception as e:
            logger.error(f"Error filtering claims: {e}")
            raise
    
    async def get_aggregated_data(self, group_by: str, filters: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """Get aggregated claims data"""
        try:
            valid_group_fields = ['claim_status', 'claim_type', 'provider_id', 'diagnosis_code']
            if group_by not in valid_group_fields:
                raise ValueError(f"Invalid group_by field. Must be one of: {valid_group_fields}")
            
            where_conditions = []
            params = []
            
            if filters:
                param_count = 1
                if filters.get('start_date'):
                    where_conditions.append(f"claim_date >= %s")
                    params.append(filters['start_date'])
                
                if filters.get('end_date'):
                    where_conditions.append(f"claim_date <= %s")
                    params.append(filters['end_date'])
            
            where_clause = " WHERE " + " AND ".join(where_conditions) if where_conditions else ""
            
            query = f"""
            SELECT 
                {group_by},
                COUNT(*) as count,
                SUM(claim_amount) as total_amount,
                AVG(claim_amount) as avg_amount,
                AVG(processing_days) as avg_processing_days
            FROM claims 
            {where_clause}
            GROUP BY {group_by}
            ORDER BY count DESC
            LIMIT 20
            """
            
            return await self.db.execute_query(query, params)
            
        except Exception as e:
            logger.error(f"Error getting aggregated data: {e}")
            raise