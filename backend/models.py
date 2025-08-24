from datetime import datetime, date
from decimal import Decimal
from typing import Optional, List, Dict, Any
from dataclasses import dataclass

@dataclass
class Claim:
    """Data model for insurance claim"""
    claim_id: str
    policy_number: Optional[str] = None
    claim_date: Optional[date] = None
    claim_amount: Optional[Decimal] = None
    claim_status: Optional[str] = None
    claim_type: Optional[str] = None
    settlement_amount: Optional[Decimal] = None
    processing_days: Optional[int] = None
    diagnosis_code: Optional[str] = None
    provider_id: Optional[str] = None
    file_source: Optional[str] = None
    created_at: Optional[datetime] = None
    updated_at: Optional[datetime] = None
    
    @classmethod
    def from_csv_row(cls, row: Dict[str, str], file_source: str) -> 'Claim':
        """Create Claim instance from CSV row data"""
        def safe_decimal(value: str) -> Optional[Decimal]:
            try:
                return Decimal(str(value).strip()) if value and str(value).strip() else None
            except:
                return None
        
        def safe_int(value: str) -> Optional[int]:
            try:
                return int(str(value).strip()) if value and str(value).strip() else None
            except:
                return None
        
        def safe_date(value: str) -> Optional[date]:
            try:
                if not value or not str(value).strip():
                    return None
                date_str = str(value).strip()
                # Try multiple date formats
                for fmt in ['%Y-%m-%d', '%m/%d/%Y', '%d/%m/%Y', '%Y/%m/%d']:
                    try:
                        return datetime.strptime(date_str, fmt).date()
                    except ValueError:
                        continue
                return None
            except:
                return None
        
        return cls(
            claim_id=str(row.get('claim_id', '')).strip(),
            policy_number=str(row.get('policy_number', '')).strip() or None,
            claim_date=safe_date(row.get('claim_date')),
            claim_amount=safe_decimal(row.get('claim_amount')),
            claim_status=str(row.get('claim_status', '')).strip() or None,
            claim_type=str(row.get('claim_type', '')).strip() or None,
            settlement_amount=safe_decimal(row.get('settlement_amount')),
            processing_days=safe_int(row.get('processing_days')),
            diagnosis_code=str(row.get('diagnosis_code', '')).strip() or None,
            provider_id=str(row.get('provider_id', '')).strip() or None,
            file_source=file_source
        )
    
    def to_db_params(self) -> List[Any]:
        """Convert to database parameters for insertion"""
        return [
            self.claim_id,
            self.policy_number,
            self.claim_date,
            self.claim_amount,
            self.claim_status,
            self.claim_type,
            self.settlement_amount,
            self.processing_days,
            self.diagnosis_code,
            self.provider_id,
            self.file_source
        ]
    
    def is_valid(self) -> bool:
        """Check if claim has minimum required data"""
        return bool(self.claim_id and self.claim_id.strip())

@dataclass
class Conversation:
    """Data model for conversation"""
    conversation_id: str
    created_at: Optional[datetime] = None

@dataclass
class Message:
    """Data model for chat message"""
    conversation_id: str
    message_type: str  # 'user' or 'assistant'
    content: str
    created_at: Optional[datetime] = None

@dataclass
class DatabaseStats:
    """Data model for database statistics"""
    total_claims: int
    total_amount: Optional[Decimal]
    avg_amount: Optional[Decimal]
    avg_processing_days: Optional[float]
    recent_claims_30_days: int
    status_breakdown: List[Dict[str, Any]]
    type_breakdown: List[Dict[str, Any]]
    last_updated: datetime

@dataclass
class DataImportResult:
    """Result of data import operation"""
    filename: str
    records_imported: int
    records_skipped: int
    errors: List[str]
    status: str  # 'success', 'partial', 'failed'