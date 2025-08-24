from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
from datetime import datetime
from decimal import Decimal

class ClaimData(BaseModel):
    """Schema for claim data validation"""
    claim_id: str
    policy_number: Optional[str] = None
    claim_date: Optional[str] = None
    claim_amount: Optional[Decimal] = None
    claim_status: Optional[str] = None
    claim_type: Optional[str] = None
    settlement_amount: Optional[Decimal] = None
    processing_days: Optional[int] = None
    diagnosis_code: Optional[str] = None
    provider_id: Optional[str] = None

class ChatMessage(BaseModel):
    """Schema for chat messages"""
    message: str = Field(..., min_length=1, max_length=1000)
    conversation_id: Optional[str] = None

class FileUploadResponse(BaseModel):
    """Schema for file upload response"""
    filename: str
    records_imported: int
    status: str
    errors: Optional[List[str]] = None

class DataImportResponse(BaseModel):
    """Schema for data import response"""
    success: bool
    message: str
    files_processed: List[Dict[str, Any]]
    total_records: int

class DatabaseStatsResponse(BaseModel):
    """Schema for database statistics response"""
    total_claims: int
    total_amount: Optional[Decimal] = None
    avg_amount: Optional[Decimal] = None
    avg_processing_days: Optional[float] = None
    recent_claims_30_days: int
    status_breakdown: List[Dict[str, Any]]
    type_breakdown: List[Dict[str, Any]]
    last_updated: datetime

class ConversationHistory(BaseModel):
    """Schema for conversation history"""
    conversation_id: str
    history: List[Dict[str, Any]]

class StreamingChatResponse(BaseModel):
    """Schema for streaming chat response"""
    chunk: Optional[str] = None
    done: bool = False
    conversation_id: Optional[str] = None

class ErrorResponse(BaseModel):
    """Schema for error responses"""
    error: bool = True
    message: str
    details: Optional[Dict[str, Any]] = None