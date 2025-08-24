from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import asyncio
import json
from typing import List, Optional
import csv
import io

from database import Database
from services.data_import import DataImportService
from services.gemini_service import GeminiService
from schemas import ChatMessage, DataImportResponse

app = FastAPI(title="Claims AI Pipeline", version="1.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db = Database()
data_import_service = DataImportService(db)
llm_service = GeminiService(db)

@app.on_event("startup")
async def startup_event():
    await db.connect()

@app.on_event("shutdown")
async def shutdown_event():
    await db.disconnect()

@app.get("/")
async def root():
    return {"message": "Claims AI Pipeline API", "status": "running"}

@app.post("/upload-claims", response_model=DataImportResponse)
async def upload_claims(files: List[UploadFile] = File(...)):
    try:
        results = []
        total_records = 0
        
        for file in files:
            if not file.filename.endswith('.csv'):
                raise HTTPException(status_code=400, detail=f"File {file.filename} is not a CSV file")
            
            content = await file.read()
            csv_data = content.decode('utf-8')
            
            records_imported = await data_import_service.import_csv_data(csv_data, file.filename)
            
            results.append({
                "filename": file.filename,
                "records_imported": records_imported,
                "status": "success"
            })
            total_records += records_imported
        
        return DataImportResponse(
            success=True,
            message=f"Successfully imported {total_records} records from {len(files)} files.",
            files_processed=results,
            total_records=total_records
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error processing files: {str(e)}")

@app.post("/chat")
async def chat_with_ai(message: ChatMessage):
    try:
        async def generate_response():
            async for chunk in llm_service.stream_response(message.message, message.conversation_id):
                yield f"data: {json.dumps({'chunk': chunk})}\n\n"
            yield f"data: {json.dumps({'done': True})}\n\n"
        
        return StreamingResponse(
            generate_response(),
            media_type="text/event-stream"
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error in chat: {str(e)}")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)