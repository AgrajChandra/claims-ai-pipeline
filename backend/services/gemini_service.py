import os
import google.generativeai as genai
import logging
from typing import AsyncGenerator, List, Dict, Any
import asyncio
import numpy as np
import faiss
from sentence_transformers import SentenceTransformer
import re # Import regular expressions

logger = logging.getLogger(__name__)

class GeminiService:
    def __init__(self, db, distance_threshold: float = 1.0):
        self.db = db
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise RuntimeError("GEMINI_API_KEY not found in .env")
        genai.configure(api_key=api_key)
        self.model = genai.GenerativeModel("gemini-1.5-flash")

        # RAG components
        self.embedding_model = SentenceTransformer('all-MiniLM-L6-v2')
        self.rag_index = None
        self.rag_document_metadata: List[Dict[str, Any]] = []
        self.rag_document_texts: List[str] = []
        self.distance_threshold = distance_threshold

    async def _build_rag_index(self):
        """
        Builds the RAG index (no changes to this method).
        """
        try:
            logger.info("First request received. Building RAG index...")
            
            claims = await self.db.execute_query(
                "SELECT claim_id, claim_type, claim_status, diagnosis_code FROM claims LIMIT 5000"
            )
            if not claims:
                logger.warning("No claims data found to build RAG index.")
                return

            self.rag_document_metadata.clear()
            self.rag_document_texts.clear()
            
            for claim in claims:
                if not claim.get('claim_id'):
                    continue
                
                self.rag_document_metadata.append(claim)
                text_representation = (
                    f"Claim ID: {claim.get('claim_id')}\n"
                    f"Claim Type: {claim.get('claim_type')}\n"
                    f"Status: {claim.get('claim_status')}\n"
                    f"Diagnosis Code: {claim.get('diagnosis_code')}"
                )
                self.rag_document_texts.append(text_representation)

            if not self.rag_document_texts:
                logger.warning("No valid documents to index after processing.")
                return

            logger.info(f"Creating embeddings for {len(self.rag_document_texts)} documents...")
            embeddings = self.embedding_model.encode(self.rag_document_texts, convert_to_tensor=False, show_progress_bar=True)
            
            dimension = embeddings.shape[1]
            self.rag_index = faiss.IndexFlatL2(dimension)
            self.rag_index.add(np.array(embeddings, dtype=np.float32))
            
            logger.info(f"RAG index with {self.rag_index.ntotal} vectors built successfully.")

        except Exception as e:
            logger.error(f"Failed to build RAG index: {e}", exc_info=True)
            self.rag_index = None

    async def _search_rag_index(self, query: str, k: int = 5) -> Dict[str, Any]:
        """
        Performs a semantic search using the RAG index (no changes to this method).
        """
        if self.rag_index is None or not self.rag_document_metadata:
            logger.warning("RAG index not available. Triggering fallback.")
            return {"results": [], "fallback_needed": True}
        
        try:
            query_embedding = self.embedding_model.encode([query])
            distances, indices = self.rag_index.search(np.array(query_embedding, dtype=np.float32), k)
            
            if len(indices[0]) == 0 or distances[0][0] > self.distance_threshold:
                logger.warning(f"No confident semantic match found. Best distance: {distances[0][0] if len(distances[0]) > 0 else 'N/A'}.")
                return {"results": [], "fallback_needed": True}
            
            results = [self.rag_document_metadata[i] for i in indices[0]]
            return {"results": results, "fallback_needed": False}
            
        except Exception as e:
            logger.error(f"Error during RAG search: {e}", exc_info=True)
            return {"results": [], "fallback_needed": True}

    async def stream_response(self, user_message: str, conversation_id: str = None) -> AsyncGenerator[str, None]:
        """
        Main method to generate a response, now with intent detection logic.
        """
        try:
            # Lazy load the RAG index if it hasn't been built yet.
            if self.rag_index is None:
                await self._build_rag_index()

            context = ""
            user_message_lower = user_message.lower()

            # --- HYBRID APPROACH: INTENT DETECTION ---

            # 1. Intent: Specific ID Lookup (using regular expressions)
            # This pattern looks for "CLM-" followed by alphanumeric characters.
            claim_id_match = re.search(r'clm-[a-z0-9-]+', user_message_lower)
            if claim_id_match:
                claim_id = claim_id_match.group(0).upper()
                logger.info(f"Intent detected: ID Lookup for '{claim_id}'")
                # Use a precise SQL query for exact matches
                result = await self.db.execute_query(
                    "SELECT * FROM claims WHERE claim_id = %s", [claim_id]
                )
                if result:
                    context = f"Here is the data found for claim ID {claim_id}:\n{result[0]}"
                else:
                    context = f"I could not find any data for the claim ID {claim_id}."

            # 2. Intent: Summary/Count Question
            elif any(keyword in user_message_lower for keyword in ["how many", "count", "total number"]):
                logger.info("Intent detected: Summary/Count Question")
                # For this simple case, we do a general count. This can be expanded.
                # Example: to handle "how many pending", you would extract 'pending'
                # and add a WHERE clause.
                result = await self.db.execute_query("SELECT COUNT(*) as total FROM claims")
                total_claims = result[0]['total'] if result else 0
                context = f"The total number of claims in the database is {total_claims}."

            # 3. Default Intent: Semantic Search (using RAG)
            else:
                logger.info("Intent detected: Semantic Search")
                search_response = await self._search_rag_index(user_message, k=3)
                if not search_response["fallback_needed"]:
                    context = "Based on the following relevant claims data:\n"
                    for doc in search_response["results"]:
                        context += f"- {str(doc)}\n"
                else:
                    context = "No specific claims data was found in the database that matches your query."

            # --- Prompt and Stream to Gemini ---
            
            prompt = f"""You are a helpful assistant for analyzing insurance claims.
            
            Context:
            {context}
            
            Based ONLY on the context provided, please answer the user's question.
            User: {user_message}
            """

            response = self.model.start_chat(history=[])
            stream = response.send_message(prompt, stream=True)

            for chunk in stream:
                if chunk.text:
                    yield chunk.text
                    await asyncio.sleep(0)
                    
        except Exception as e:
            logger.error(f"Gemini RAG error: {e}", exc_info=True)
            yield f"⚠️ Error processing your request: {e}"