import numpy as np
import faiss
from sentence_transformers import SentenceTransformer
import logging
from typing import List, Dict, Any
import asyncio

# --- Basic Logging Setup ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# ----------------------------------------------------------------------------
# 1. THE IMPROVED RAG SERVICE
# ----------------------------------------------------------------------------
class RAGService:
    """
    Handles the creation of a vector index for Retrieval-Augmented Generation (RAG)
    and provides a search functionality with a confidence-based fallback.
    """
    def __init__(self, database, distance_threshold: float = 1.0):
        """
        Initializes the RAG Service.
        
        Args:
            database: An asynchronous database connection object with an `execute_query` method.
            distance_threshold (float): The L2 distance threshold for search results.
                                        If the best match's distance is above this, we trigger a
                                        fallback. 1.0 is a reasonable default for normalized embeddings.
        """
        self.db = database
        self.model = SentenceTransformer('all-MiniLM-L6-v2')
        self.index = None
        
        # We store the original structured data (metadata) and the text used for embedding separately.
        self.documents_metadata: List[Dict[str, Any]] = []
        self.document_texts: List[str] = []
        
        # A configurable distance threshold for the fallback logic.
        self.distance_threshold = distance_threshold

    async def build_index(self):
        """
        Fetches claims data, creates structured text for embeddings, and builds the FAISS index.
        """
        try:
            logger.info("Starting to build RAG index...")
            
            # Fetch data from the database
            claims = await self.db.execute_query(
                "SELECT claim_id, claim_type, claim_status, diagnosis_code FROM claims LIMIT 5000"
            )

            if not claims:
                logger.warning("No claims data found to build RAG index.")
                return

            # Clear old data before rebuilding
            self.documents_metadata.clear()
            self.document_texts.clear()

            # Create a structured text chunk for each document for better embeddings.
            for claim in claims:
                if not claim.get('claim_id'):
                    continue
                
                # Store the original, clean data dictionary
                self.documents_metadata.append(claim)
                
                # Create a better text representation for the embedding model
                text_representation = (
                    f"Claim ID: {claim.get('claim_id')}\n"
                    f"Claim Type: {claim.get('claim_type')}\n"
                    f"Status: {claim.get('claim_status')}\n"
                    f"Diagnosis Code: {claim.get('diagnosis_code')}"
                )
                self.document_texts.append(text_representation)

            if not self.document_texts:
                logger.warning("No valid documents to index after processing.")
                return

            logger.info(f"Creating embeddings for {len(self.document_texts)} documents...")
            embeddings = self.model.encode(self.document_texts, convert_to_tensor=False, show_progress_bar=True)

            # Build the FAISS index
            dimension = embeddings.shape[1]
            self.index = faiss.IndexFlatL2(dimension) # Using L2 (Euclidean) distance
            self.index.add(np.array(embeddings, dtype=np.float32))

            logger.info(f"RAG index with {self.index.ntotal} vectors built successfully.")
        except Exception as e:
            logger.error(f"Failed to build RAG index: {e}", exc_info=True)
            self.index = None # Ensure index is None on failure

    async def search(self, query: str, k: int = 5) -> Dict[str, Any]:
        """
        Searches the FAISS index and determines if a fallback to an LLM is needed.

        Args:
            query (str): The user's search query.
            k (int): The number of top results to retrieve.

        Returns:
            A dictionary containing:
            - 'results' (List[Dict]): A list of the retrieved documents (metadata).
            - 'fallback_needed' (bool): True if no confident match was found.
        """
        if self.index is None or not self.documents_metadata:
            logger.warning("RAG index not available. Triggering fallback.")
            return {"results": [], "fallback_needed": True}

        try:
            query_embedding = self.model.encode([query])
            distances, indices = self.index.search(np.array(query_embedding, dtype=np.float32), k)

            # Fallback logic: trigger if no results or if the best match is not good enough.
            if len(indices[0]) == 0 or distances[0][0] > self.distance_threshold:
                logger.warning(f"No confident match found. Best distance: {distances[0][0] if len(distances[0]) > 0 else 'N/A'}. Triggering fallback.")
                return {"results": [], "fallback_needed": True}

            # If the match is good, return the original, structured metadata.
            results = [self.documents_metadata[i] for i in indices[0]]
            return {"results": results, "fallback_needed": False}
        
        except Exception as e:
            logger.error(f"Error during RAG search: {e}", exc_info=True)
            return {"results": [], "fallback_needed": True}

# ----------------------------------------------------------------------------
# 2. MOCK COMPONENTS FOR DEMONSTRATION
# ----------------------------------------------------------------------------
class MockDB:
    """A mock database class to simulate database interactions for the example."""
    async def execute_query(self, query: str):
        logger.info(f"MockDB executing query: {query}")
        return [
            {'claim_id': 101, 'claim_type': 'Medical', 'claim_status': 'Approved', 'diagnosis_code': 'J45.909'},
            {'claim_id': 102, 'claim_type': 'Dental', 'claim_status': 'Pending', 'diagnosis_code': 'K02.51'},
            {'claim_id': 103, 'claim_type': 'Vision', 'claim_status': 'Rejected', 'diagnosis_code': 'H52.1'},
            {'claim_id': 104, 'claim_type': 'Medical', 'claim_status': 'Pending', 'diagnosis_code': 'M54.5'},
        ]

class MockLLMService:
    """A mock LLM service to simulate generating an answer."""
    def ask(self, query: str, context: str = "") -> str:
        print("\n--- 📞 Fallback to LLM ---")
        if context:
            print(f"LLM is answering the query '{query}' with the following context:")
            print(context)
        else:
            print(f"LLM is answering the query '{query}' with no context from the database.")
        
        # In a real application, this would be an API call to a service like Gemini
        return f"This is a generated answer from the LLM for the query: '{query}'"

# ----------------------------------------------------------------------------
# 3. EXAMPLE USAGE LOGIC
# ----------------------------------------------------------------------------
async def handle_user_query(rag_service: RAGService, llm_service: MockLLMService, user_query: str):
    """
    Demonstrates the full workflow: search, check for fallback, and get a final answer.
    """
    print(f"\n=================================================")
    print(f"🕵️  Handling new query: '{user_query}'")
    print(f"=================================================")
    
    # 1. Search for relevant documents using the RAG service
    search_response = await rag_service.search(user_query)
    
    # 2. Check the response to see if a fallback is needed
    if search_response["fallback_needed"]:
        print("✅ RAG search did not find a confident result. Using LLM fallback.")
        # Fallback to the LLM without any specific context
        final_answer = llm_service.ask(query=user_query)
    else:
        print("✅ RAG search found relevant documents.")
        # Provide the retrieved documents as context to the LLM
        retrieved_docs = search_response["results"]
        context_for_llm = "\n---\n".join([str(doc) for doc in retrieved_docs])
        
        final_answer = llm_service.ask(query=user_query, context=context_for_llm)
        
    print(f"\n💡 Final Answer: {final_answer}")

async def main():
    """Main function to run the demonstration."""
    # Initialize the mock services and the RAG service
    db = MockDB()
    llm = MockLLMService()
    rag = RAGService(database=db, distance_threshold=1.0)

    # Build the vector index on startup
    await rag.build_index()

    # --- Run Example Queries ---

    # Query that should match well with the data in MockDB
    await handle_user_query(rag, llm, "show me pending medical claims")
    
    # Query that is unrelated to the data and should trigger the fallback
    await handle_user_query(rag, llm, "what is the company's policy on remote work?")
    
    # Another query that should match well
    await handle_user_query(rag, llm, "information on rejected vision claims")


if __name__ == "__main__":
    asyncio.run(main())