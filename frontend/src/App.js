import React, { useState, useRef, useEffect } from "react";
import { v4 as uuidv4 } from 'uuid';
import "./App.css";

// --- Component for the CSV Preview Table Modal ---
function CsvPreviewModal({ isOpen, onClose, data, fileName }) {
  if (!isOpen) return null;

  return (
    <div className="modal-overlay" onClick={onClose}>
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3 title={fileName}>Preview: {fileName}</h3>
          <button className="modal-close-btn" onClick={onClose}>&times;</button>
        </div>
        <div className="preview-table-container">
          <table className="preview-table">
            <thead>
              <tr>
                {data.headers.map((header, index) => (
                  <th key={index}>{header}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {data.rows.map((row, rowIndex) => (
                <tr key={rowIndex}>
                  {row.map((cell, cellIndex) => (
                    <td key={cellIndex}>{cell}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}

// --- NEW Component for the File Staging Modal ---
function UploadStagingModal({
  isOpen,
  onClose,
  files,
  onRemoveFile,
  onPreviewFile,
  onUploadAll,
  onAddMoreFilesClick,
  loading
}) {
  if (!isOpen) return null;

  return (
    <div className="modal-overlay">
      <div className="modal-content" onClick={(e) => e.stopPropagation()}>
        <div className="modal-header">
          <h3>File Upload Queue</h3>
          <button className="modal-close-btn" onClick={onClose}>&times;</button>
        </div>
        <div className="staging-list">
          {files.length === 0 ? (
            <p>No files selected. Click "Add More Files" to begin.</p>
          ) : (
            files.map(file => (
              <div key={file.name} className="file-item">
                <span className="file-item-name" title={file.name}>{file.name}</span>
                <div className="file-item-actions">
                  <button className="action-btn preview-btn" onClick={() => onPreviewFile(file)}>Preview</button>
                  <button className="action-btn remove-btn" onClick={() => onRemoveFile(file.name)}>Remove</button>
                </div>
              </div>
            ))
          )}
        </div>
        <div className="staging-footer">
          <button className="upload-btn" onClick={onAddMoreFilesClick}>Add More Files</button>
          <button
            className="send-btn"
            onClick={onUploadAll}
            disabled={loading || files.length === 0}
          >
            {loading ? "Uploading..." : `Upload All (${files.length})`}
          </button>
        </div>
      </div>
    </div>
  );
}


function App() {
  const [messages, setMessages] = useState([
    { sender: "ai", text: "Hi! How can I help you with your claims data today?" },
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [files, setFiles] = useState([]);
  const messagesEndRef = useRef(null);
  const fileInputRef = useRef(null); // Ref for the file input
  const [conversationId, setConversationId] = useState(null);

  // State for modals
  const [isStagingOpen, setIsStagingOpen] = useState(false);
  const [isPreviewOpen, setIsPreviewOpen] = useState(false);
  const [previewData, setPreviewData] = useState({ headers: [], rows: [] });
  const [previewFileName, setPreviewFileName] = useState("");

  useEffect(() => {
    setConversationId(uuidv4());
  }, []);

  const scrollToBottom = () => {
    messagesEndRef.current?.scrollIntoView({ behavior: "smooth" });
  };

  useEffect(() => {
    scrollToBottom();
  }, [messages]);

  const handleSend = async () => {
    if (!input.trim() || !conversationId) return;
    const userMessage = { sender: "user", text: input };
    setMessages((prev) => [...prev, userMessage]);
    setInput("");
    setLoading(true);
    let aiMessage = { sender: "ai", text: "" };
    setMessages((prev) => [...prev, aiMessage]);
    try {
      const response = await fetch("http://localhost:8000/chat", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ message: input, conversation_id: conversationId }),
      });

      if (!response.body) throw new Error("No response body");
      const reader = response.body.getReader();
      let done = false;
      let fullText = "";
      while (!done) {
        const { value, done: streamDone } = await reader.read();
        done = streamDone;
        if (value) {
          const chunk = new TextDecoder().decode(value);
          chunk.split(/\n/).forEach((line) => {
            if (line.startsWith("data:")) {
              try {
                const payload = JSON.parse(line.replace("data:", "").trim());
                if (payload.chunk) {
                  fullText += payload.chunk;
                  setMessages((prev) => {
                    const updated = [...prev];
                    updated[updated.length - 1] = { sender: "ai", text: fullText };
                    return updated;
                  });
                }
              } catch { }
            }
          });
        }
      }
    } catch (err) {
      setMessages((prev) => {
        const updated = [...prev];
        updated[updated.length - 1] = { sender: "ai", text: "Sorry, something went wrong." };
        return updated;
      });
    }
    setLoading(false);
  };

  const handleInputKeyDown = (e) => {
    if (e.key === "Enter" && !loading) handleSend();
  };

  const handleFileChange = (e) => {
    const chosenFiles = Array.from(e.target.files);
    if (!chosenFiles.length) return;

    setFiles(prevFiles => {
      const newFiles = chosenFiles.filter(
        newFile => !prevFiles.some(existingFile => existingFile.name === newFile.name)
      );
      return [...prevFiles, ...newFiles];
    });
    // Open the staging modal immediately after selecting files
    setIsStagingOpen(true);
  };

  const handleRemoveFile = (fileName) => {
    setFiles(prevFiles => prevFiles.filter(file => file.name !== fileName));
  };

  const handlePreview = (fileToPreview) => {
    setPreviewFileName(fileToPreview.name);
    const reader = new FileReader();
    reader.onload = (event) => {
      const text = event.target.result;
      const lines = text.split('\n').filter(line => line.trim() !== '');
      if (lines.length > 0) {
        const headers = lines[0].split(',').map(h => h.trim());
        const rows = lines.slice(1).map(line => line.split(',').map(cell => cell.trim()));
        setPreviewData({ headers, rows });
        setIsPreviewOpen(true); // Open the preview modal
      }
    };
    reader.readAsText(fileToPreview);
  };

  const handleUploadAll = async () => {
    if (!files.length) return;
    setLoading(true);

    const formData = new FormData();
    files.forEach(file => formData.append("files", file));

    try {
      const response = await fetch("http://localhost:8000/upload-claims", {
        method: "POST",
        body: formData,
      });
      const result = await response.json();
      setMessages(prev => [...prev, { sender: "ai", text: result.message || "Files uploaded successfully!" }]);
      setFiles([]); // Clear queue
      setIsStagingOpen(false); // Close staging modal
    } catch (err) {
      setMessages(prev => [...prev, { sender: "ai", text: "File upload failed." }]);
    }
    setLoading(false);
  };

  return (
    <div className="chatbot-container">
      <UploadStagingModal
        isOpen={isStagingOpen}
        onClose={() => setIsStagingOpen(false)}
        files={files}
        onRemoveFile={handleRemoveFile}
        onPreviewFile={handlePreview}
        onUploadAll={handleUploadAll}
        onAddMoreFilesClick={() => fileInputRef.current.click()}
        loading={loading}
      />
      <CsvPreviewModal
        isOpen={isPreviewOpen}
        onClose={() => setIsPreviewOpen(false)}
        data={previewData}
        fileName={previewFileName}
      />

      <div className="chat-window">
        <div className="chat-header">Claims AI Chatbot</div>
        <div className="chat-messages">
          {messages.map((msg, idx) => (
            <div key={idx} className={`message-row ${msg.sender === "user" ? "user" : "ai"}`}>
              <div className="message-bubble">{msg.text}</div>
            </div>
          ))}
          <div ref={messagesEndRef} />
        </div>

        <div className="chat-input-area">
          <input
            type="file"
            id="file-upload"
            accept=".csv"
            multiple
            ref={fileInputRef}
            style={{ display: "none" }}
            onChange={handleFileChange}
            onClick={(e) => (e.target.value = null)}
          />
          <button
            className="upload-btn"
            onClick={() => files.length > 0 ? setIsStagingOpen(true) : fileInputRef.current.click()}
          >
            Upload Files
            {files.length > 0 && <span className="file-count-badge">{files.length}</span>}
          </button>
          <div className="text-controls">
            <input
              type="text"
              className="chat-input"
              placeholder="Type your message..."
              value={input}
              onChange={(e) => setInput(e.target.value)}
              onKeyDown={handleInputKeyDown}
              disabled={loading}
            />
            <button
              className="send-btn"
              onClick={handleSend}
              disabled={loading || !input.trim()}
            >
              Send
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

export default App;