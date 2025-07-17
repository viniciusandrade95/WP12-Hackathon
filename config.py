import os
from pathlib import Path

class Config:
    # API Configuration
    API_KEY = os.getenv('API_KEY', 'sk-47e33d0d83f64299949c78b961956144')
    BASE_URL = os.getenv('BASE_URL', 'https://llm.lab.sspcloud.fr/api/chat/completions')
    
    # File Upload Configuration
    UPLOAD_FOLDER = os.path.join(Path(__file__).parent, 'static', 'uploads')
    MAX_CONTENT_LENGTH = 100 * 1024 * 1024  # 100MB max file size
    
    # Database Configuration
    DATABASE_PATH = os.path.join(Path(__file__).parent, 'intelligent_rag.db')
    
    # Processing Configuration
    MAX_PROCESSING_TIME = 900  # 15 minutes
    MAX_PAGES_TO_PROCESS = 20
    
    # Security
    SECRET_KEY = os.getenv('SECRET_KEY', 'your-secret-key-here')
    
    def __init__(self):
        # Ensure upload directory exists
        os.makedirs(self.UPLOAD_FOLDER, exist_ok=True)