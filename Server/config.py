# config.py
import os

class Config:
    HOST = "0.0.0.0"
    PORT = 8080
    
    #Timing
    IDLE_TIMEOUT = 60                   # seconds - close inactive sessions
    
    # Security
    MAX_SESSIONS = 50
    
    # Optional: Simple authentication token (you can enhance this)
    AUTH_TOKEN = os.getenv("PROXY_AUTH_TOKEN", "change_me_in_production")