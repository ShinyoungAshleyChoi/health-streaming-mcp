"""
CORS middleware configuration
"""
from fastapi.middleware.cors import CORSMiddleware


def setup_cors(app):
    """Configure CORS middleware"""
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],  # Configure appropriately for production
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )
