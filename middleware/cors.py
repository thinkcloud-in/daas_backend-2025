from fastapi.middleware.cors import CORSMiddleware
import os
 
def cors(app):
    allowed_origins = os.getenv("CORS_ALLOWED_ORIGINS", "*").split(",")
    allowed_methods = os.getenv("CORS_ALLOWED_METHODS", "GET,POST,PUT,DELETE,OPTIONS,PATCH").split(",")
    allowed_headers = os.getenv("CORS_ALLOWED_HEADERS", "Content-Type,Authorization,X-Requested-With").split(",")
    allow_credentials = os.getenv("CORS_ALLOW_CREDENTIALS", "true").lower() == "true"
    max_age = int(os.getenv("CORS_MAX_AGE", 3600))
    app.add_middleware(
        CORSMiddleware,
        allow_origins=allowed_origins,
        allow_methods=allowed_methods,
        allow_headers=allowed_headers,
        allow_credentials=allow_credentials,
        max_age=max_age
    )