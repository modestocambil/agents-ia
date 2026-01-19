"""
Punto de entrada principal de la API
"""
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.api.routes import query
from app.api.routes import clarification

import structlog

# Configurar logging
structlog.configure(
    processors=[
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.add_log_level,
        structlog.processors.JSONRenderer()
    ]
)

logger = structlog.get_logger()


# Crear instancia de FastAPI
app = FastAPI(
    title=settings.APP_NAME,
    version=settings.APP_VERSION,
    debug=settings.DEBUG,
    description="API de agentes inteligentes para consultas SQL en lenguaje natural"
)

# Configurar CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.on_event("startup")
async def startup_event():
    """Eventos al iniciar la aplicación"""
    logger.info("startup", message="Iniciando SQL Agent API")
    logger.info("config", 
                openai_model=settings.OPENAI_MODEL,
                database_url=settings.DATABASE_URL.split("@")[-1] if "@" in settings.DATABASE_URL else "not_configured")
    
    # Verificar conexión a BD
    try:
        from app.core.database import db_manager
        is_connected = db_manager.test_connection()
        
        if is_connected:
            tables_count = len(db_manager.get_all_tables())
            logger.info("database_connected", tables_count=tables_count)
            
            # NUEVO: Inicializar grafo de relaciones
            from app.tools.database_graph import db_graph
            logger.info("initializing_database_graph")
            await db_graph.initialize()
            
            # Mostrar estadísticas del grafo
            stats = db_graph.get_graph_stats()
            logger.info(
                "graph_initialized",
                tables=stats["tables_count"],
                relationships=stats["relationships_count"],
                avg_connections=round(stats["avg_connections_per_table"], 2)
            )
        else:
            logger.warning("database_not_connected")
    except Exception as e:
        logger.error("database_connection_error", error=str(e))

@app.on_event("shutdown")
async def shutdown_event():
    """Eventos al cerrar la aplicación"""
    logger.info("shutdown", message="Cerrando SQL Agent API")
    
    # Cerrar conexiones
    try:
        from app.core.database import db_manager
        db_manager.close()
    except:
        pass


@app.get("/")
async def root():
    """
    Endpoint raíz de la API
    """
    return {
        "message": "SQL Agent API",
        "version": settings.APP_VERSION,
        "status": "running",
        "docs": "/docs",
        "health": "/health"
    }


@app.get("/health")
async def health_check():
    """
    Health check endpoint
    """
    return {
        "status": "healthy",
        "version": settings.APP_VERSION,
        "openai_configured": bool(settings.OPENAI_API_KEY and settings.OPENAI_API_KEY != "your-openai-api-key-here"),
        "database_configured": bool(settings.DATABASE_URL and "localhost" not in settings.DATABASE_URL or True)
    }


# Registrar rutas de query
app.include_router(
    query.router,
    prefix="/api/v1",
    tags=["Query"]
)


app.include_router(
    clarification.router,
    prefix="/api/v1",
    tags=["Learning & Clarification"]
)