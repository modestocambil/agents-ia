"""
Schemas para queries y respuestas
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any
from datetime import datetime


class QueryRequest(BaseModel):
    """
    Request para hacer una query en lenguaje natural
    """
    query: str = Field(..., description="Pregunta en lenguaje natural")
    conversation_id: Optional[str] = Field(None, description="ID de conversación para contexto")
    user_id: Optional[str] = Field(None, description="ID del usuario")
    
    class Config:
        json_schema_extra = {
            "example": {
                "query": "¿Cuántas ventas tuvo el cliente Juan Pérez en diciembre?",
                "conversation_id": "conv_123",
                "user_id": "user_456"
            }
        }


class QueryResponse(BaseModel):
    """
    Response con el resultado de la query
    """
    success: bool = Field(..., description="Si la query fue exitosa")
    answer: str = Field(..., description="Respuesta en lenguaje natural")
    sql_generated: Optional[str] = Field(None, description="SQL generado")
    data: Optional[List[Dict[str, Any]]] = Field(None, description="Datos resultado")
    tables_used: Optional[List[str]] = Field(None, description="Tablas utilizadas")
    execution_time_ms: Optional[float] = Field(None, description="Tiempo de ejecución")
    confidence_score: Optional[float] = Field(None, description="Nivel de confianza")
    from_cache: bool = Field(False, description="Si vino del cache")
    conversation_id: Optional[str] = Field(None, description="ID de conversación")
    
    # Campos para clarificaciones
    needs_clarification: Optional[bool] = Field(False, description="Si necesita clarificación del usuario")
    clarification_options: Optional[List[str]] = Field(None, description="Opciones de clarificación")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "answer": "El cliente Juan Pérez tuvo 15 ventas en diciembre por un total de $12,500",
                "sql_generated": "SELECT COUNT(*) as total_ventas",
                "data": [{"total_ventas": 15, "total": 12500}],
                "tables_used": ["sales", "customers"],
                "execution_time_ms": 245.5,
                "confidence_score": 0.95,
                "from_cache": False,
                "needs_clarification": False
            }
        }

class ErrorResponse(BaseModel):
    """
    Response de error
    """
    success: bool = False
    error: str = Field(..., description="Mensaje de error")
    error_type: str = Field(..., description="Tipo de error")
    details: Optional[Dict[str, Any]] = Field(None, description="Detalles adicionales")


class ClarificationRequest(BaseModel):
    """
    Request cuando el Learning Agent necesita clarificación
    """
    question: str = Field(..., description="Pregunta de clarificación")
    options: Optional[List[str]] = Field(None, description="Opciones disponibles")
    context: Dict[str, Any] = Field(..., description="Contexto de la ambigüedad")
    conversation_id: str = Field(..., description="ID de conversación")


class ClarificationResponse(BaseModel):
    """
    Response del usuario a una clarificación
    """
    answer: str = Field(..., description="Respuesta del usuario")
    conversation_id: str = Field(..., description="ID de conversación")