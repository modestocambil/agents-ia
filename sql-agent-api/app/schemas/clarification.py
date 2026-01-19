"""
Schemas para el proceso de clarificación y aprendizaje
"""
from pydantic import BaseModel, Field
from typing import Optional, List, Dict, Any


class ClarificationNeeded(BaseModel):
    """
    Modelo cuando se necesita clarificación del usuario
    """
    needs_clarification: bool = True
    question: str = Field(..., description="Pregunta para el usuario")
    options: Optional[List[str]] = Field(None, description="Opciones disponibles")
    context: Dict[str, Any] = Field(..., description="Contexto de la ambigüedad")
    ambiguity_type: str = Field(..., description="Tipo de ambigüedad")
    conversation_id: str = Field(..., description="ID de conversación")
    
    class Config:
        json_schema_extra = {
            "example": {
                "needs_clarification": True,
                "question": "Veo que tu BD tiene 'provinces'. Cuando dices 'zona', ¿te refieres a provincia?",
                "options": ["Sí, zona = provincia", "No, zona es otra cosa"],
                "context": {"user_term": "zona", "db_term": "provinces"},
                "ambiguity_type": "term_mapping",
                "conversation_id": "conv_123"
            }
        }


class ClarificationResponse(BaseModel):
    """
    Respuesta del usuario a una clarificación
    """
    answer: str = Field(..., description="Respuesta del usuario")
    conversation_id: str = Field(..., description="ID de conversación")
    
    class Config:
        json_schema_extra = {
            "example": {
                "answer": "Sí, correcto. Zona es lo mismo que provincia",
                "conversation_id": "conv_123"
            }
        }


class ClarificationProcessedResponse(BaseModel):
    """
    Resultado después de procesar una clarificación
    """
    success: bool = True
    message: str = Field(..., description="Mensaje explicativo")
    learning_summary: Dict[str, Any] = Field(..., description="Resumen del aprendizaje")
    retry_result: Dict[str, Any] = Field(..., description="Resultado de reintentar la query")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Entendido. ✅ Aprendizaje guardado: 'zona' → tabla 'provinces'",
                "learning_summary": {
                    "mappings_stored": [
                        {
                            "user_term": "zona",
                            "db_table": "provinces",
                            "confidence": 0.85
                        }
                    ],
                    "total_stored": 1
                },
                "retry_result": {
                    "answer": "Hay 3 zonas registradas",
                    "success": True
                }
            }
        }
        
class LearningStored(BaseModel):
    """
    Confirmación de aprendizaje almacenado
    """
    success: bool = True
    message: str = Field(..., description="Mensaje de confirmación")
    learning_summary: Dict[str, Any] = Field(..., description="Resumen del aprendizaje")
    can_retry_query: bool = Field(True, description="Si se puede reintentar la query original")
    
    class Config:
        json_schema_extra = {
            "example": {
                "success": True,
                "message": "Aprendizaje almacenado: 'zona' ahora se asocia con 'provinces'",
                "learning_summary": {
                    "user_term": "zona",
                    "db_mapping": "provinces.name",
                    "confidence": 0.95
                },
                "can_retry_query": True
            }
        }