"""
Endpoints para manejo de clarificaciones y aprendizaje
"""
from fastapi import APIRouter, HTTPException
from datetime import datetime 
from app.schemas.clarification import (
    ClarificationNeeded,
    ClarificationResponse,
    ClarificationProcessedResponse,
    LearningStored
)
from app.agents.learning_agent import learning_agent
from app.agents.explorer_agent import explorer_agent
from app.knowledge_graph.storage import kg_storage
import structlog

logger = structlog.get_logger()

router = APIRouter()


# Almacenamiento temporal de sesiones de clarificación
clarification_sessions = {}


@router.post("/clarify", response_model=ClarificationProcessedResponse)
async def respond_to_clarification(response: ClarificationResponse):
    """
    Procesa la respuesta del usuario a una clarificación
    Y ALMACENA EL APRENDIZAJE AUTOMÁTICAMENTE
    
    Args:
        response: Respuesta del usuario con conversation_id
        
    Returns:
        Resultado del aprendizaje o nueva query
    """
    logger.info(
        "clarification_response_received",
        conversation_id=response.conversation_id,
        answer=response.answer
    )
    
    # Buscar sesión
    session = clarification_sessions.get(response.conversation_id)
    
    if not session:
        raise HTTPException(
            status_code=404,
            detail="Sesión de clarificación no encontrada"
        )
    
    try:
        # Procesar respuesta del usuario con Learning Agent
        result = await learning_agent.process_user_response(
            original_query=session["original_query"],
            clarification_question=session["clarification"]["question"],
            user_answer=response.answer,
            context=session["context"]
        )
        
        if not result.get("success"):
            raise HTTPException(
                status_code=500,
                detail="Error al procesar la respuesta"
            )
        
        learning = result.get("learning", {})
        
        logger.info(
            "learning_extracted_from_response",
            learning=learning
        )
        
        # ALMACENAR APRENDIZAJE AUTOMÁTICAMENTE
        mappings_stored = []
        
        if learning.get("suggested_mapping"):
            mapping = learning["suggested_mapping"]
            user_term = mapping.get("user_term")
            db_table = mapping.get("db_table")
            db_field = mapping.get("db_field")
            confidence = learning.get("confidence", 0.85)
            
            if user_term and db_table:
                # Almacenar en Knowledge Graph
                success = await kg_storage.store_semantic_mapping(
                    user_term=user_term,
                    db_table=db_table,
                    db_field=db_field,
                    confidence=confidence,
                    context={
                        "original_query": session["original_query"],
                        "clarification": session["clarification"]["question"],
                        "user_response": response.answer,
                        "learned_at": datetime.now().isoformat()
                    }
                )
                
                if success:
                    mappings_stored.append({
                        "user_term": user_term,
                        "db_table": db_table,
                        "db_field": db_field,
                        "confidence": confidence
                    })
                    
                    logger.info(
                        "mapping_stored_from_clarification",
                        user_term=user_term,
                        db_table=db_table,
                        confidence=confidence
                    )
                else:
                    logger.warning(
                        "mapping_storage_failed",
                        user_term=user_term,
                        db_table=db_table
                    )
        
        # Limpiar sesión
        del clarification_sessions[response.conversation_id]
        
        # REINTENTAR QUERY ORIGINAL con el nuevo conocimiento
        logger.info(
            "retrying_original_query_with_learning",
            original_query=session["original_query"]
        )
        
        retry_result = await explorer_agent.explore_and_answer(
            user_query=session["original_query"],
            max_iterations=15
        )
        
        # Construir respuesta con notificación del aprendizaje
        learning_message = ""
        if mappings_stored:
            mapping = mappings_stored[0]
            learning_message = f"\n\n✅ Aprendizaje guardado: '{mapping['user_term']}' → tabla '{mapping['db_table']}' (confianza: {mapping['confidence']:.0%}). La próxima vez no necesitaré preguntarte."
        
        return {
            "success": retry_result.get("success", False),
            "message": learning.get("explanation", "Procesado correctamente") + learning_message,
            "learning_summary": {
                "mappings_stored": mappings_stored,
                "total_stored": len(mappings_stored),
                "confidence": learning.get("confidence", 0.0)
            },
            "retry_result": {
                "answer": retry_result.get("answer", ""),
                "sql_generated": retry_result.get("sql_generated"),
                "success": retry_result.get("success", False)
            }
        }
        
    except HTTPException:
        raise
        
    except Exception as e:
        logger.error("clarification_processing_error", error=str(e))
        
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )

@router.get("/learnings")
async def get_all_learnings():
    """
    Obtiene todos los aprendizajes almacenados
    
    Returns:
        Diccionario con todos los aprendizajes del sistema
    """
    try:
        learnings = kg_storage.get_all_mappings()
        
        return {
            "success": True,
            "learnings": learnings,
            "summary": {
                "total_learnings": learnings["total_learnings"],
                "semantic_mappings": len(learnings["semantic_mappings"]),
                "field_semantics": len(learnings["field_semantics"]),
                "query_patterns": len(learnings["query_patterns"]),
                "business_rules": len(learnings["business_rules"])
            }
        }
        
    except Exception as e:
        logger.error("get_learnings_error", error=str(e))
        
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@router.get("/learnings/mappings/{user_term}")
async def get_mapping(user_term: str):
    """
    Obtiene un mapeo semántico específico
    
    Args:
        user_term: Término del usuario a buscar
        
    Returns:
        Mapeo encontrado o 404
    """
    try:
        mapping = await kg_storage.get_semantic_mapping(user_term)
        
        if not mapping:
            raise HTTPException(
                status_code=404,
                detail=f"No se encontró mapeo para '{user_term}'"
            )
        
        return {
            "success": True,
            "mapping": mapping
        }
        
    except HTTPException:
        raise
        
    except Exception as e:
        logger.error("get_mapping_error", error=str(e))
        
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@router.post("/learnings/mapping")
async def create_manual_mapping(
    user_term: str,
    db_table: str,
    db_field: str = None,
    confidence: float = 0.9
):
    """
    Crea un mapeo semántico manualmente (sin clarificación)
    
    Args:
        user_term: Término del usuario
        db_table: Tabla de la BD
        db_field: Campo específico (opcional)
        confidence: Nivel de confianza
        
    Returns:
        Confirmación de almacenamiento
    """
    try:
        success = await kg_storage.store_semantic_mapping(
            user_term=user_term,
            db_table=db_table,
            db_field=db_field,
            confidence=confidence
        )
        
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Error al almacenar mapeo"
            )
        
        return {
            "success": True,
            "message": f"Mapeo creado: '{user_term}' → '{db_table}.{db_field or '*'}'",
            "mapping": {
                "user_term": user_term,
                "db_table": db_table,
                "db_field": db_field,
                "confidence": confidence
            }
        }
        
    except Exception as e:
        logger.error("create_mapping_error", error=str(e))
        
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )


@router.delete("/learnings/clear")
async def clear_all_learnings():
    """
    Limpia todos los aprendizajes (útil para testing)
    
    Returns:
        Confirmación
    """
    try:
        kg_storage.clear_all()
        
        logger.warning("all_learnings_cleared")
        
        return {
            "success": True,
            "message": "Todos los aprendizajes han sido eliminados"
        }
        
    except Exception as e:
        logger.error("clear_learnings_error", error=str(e))
        
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
    

@router.post("/learnings/business-rule")
async def create_business_rule(request: dict):
    """
    Almacena una regla de negocio
    
    Request Body:
    {
        "rule_name": "nombre_regla",
        "rule_definition": "definición en lenguaje natural",
        "tables_involved": ["tabla1", "tabla2"],
        "formula": "fórmula SQL opcional",
        "confidence": 0.95
    }
    
    Returns:
        Confirmación
    """
    try:
        # Extraer parámetros del body
        rule_name = request.get("rule_name")
        rule_definition = request.get("rule_definition")
        tables_involved = request.get("tables_involved", [])
        formula = request.get("formula")
        confidence = request.get("confidence", 0.95)
        
        # Validaciones
        if not rule_name:
            raise HTTPException(status_code=400, detail="rule_name es requerido")
        if not rule_definition:
            raise HTTPException(status_code=400, detail="rule_definition es requerido")
        if not isinstance(tables_involved, list):
            raise HTTPException(status_code=400, detail="tables_involved debe ser una lista")
        
        success = await kg_storage.store_business_rule(
            rule_name=rule_name,
            rule_definition=rule_definition,
            tables_involved=tables_involved,
            formula=formula,
            confidence=confidence
        )
        
        if not success:
            raise HTTPException(
                status_code=500,
                detail="Error al almacenar regla de negocio"
            )
        
        return {
            "success": True,
            "message": f"Regla de negocio creada: {rule_name}",
            "rule": {
                "name": rule_name,
                "definition": rule_definition,
                "formula": formula,
                "tables": tables_involved
            }
        }
        
    except HTTPException:
        raise
    except Exception as e:
        logger.error("create_business_rule_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))

@router.get("/learnings/business-rules")
async def get_all_business_rules():
    """
    Obtiene todas las reglas de negocio almacenadas
    """
    try:
        learnings = kg_storage.get_all_mappings()
        
        return {
            "success": True,
            "business_rules": learnings.get("business_rules", {}),
            "total": len(learnings.get("business_rules", {}))
        }
        
    except Exception as e:
        logger.error("get_business_rules_error", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))
    

@router.get("/learnings/recent")
async def get_recent_learnings(limit: int = 10):
    """
    Obtiene los aprendizajes más recientes
    
    Args:
        limit: Número de aprendizajes a retornar (default: 10)
        
    Returns:
        Lista de aprendizajes recientes
    """
    try:
        from sqlalchemy import text
        
        query = text("""
            SELECT user_term, db_table, db_field, confidence, created_at, usage_count
            FROM kg_semantic_mappings
            ORDER BY created_at DESC
            LIMIT :limit
        """)
        
        from app.core.database import db_manager
        
        with db_manager.get_session() as session:
            result = session.execute(query, {"limit": limit})
            rows = result.fetchall()
            
            mappings = []
            for row in rows:
                mappings.append({
                    "user_term": row[0],
                    "db_table": row[1],
                    "db_field": row[2],
                    "confidence": float(row[3]),
                    "learned_at": row[4].isoformat() if row[4] else None,
                    "usage_count": row[5]
                })
        
        return {
            "success": True,
            "recent_learnings": mappings,
            "total": len(mappings)
        }
        
    except Exception as e:
        logger.error("get_recent_learnings_error", error=str(e))
        
        # Fallback a storage en memoria si MySQL falla
        try:
            all_learnings = kg_storage.get_all_mappings()
            mappings_dict = all_learnings.get("semantic_mappings", {})
            
            # Aplanar y ordenar por fecha
            all_mappings = []
            for term, mappings_list in mappings_dict.items():
                if isinstance(mappings_list, list):
                    all_mappings.extend(mappings_list)
            
            # Tomar los últimos N
            recent = all_mappings[-limit:] if len(all_mappings) > limit else all_mappings
            recent.reverse()
            
            return {
                "success": True,
                "recent_learnings": recent,
                "total": len(recent),
                "source": "in_memory"
            }
            
        except Exception as fallback_error:
            logger.error("fallback_error", error=str(fallback_error))
            raise HTTPException(status_code=500, detail=str(e))