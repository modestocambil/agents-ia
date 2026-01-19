"""
Endpoints para queries en lenguaje natural
"""
from fastapi import APIRouter, HTTPException
from app.schemas.query import QueryRequest, QueryResponse, ErrorResponse
from app.agents.explorer_agent import explorer_agent
from app.agents.learning_agent import learning_agent
from app.api.routes.clarification import clarification_sessions
import structlog
import time
import uuid

logger = structlog.get_logger()

router = APIRouter()

# Almacenamiento temporal de sesiones pendientes de clarificación

# Almacenamiento de contexto conversacional
conversation_contexts = {}  # {conversation_id: {"queries": [], "results": [], "timestamp": datetime}}


@router.post("/query", response_model=QueryResponse)
async def execute_query(request: QueryRequest):
    """
    Ejecuta una query en lenguaje natural con soporte de contexto conversacional
    
    Args:
        request: QueryRequest con la pregunta del usuario
        
    Returns:
        QueryResponse con la respuesta y metadata
    """
    start_time = time.time()
    
    logger.info(
        "query_received",
        query=request.query,
        user_id=request.user_id,
        conversation_id=request.conversation_id
    )
    
    try:
        # Recuperar contexto conversacional si existe
        enhanced_query = request.query
        context_used = False
        
        if request.conversation_id and request.conversation_id in conversation_contexts:
            ctx = conversation_contexts[request.conversation_id]
            
            if ctx["queries"]:
                context_used = True
                context_hint = "\n\n[CONTEXTO DE CONVERSACIÓN ANTERIOR - USA ESTO PARA ENTENDER REFERENCIAS]:\n"
                
                # Incluir últimas 3 interacciones
                recent_interactions = list(zip(ctx["queries"][-3:], ctx["results"][-3:]))
                
                for i, (prev_query, prev_result) in enumerate(recent_interactions, 1):
                    context_hint += f"\n--- Interacción {i} ---"
                    context_hint += f"\nUsuario preguntó: {prev_query}"
                    context_hint += f"\nTú respondiste: {prev_result['answer'][:300]}"
                    
                    if prev_result.get('tables'):
                        context_hint += f"\nUsaste las tablas: {', '.join(prev_result['tables'])}"
                    
                    if prev_result.get('sql'):
                        context_hint += f"\nSQL ejecutado: {prev_result['sql'][:200]}"
                    
                    # MEJORADO: Incluir DATOS COMPLETOS si hay
                    if prev_result.get('data') and len(prev_result['data']) > 0:
                        context_hint += f"\n\nDATOS RETORNADOS ({len(prev_result['data'])} registros):"
                        
                        # Mostrar hasta 10 registros completos
                        for idx, record in enumerate(prev_result['data'][:10], 1):
                            context_hint += f"\n  Registro {idx}: {record}"
                        
                        if len(prev_result['data']) > 10:
                            context_hint += f"\n  ... y {len(prev_result['data']) - 10} registros más"
                    
                    context_hint += "\n"
                
                context_hint += "\n\n[NUEVA PREGUNTA DEL USUARIO - puede referirse al contexto anterior]:\n"
                enhanced_query = context_hint + request.query
                
                logger.info(
                    "using_conversation_context",
                    conversation_id=request.conversation_id,
                    previous_queries=len(ctx["queries"])
                )
        
        # Ejecutar exploración con el agente
        result = await explorer_agent.explore_and_answer(
            user_query=enhanced_query,
            max_iterations=15
        )
        
        execution_time = (time.time() - start_time) * 1000  # en ms
        
        # CASO 1: Necesita clarificación
        if result.get("needs_clarification"):
            ambiguity = result.get("ambiguity", {})
            
            logger.info(
                "clarification_needed",
                type=ambiguity.get("type"),
                conversation_id=request.conversation_id
            )
            
            # Generar pregunta clarificadora con Learning Agent
            clarification = await learning_agent.analyze_ambiguity(
                user_query=request.query,
                explorer_context=ambiguity.get("context", {}),
                ambiguity_type=ambiguity.get("type"),
                options=ambiguity.get("options")
            )
            
            # Guardar sesión para continuar después
            session_id = request.conversation_id or str(uuid.uuid4())
            clarification_sessions[session_id] = {
                "original_query": request.query,
                "user_id": request.user_id,
                "context": ambiguity.get("context", {}),
                "clarification": clarification,
                "created_at": time.time()
            }
            
            # Retornar pregunta al usuario
            return {
                "success": False,
                "answer": f"❓ {clarification.get('question', 'Necesito más información')}",
                "sql_generated": None,
                "data": None,
                "tables_used": None,
                "execution_time_ms": execution_time,
                "confidence_score": 0.0,
                "from_cache": False,
                "conversation_id": session_id,
                "needs_clarification": True,
                "clarification_options": clarification.get("options")
            }
        
        # CASO 2: Exploración falló
        if not result.get("success", False):
            raise HTTPException(
                status_code=500,
                detail={
                    "error": result.get("error", "Error desconocido"),
                    "answer": result.get("answer", "No se pudo procesar la consulta")
                }
            )
        
       # CASO 3: Éxito - extraer información del resultado
        answer = result.get("answer", "")

        # Buscar si hay SQL generado en el historial
        sql_generated = None
        tables_used = []
        data = None

        conversation = result.get("conversation_history", [])
        for msg in conversation:
            if msg.get("role") == "tool" and msg.get("name") == "build_and_execute_query":
                try:
                    import json
                    tool_result = json.loads(msg.get("content", "{}"))
                    
                    # 🔥 CORRECCIÓN: Verificar que tool_result sea dict
                    if isinstance(tool_result, dict):
                        sql_generated = tool_result.get("query")
                        data = tool_result.get("data")
                        
                        # Extraer tablas del query
                        if sql_generated:
                            import re
                            tables = re.findall(r'FROM\s+(\w+)|JOIN\s+(\w+)', sql_generated, re.IGNORECASE)
                            tables_used = list(set([t[0] or t[1] for t in tables if t[0] or t[1]]))
                            
                        # 🔥 VALIDACIÓN: Si hay SQL pero no hay data, es un error
                        if sql_generated and data is None:
                            logger.error(
                                "sql_execution_failed",
                                sql=sql_generated,
                                tool_result=tool_result
                            )
                            raise HTTPException(
                                status_code=500,
                                detail={
                                    "error": "SQL_EXECUTION_FAILED",
                                    "message": "La consulta SQL se generó pero no devolvió datos",
                                    "sql": sql_generated
                                }
                            )
                            
                except json.JSONDecodeError as e:
                    logger.error("tool_parse_error", error=str(e), content=msg.get("content", ""))
                except Exception as e:
                    logger.error("tool_processing_error", error=str(e))


        
        response = QueryResponse(
            success=True,
            answer=answer,
            sql_generated=sql_generated,
            data=data,
            tables_used=tables_used if tables_used else None,
            execution_time_ms=execution_time,
            confidence_score=0.85,
            from_cache=False,
            conversation_id=request.conversation_id
        )
        
        # IMPORTANTE: Guardar en contexto conversacional
        if request.conversation_id:
            if request.conversation_id not in conversation_contexts:
                conversation_contexts[request.conversation_id] = {
                    "queries": [],
                    "results": [],
                    "timestamp": time.time()
                }
            
            # Agregar query y resultado al historial
            conversation_contexts[request.conversation_id]["queries"].append(request.query)
            conversation_contexts[request.conversation_id]["results"].append({
                "answer": answer,
                "sql": sql_generated,
                "data": data[:20] if data else None,  # Solo primeros 10 registros
                "tables": tables_used
            })
            
            # Mantener solo últimas 5 interacciones (para no sobrecargar)
            if len(conversation_contexts[request.conversation_id]["queries"]) > 5:
                conversation_contexts[request.conversation_id]["queries"].pop(0)
                conversation_contexts[request.conversation_id]["results"].pop(0)
            
            # Actualizar timestamp
            conversation_contexts[request.conversation_id]["timestamp"] = time.time()
            
            logger.info(
                "conversation_context_saved",
                conversation_id=request.conversation_id,
                total_interactions=len(conversation_contexts[request.conversation_id]["queries"])
            )
        
        logger.info(
            "query_completed",
            execution_time_ms=execution_time,
            iterations=result.get("iterations", 0),
            tables_used=tables_used,
            context_used=context_used
        )
        
        return response
        
    except HTTPException:
        raise
        
    except Exception as e:
        logger.error("query_error", error=str(e))
        
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "message": "Error al procesar la consulta"
            }
        )

@router.get("/test-connection")
async def test_database_connection():
    """
    Prueba la conexión a la base de datos
    
    Returns:
        Estado de la conexión
    """
    try:
        from app.core.database import db_manager
        
        is_connected = db_manager.test_connection()
        
        if is_connected:
            tables_count = len(db_manager.get_all_tables())
            
            return {
                "status": "connected",
                "message": "Conexión a la base de datos exitosa",
                "tables_count": tables_count
            }
        else:
            raise HTTPException(
                status_code=500,
                detail="No se pudo conectar a la base de datos"
            )
            
    except Exception as e:
        logger.error("connection_test_error", error=str(e))
        
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "message": "Error al conectar con la base de datos"
            }
        )


@router.get("/tables")
async def list_tables():
    """
    Lista todas las tablas disponibles en la base de datos
    
    Returns:
        Lista de nombres de tablas
    """
    try:
        from app.core.database import db_manager
        
        tables = db_manager.get_all_tables()
        
        return {
            "total": len(tables),
            "tables": tables
        }
        
    except Exception as e:
        logger.error("list_tables_error", error=str(e))
        
        raise HTTPException(
            status_code=500,
            detail={
                "error": str(e),
                "message": "Error al obtener lista de tablas"
            }
        )
    
@router.get("/conversation/{conversation_id}")
async def get_conversation_context(conversation_id: str):
    """
    Obtiene el contexto de una conversación
    
    Args:
        conversation_id: ID de la conversación
        
    Returns:
        Historial de la conversación
    """
    if conversation_id not in conversation_contexts:
        raise HTTPException(
            status_code=404,
            detail="Conversación no encontrada"
        )
    
    ctx = conversation_contexts[conversation_id]
    
    return {
        "conversation_id": conversation_id,
        "total_interactions": len(ctx["queries"]),
        "history": [
            {
                "query": q,
                "answer": r["answer"],
                "sql": r.get("sql"),
                "tables": r.get("tables")
            }
            for q, r in zip(ctx["queries"], ctx["results"])
        ]
    }


@router.delete("/conversation/{conversation_id}")
async def clear_conversation_context(conversation_id: str):
    """
    Limpia el contexto de una conversación
    
    Args:
        conversation_id: ID de la conversación
        
    Returns:
        Confirmación
    """
    if conversation_id in conversation_contexts:
        del conversation_contexts[conversation_id]
        return {
            "success": True,
            "message": f"Contexto de conversación '{conversation_id}' eliminado"
        }
    
    raise HTTPException(
        status_code=404,
        detail="Conversación no encontrada"
    )