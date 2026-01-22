"""
Endpoints para queries en lenguaje natural
"""
from fastapi import APIRouter, HTTPException
from app.schemas.query import QueryRequest, QueryResponse, ErrorResponse
from typing import Dict, Any, List, Optional  
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

async def _build_intelligent_context(
    ctx: Dict[str, Any],
    new_query: str,
    max_interactions: int = 3,
    max_records: int = 3
) -> str:
    """
    Construye contexto conversacional INTELIGENTE
    Filtra campos irrelevantes automáticamente
    """
    context_hint = "\n\n[CONTEXTO DE CONVERSACIÓN ANTERIOR]:\n"
    context_hint += "IMPORTANTE: Usa SOLO campos relevantes para la nueva pregunta.\n\n"
    
    recent_interactions = list(zip(
        ctx["queries"][-max_interactions:],
        ctx["results"][-max_interactions:]
    ))
    
    for i, (prev_query, prev_result) in enumerate(recent_interactions, 1):
        context_hint += f"\n--- Interacción {i} ---"
        context_hint += f"\nPregunta: {prev_query}"
        context_hint += f"\nRespuesta: {prev_result['answer'][:200]}"
        
        if prev_result.get('tables'):
            context_hint += f"\nTablas: {', '.join(prev_result['tables'])}"
        
        if prev_result.get('sql'):
            context_hint += f"\nSQL: {prev_result['sql'][:150]}..."
        
        # Datos con formato inteligente
        data_summary = prev_result.get('data')
        
        if data_summary:
            if isinstance(data_summary, dict):
                context_hint += await _format_summary_for_context(
                    summary=data_summary,
                    max_records=max_records
                )
            elif isinstance(data_summary, list) and len(data_summary) > 0:
                # Legacy: lista directa de datos
                context_hint += f"\n[LEGACY: {len(data_summary)} registros]"
        
        context_hint += "\n"
    
    context_hint += "\n[NUEVA PREGUNTA]:\n"
    
    return context_hint


async def _format_summary_for_context(
    summary: Dict[str, Any],
    max_records: int = 3
) -> str:
    """Formatea un resumen para el contexto"""
    context = ""
    
    summary_type = summary.get("type", "unknown")
    row_count = summary.get("row_count", 0)
    data = summary.get("data", [])
    field_info = summary.get("field_info", {})
    
    if summary_type == "aggregation":
        context += f"\nDatos agregados ({row_count} grupos):"
        for idx, record in enumerate(data[:max_records], 1):
            context += f"\n  {idx}. {record}"
        
        if len(data) > max_records:
            context += f"\n  ... +{len(data) - max_records} grupos más"
    
    else:
        total_fields = field_info.get("total_fields", 0)
        essential = field_info.get("essential_fields", [])
        omitted = field_info.get("omitted_count", 0)
        
        context += f"\nDatos ({row_count} registros"
        if total_fields:
            context += f", {total_fields} campos totales"
        if essential:
            context += f", mostrando {len(essential)} esenciales"
        context += "):"
        
        for idx, record in enumerate(data[:max_records], 1):
            context += f"\n  {idx}. {record}"
        
        if len(data) > max_records:
            context += f"\n  ... +{len(data) - max_records} más"
        
        if omitted and omitted > 0:
            context += f"\n  [Omitidos: {omitted} campos]"
    
    if summary.get("stats"):
        context += f"\n  Stats: {summary['stats']}"
    
    return context


async def _create_intelligent_summary(
    data: List[Dict[str, Any]],
    sql: Optional[str],
    tables: Optional[List[str]],
    user_query: str
) -> Dict[str, Any]:
    """Crea resumen INTELIGENTE usando SchemaIntelligenceAgent"""
    if not data:
        return None
    
    from app.tools.database_tools import schema_intelligence
    
    first_record = data[0]
    all_fields = list(first_record.keys())
    total_fields = len(all_fields)
    
    summary = {
        "row_count": len(data),
        "tables": tables or [],
        "query_type": _detect_query_type_simple(sql) if sql else "unknown",
        "field_info": {"total_fields": total_fields}
    }
    
    # Detectar agregación
    is_aggregation = sum(
        1 for field in all_fields
        if any(agg in field.lower() for agg in ['sum', 'count', 'avg', 'min', 'max', 'total'])
    ) >= 2
    
    if is_aggregation:
        summary["type"] = "aggregation"
        summary["data"] = data[:10]
        summary["field_info"]["all_fields"] = all_fields
    
    else:
        summary["type"] = "listing"
        
        # Agente decide campos
        essential_fields = []
        if tables and len(tables) > 0:
            try:
                essential_fields = await schema_intelligence.get_essential_fields_for_query(
                    table_name=tables[0],
                    user_query=user_query
                )
                essential_fields = [f for f in essential_fields if f in all_fields]
            except Exception as e:
                logger.error("essential_fields_error", error=str(e))
        
        # Fallback
        if not essential_fields:
            essential_fields = [f for f in all_fields if 'id' in f.lower()][:5]
            if not essential_fields:
                essential_fields = all_fields[:5]
        
        # Guardar solo campos esenciales
        summary["data"] = [
            {k: v for k, v in record.items() if k in essential_fields}
            for record in data[:5]
        ]
        
        summary["field_info"]["essential_fields"] = essential_fields
        summary["field_info"]["omitted_count"] = total_fields - len(essential_fields)
    
    return summary


def _detect_query_type_simple(sql: str) -> str:
    """Detecta tipo de query del SQL"""
    sql_upper = sql.upper()
    
    if 'GROUP BY' in sql_upper:
        return "aggregation"
    elif 'ORDER BY' in sql_upper:
        return "ranking"
    else:
        return "listing"

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
                    
                    # 🔥 NUEVO: Usar context builder inteligente
                    context_hint = await _build_intelligent_context(
                        ctx=ctx,
                        new_query=request.query,
                        max_interactions=3,
                        max_records=3
                    )
                    
                    enhanced_query = context_hint + request.query
                    
                    logger.info(
                        "using_intelligent_context",
                        conversation_id=request.conversation_id,
                        context_size=len(context_hint)
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
        # IMPORTANTE: Guardar en contexto conversacional
        if request.conversation_id:
            if request.conversation_id not in conversation_contexts:
                conversation_contexts[request.conversation_id] = {
                    "queries": [],
                    "results": [],
                    "timestamp": time.time()
                }
            
            # 🔥 NUEVO: Crear resumen inteligente
            data_summary = await _create_intelligent_summary(
                data=data,
                sql=sql_generated,
                tables=tables_used,
                user_query=request.query
            ) if data else None
            
            # Agregar query y resultado al historial
            conversation_contexts[request.conversation_id]["queries"].append(request.query)
            conversation_contexts[request.conversation_id]["results"].append({
                "answer": answer,
                "sql": sql_generated,
                "data": data_summary,  # 🔥 Solo resumen, NO datos completos
                "tables": tables_used
            })
            
            # Mantener solo últimas 5 interacciones
            if len(conversation_contexts[request.conversation_id]["queries"]) > 5:
                conversation_contexts[request.conversation_id]["queries"].pop(0)
                conversation_contexts[request.conversation_id]["results"].pop(0)
            
            # Actualizar timestamp
            conversation_contexts[request.conversation_id]["timestamp"] = time.time()
            
            logger.info(
                "context_saved_intelligently",
                conversation_id=request.conversation_id,
                summary_type=data_summary.get("type") if data_summary else None
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