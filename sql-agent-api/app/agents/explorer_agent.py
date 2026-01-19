"""
Explorer Agent - Agente que explora la base de datos
"""
from openai import OpenAI
from typing import Dict, Any, List, Optional
import json
import structlog
from app.core.config import settings
from app.tools.database_tools import database_tools, DATABASE_TOOLS_DEFINITIONS

logger = structlog.get_logger()


class ExplorerAgent:
    """
    Agente que explora la base de datos de forma inteligente
    usando K-Hop Neighborhood y OpenAI Function Calling
    """
    
    def __init__(self):
        self.client = OpenAI(api_key=settings.OPENAI_API_KEY)
        self.tools = database_tools
        self.conversation_history = []
        
        self.system_prompt = """
Eres un agente SQL experto que explora bases de datos de forma INTELIGENTE y EFICIENTE.

IMPORTANTE - CONTEXTO CONVERSACIONAL:
Si el usuario hace referencia a información mencionada anteriormente (ej: "dame info de Juan", "detalles de ese cliente", "la mayor venta"), DEBES:
1. LEER CUIDADOSAMENTE el [CONTEXTO DE CONVERSACIÓN ANTERIOR] incluido en el prompt
2. Identificar los datos específicos mencionados (nombres, IDs, valores)
3. USAR esos datos para construir tu query SQL con filtros WHERE apropiados
4. NO decir que "no puedes acceder" - los datos ESTÁN en el contexto

Ejemplo correcto:
[CONTEXTO]: "Registro 1: {'id': 123, 'name': 'Juan Perez'}"
Usuario: "Dame info de Juan Perez"
Tú: SELECT * FROM clients WHERE name = 'Juan Perez' OR id = 123

Tu objetivo: Responder preguntas del usuario explorando una base de datos 
relacional de forma progresiva usando el algoritmo K-Hop Neighborhood.

ESTRATEGIA DE EXPLORACIÓN:

1. ANÁLISIS INICIAL DE QUERY
   - Identifica la intención del usuario
   - Extrae entidades clave mencionadas
   - Determina métricas requeridas (COUNT, SUM, AVG, etc)
   - Identifica filtros temporales o condiciones

2. IDENTIFICACIÓN DE TABLA PRINCIPAL
   - Busca en get_table_list() la tabla que mejor coincida con la entidad principal
   - Usa similitud semántica entre términos del usuario y nombres de tablas
   - Si hay ambigüedad, elige la más relevante o pide clarificación

3. EXPLORACIÓN K-HOP INTELIGENTE (CLAVE)
   Para queries que involucran múltiples conceptos:
   
   a) USA explore_k_hop_neighborhood() desde la tabla principal
      - Esto te da TODAS las tablas relacionadas en un radio K
      - El sistema ya filtra por relevancia semántica
      - Obtienes las relaciones (FKs) necesarias para hacer JOINs
   
   b) Ejemplo:
      Query: "¿Qué clientes compraron productos de categoría Cocinas?"
      
      Paso 1: Identifica tabla principal → "clients"
      Paso 2: Llama explore_k_hop_neighborhood(
                start_table="clients",
                user_query="clientes productos categoría cocinas",
                k=3,
                max_tables=5
              )
      Paso 3: Recibes:
              - orders (nivel 1, relacionado a clients)
              - orders_lines (nivel 2, relacionado a orders)
              - products (nivel 2, relacionado a orders_lines)
              - categorys (nivel 3, relacionado a products)
      Paso 4: Construyes JOINs usando las relaciones retornadas
      Paso 5: Ejecutas query completa

4. EXPLORACIÓN PROGRESIVA (si K-Hop no está disponible)
   - Nivel 0: Explora schema de tabla principal
   - Nivel 1: Descubre tablas directamente relacionadas (find_table_relationships)
   - Nivel 2: Solo si es necesario, explora relaciones de segundo nivel
   - NUNCA explores todas las 52 tablas

5. CONSTRUCCIÓN DE QUERY SQL
   - Genera SQL SOLO con las tablas necesarias
   - Usa JOINs explícitos (nunca JOINs implícitos)
   - Aplica filtros en WHERE apropiadamente
   - Usa GROUP BY cuando agregues (COUNT, SUM, etc)
   - SIEMPRE incluye LIMIT para proteger performance
   - Agrega alias claros a las tablas (ej: clients c, orders o)
   - USA "LIKE 'valor%'" cuando:
      - Buscas prefijos: WHERE code LIKE 'C2025%'
      - Buscas inicios de palabras: WHERE name LIKE 'Juan%'
   
   IMPORTANTE: Si el usuario menciona un nombre parcial (ej: "Costasol", "Juan", "Almería"),
   SIEMPRE usa LIKE '%valor%' en campos de texto.
   
   Ejemplos:
   Usuario: "clientes de Almería" → WHERE location LIKE '%Almería%'
   Usuario: "facturas de Juan" → WHERE clients.name LIKE '%Juan%'
   Usuario: "cliente con ID 100" → WHERE client_id = 100
   Usuario: "facturas pagadas" → WHERE status = 'paid'

6. VALIDACIÓN Y EJECUCIÓN
   - Valida sintaxis del SQL antes de ejecutar
   - Ejecuta con build_and_execute_query()
   - Si el resultado está vacío, analiza por qué
   - Si hay error, reintenta con ajustes

7. GENERACIÓN DE RESPUESTA
   - Convierte datos SQL a lenguaje natural
   - Sé específico con números y datos
   - Menciona las tablas que usaste si es relevante

HERRAMIENTAS DISPONIBLES:

 get_table_list(include_row_counts=False)
   Cuándo: Al inicio, para ver qué tablas existen
   Retorna: Lista de todas las tablas en la BD

 explore_table_schema(table_name, include_sample_data=False, include_statistics=False)
   Cuándo: Cuando necesitas ver columnas, tipos, constraints de una tabla
   Retorna: Schema completo, opcionalmente con datos de ejemplo

 find_table_relationships(tables, include_implicit=False)
   Cuándo: Cuando ya tienes 2-3 tablas y necesitas saber cómo conectarlas
   Retorna: Foreign keys y relaciones entre las tablas especificadas

 explore_k_hop_neighborhood(start_table, user_query, k=2, max_tables=5) RECOMENDADA
   Cuándo: Para queries complejas que involucran múltiples tablas/conceptos
   Ventajas:
   - Descubre AUTOMÁTICAMENTE todas las tablas relacionadas hasta profundidad K
   - Ya viene filtrado por relevancia semántica al user_query
   - Te da las relaciones (FKs) para construir JOINs
   - Evita explorar tablas irrelevantes
   Ejemplo: "clientes que compraron productos de categoría X"
            → explore_k_hop_neighborhood("clients", query, k=3)
            → Te retorna: orders, orders_lines, products, categorys + relaciones
   Retorna: Tablas relevantes ordenadas por nivel de distancia

   build_and_execute_query(tables, joins, filters, aggregations, limit=100)
   Cuándo: Cuando ya tienes todo listo para construir el SQL
   Retorna: Resultados de la query ejecutada

DECISIONES CLAVE:

¿Cuándo usar explore_k_hop_neighborhood vs exploración manual?

USE explore_k_hop_neighborhood CUANDO:
Query menciona múltiples conceptos (ej: "clientes", "productos", "categorías")
No estás seguro qué tablas intermedias necesitas
Query requiere más de 2 tablas
Quieres encontrar el camino entre dos entidades

USE exploración manual CUANDO:
Query es simple (1-2 tablas)
Ya conoces exactamente qué tablas necesitas
Es una query directa como "¿cuántos clientes hay?"

PRINCIPIOS DE EFICIENCIA:

NO HAGAS ESTO:
- Llamar get_table_list() múltiples veces
- Explorar schemas de 10+ tablas
- Generar queries sin LIMIT
- Usar SELECT * en tablas grandes
- Explorar tablas que claramente no son relevantes

HAZLO ASÍ:
- Una llamada a get_table_list() al inicio
- Máximo 3-5 tablas exploradas
- SIEMPRE usa LIMIT en queries
- SELECT solo columnas necesarias
- Usa explore_k_hop_neighborhood para queries complejas

MANEJO DE AMBIGÜEDADES:

Si detectas:
- Múltiples tablas candidatas con nombres similares
- Query sin resultados (posible término mal mapeado)
- Error "table doesn't exist" (término del usuario no existe en BD)

→ El sistema te retornará automáticamente indicando que necesitas clarificación
→ NO intentes adivinar, confía en el proceso de clarificación

FORMATO DE RESPUESTA:

Siempre responde en ESPAÑOL de forma:
- Clara y concisa
- Con números específicos cuando los tengas
- Mencionando datos relevantes del resultado
- Sin jerga técnica innecesaria

Ejemplo BUENO:
"Encontré 2,432 clientes registrados en tu base de datos."

Ejemplo MALO:
"La ejecución de SELECT COUNT(*) FROM clients retornó 2432 filas."

¡Sé eficiente, preciso y confía en las herramientas K-Hop para queries complejas!
"""
    
    async def explore_and_answer(
        self, 
        user_query: str,
        max_iterations: int = 10
    ) -> Dict[str, Any]:
        """
        Método principal: explora la BD y responde la pregunta
        
        Args:
            user_query: Pregunta del usuario
            max_iterations: Máximo de iteraciones
            
        Returns:
            Diccionario con respuesta y metadata
        """
        logger.info("explorer_start", query=user_query)
        
        # Consultar Knowledge Graph primero
        from app.knowledge_graph.storage import kg_storage
        
        # Extraer términos clave del query
        terms = user_query.lower().split()
        
         # Buscar mapeos conocidos
        system_hints = ""
        for term in terms:
            mappings = await kg_storage.get_semantic_mapping(term)
            if mappings:
                # Ahora mappings es una LISTA
                tables = [m["db_table"] for m in mappings]
                logger.info(
                    "using_learned_mappings",
                    term=term,
                    tables=tables
                )
                # Agregar hint al system prompt
                if len(tables) == 1:
                    system_hints += f"\nNOTA IMPORTANTE: El usuario usa '{term}' para referirse a la tabla '{tables[0]}'."
                else:
                    tables_str = ", ".join(tables)
                    system_hints += f"\nNOTA IMPORTANTE: El usuario usa '{term}' para referirse a las tablas: {tables_str}. Necesitas TODAS estas tablas."
        
        # Actualizar system prompt con hints
        current_system_prompt = self.system_prompt + system_hints
        
        # Resetear historial
        self.conversation_history = [
            {"role": "system", "content": current_system_prompt},
            {"role": "user", "content": user_query}
        ]
        
        iteration = 0
        tool_results_history = []  # Para detección de ambigüedades
        
        try:
            while iteration < max_iterations:
                iteration += 1
                
                logger.info("explorer_iteration", iteration=iteration)
                
                # Llamada a OpenAI
                response = self.client.chat.completions.create(
                    model=settings.OPENAI_MODEL,
                    messages=self.conversation_history,
                    tools=DATABASE_TOOLS_DEFINITIONS,
                    tool_choice="auto",
                    temperature=0.1
                )
                
                message = response.choices[0].message
                
                # ¿El agente quiere usar herramientas?
                if message.tool_calls:
                    # Convertir message a dict
                    message_dict = {
                        "role": "assistant",
                        "content": message.content,
                        "tool_calls": [
                            {
                                "id": tc.id,
                                "type": "function",
                                "function": {
                                    "name": tc.function.name,
                                    "arguments": tc.function.arguments
                                }
                            }
                            for tc in message.tool_calls
                        ]
                    }
                    self.conversation_history.append(message_dict)
                    
                    # Ejecutar cada herramienta
                    for tool_call in message.tool_calls:
                        function_name = tool_call.function.name
                        function_args = json.loads(tool_call.function.arguments)
                        
                        logger.info(
                            "tool_call",
                            function=function_name,
                            args=function_args
                        )
                        
                        # Ejecutar función
                        result = await self._execute_tool(
                            function_name,
                            function_args
                        )
                        
                        # Agregar resultado a conversación
                        tool_result = {
                            "role": "tool",
                            "tool_call_id": tool_call.id,
                            "name": function_name,
                            "content": json.dumps(result, ensure_ascii=False)
                        }
                        self.conversation_history.append(tool_result)
                        
                        # Guardar para detección de ambigüedades
                        tool_results_history.append(tool_result)
                    
                    # DETECTAR AMBIGÜEDADES después de ejecutar herramientas
                    ambiguity = await self._detect_ambiguity(
                        user_query,
                        tool_results_history
                    )
                    
                    if ambiguity:
                        logger.warning(
                            "ambiguity_requires_clarification",
                            type=ambiguity["type"]
                        )
                        
                        # Retornar indicando que se necesita clarificación
                        return {
                            "success": False,
                            "needs_clarification": True,
                            "ambiguity": ambiguity,
                            "iterations": iteration,
                            "message": "Se detectó ambigüedad que requiere clarificación del usuario"
                        }
                    
                    # Continuar loop
                    continue
                
                # Si no hay tool calls, tenemos respuesta final
                if message.content:
                    self.conversation_history.append({
                        "role": "assistant",
                        "content": message.content
                    })
                    
                    logger.info("explorer_complete", iterations=iteration)
                    
                    return {
                        "success": True,
                        "answer": message.content,
                        "iterations": iteration,
                        "conversation_history": self.conversation_history
                    }
            
            # Max iterations alcanzado
            logger.warning("explorer_max_iterations", max_iterations=max_iterations)
            
            return {
                "success": False,
                "answer": "Se alcanzó el límite de iteraciones. La consulta es muy compleja.",
                "iterations": iteration
            }
            
        except Exception as e:
            logger.error("explorer_error", error=str(e))
            
            return {
                "success": False,
                "error": str(e),
                "iterations": iteration
            }

   
        
    async def _execute_tool(
        self, 
        function_name: str, 
        args: Dict[str, Any]
    ) -> Any:
        """
        Ejecuta una herramienta específica
        
        Args:
            function_name: Nombre de la función
            args: Argumentos
            
        Returns:
            Resultado de la función
        """
        try:
            if function_name == "get_table_list":
                return await self.tools.get_table_list(**args)
            
            elif function_name == "explore_table_schema":
                return await self.tools.explore_table_schema(**args)
            
            elif function_name == "find_table_relationships":
                return await self.tools.find_table_relationships(**args)
            
            elif function_name == "build_and_execute_query":
                return await self.tools.build_and_execute_query(**args)
            
            elif function_name == "explore_k_hop_neighborhood":
                return await self.tools.explore_k_hop_neighborhood(**args)
            
            else:
                logger.error("unknown_tool", function=function_name)
                return {"error": f"Función {function_name} no encontrada"}
                
        except Exception as e:
            logger.error("tool_execution_error", function=function_name, error=str(e))
            return {"error": str(e)}


    async def _detect_ambiguity(
        self,
        user_query: str,
        tool_results: List[Dict[str, Any]]
    ) -> Optional[Dict[str, Any]]:
        """
        Detecta si hay ambigüedad en los resultados de exploración
        VERSIÓN MEJORADA: Detecta más casos
        """
        # Analizar resultados
        for result in tool_results:
            content_str = result.get("content", "{}")
            
            try:
                content = json.loads(content_str) if isinstance(content_str, str) else content_str
            except:
                continue
            
            # Caso 1: Múltiples tablas candidatas
            if result.get("name") == "get_table_list":
                tables = content.get("tables", [])
                
                # Buscar términos del usuario en nombres de tablas
                query_terms = [term for term in user_query.lower().split() if len(term) > 3]
                matching_tables = []
                
                for table in tables:
                    for term in query_terms:
                        if term in table.lower():
                            matching_tables.append(table)
                
                # Si hay múltiples coincidencias
                if len(matching_tables) > 1:
                    logger.info(
                        "ambiguity_detected",
                        type="multiple_tables",
                        tables=matching_tables
                    )
                    
                    return {
                        "type": "multiple_tables",
                        "user_query": user_query,
                        "options": matching_tables,
                        "context": {
                            "message": f"Encontré múltiples tablas que podrían corresponder a tu consulta: {', '.join(matching_tables)}"
                        }
                    }
            
            # Caso 2: Query ejecutado pero sin resultados (posible término mal mapeado)
            if result.get("name") == "build_and_execute_query":
                if content.get("success") and content.get("row_count", 0) == 0:
                    logger.info(
                        "ambiguity_detected",
                        type="empty_result"
                    )
                    
                    return {
                        "type": "empty_result",
                        "user_query": user_query,
                        "context": {
                            "query": content.get("query"),
                            "tables_used": content.get("tables", []),
                            "message": "La consulta no devolvió resultados. Los términos que usaste podrían no existir en la base de datos."
                        }
                    }
            
            # Caso 3: Error en ejecución
            if result.get("name") == "build_and_execute_query":
                if not content.get("success") and "error" in content:
                    error_msg = content.get("error", "").lower()
                    
                    # Detectar errores de tabla/columna no encontrada
                    if any(keyword in error_msg for keyword in ["doesn't exist", "unknown", "not found", "no such"]):
                        logger.info(
                            "ambiguity_detected",
                            type="term_not_mapped",
                            error=error_msg
                        )
                        
                        return {
                            "type": "term_not_mapped",
                            "user_query": user_query,
                            "context": {
                                "error": error_msg,
                                "message": "Algunos términos de tu consulta no se encontraron en la base de datos"
                            }
                        }
            
            # 🆕 Caso 4: K-Hop retorna muchas tablas (necesita clarificación)
            if result.get("name") == "explore_k_hop_neighborhood":
                tables_found = content.get("tables", [])
                
                if len(tables_found) > 5:
                    logger.info(
                        "ambiguity_detected",
                        type="too_many_tables",
                        count=len(tables_found)
                    )
                    
                    return {
                        "type": "too_many_tables",
                        "user_query": user_query,
                        "options": [t["table"] for t in tables_found[:10]],
                        "context": {
                            "message": f"Tu consulta podría involucrar {len(tables_found)} tablas. ¿Podrías ser más específico?"
                        }
                    }
        
        return None

    def get_conversation_context(self) -> List[Dict[str, Any]]:
        """
        Obtiene el historial de la conversación
        
        Returns:
            Historial completo
        """
        return self.conversation_history


# Instancia global
explorer_agent = ExplorerAgent()