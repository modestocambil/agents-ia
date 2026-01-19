"""
Herramientas de base de datos para los agentes de OpenAI
"""
from typing import List, Dict, Any, Optional
import structlog
from app.core.database import db_manager

logger = structlog.get_logger()


class DatabaseTools:
    """
    Conjunto de herramientas para exploración y consulta de BD
    Estas funciones serán llamadas por los agentes de OpenAI
    """
    
    def __init__(self):
        self.db = db_manager
    
    async def get_table_list(self, include_row_counts: bool = False) -> Dict[str, Any]:
        """
        Obtiene la lista de todas las tablas en la base de datos
        VERSION ASYNC
        """
        try:
            import asyncio
            
            # Ejecutar en thread separado
            def _get_tables():
                return self.db.get_all_tables()
            
            tables = await asyncio.to_thread(_get_tables)

            result = {
                "total_tables": len(tables),
                "tables": tables
            }

            if include_row_counts:
                counts = {}
                for table in tables:
                    try:
                        counts[table] = self.db.get_table_row_count(table)
                    except Exception:
                        counts[table] = None

                result["row_counts"] = counts

            logger.info("tool_get_table_list", tables_count=len(tables))
            return result

        except Exception as e:
            logger.error("tool_get_table_list_error", error=str(e))
            return {"error": str(e)}


    async def explore_table_schema(
        self,
        table_name: str,
        include_sample_data: bool = False,
        include_statistics: bool = False
    ) -> Dict[str, Any]:
        """
        Explora el schema completo de una tabla
        
        Args:
            table_name: Nombre de la tabla
            include_sample_data: Si incluir datos de ejemplo
            include_statistics: Si incluir estadísticas
            
        Returns:
            Diccionario con schema completo
        """
        try:
            schema = self.db.get_table_schema(table_name)
            
            result = {
                "table_name": table_name,
                "columns": [
                    {
                        "name": col["name"],
                        "type": str(col["type"]),
                        "nullable": col.get("nullable", True),
                        "default": col.get("default")
                    }
                    for col in schema["columns"]
                ],
                "primary_key": schema["primary_key"],
                "foreign_keys": schema["foreign_keys"]
            }
            
            if include_sample_data:
                sample = self.db.get_sample_data(table_name, limit=5)
                result["sample_data"] = sample
            
            if include_statistics:
                # Estadísticas básicas
                stats = {}
                for col in schema["columns"]:
                    col_name = col["name"]
                    try:
                        query = f"""
                        SELECT 
                            COUNT(DISTINCT {col_name}) as unique_values,
                            COUNT(*) - COUNT({col_name}) as null_count
                        FROM {table_name}
                        LIMIT 1
                        """
                        stat_result = await self.db.execute_query(query, limit=1)
                        if stat_result:
                            stats[col_name] = stat_result[0]
                    except:
                        stats[col_name] = None
                
                result["statistics"] = stats
            
            logger.info("tool_explore_schema", table=table_name)
            return result
            
        except Exception as e:
            logger.error("tool_explore_schema_error", table=table_name, error=str(e))
            return {"error": str(e), "table_name": table_name}
    
    async def find_table_relationships(
        self,
        tables: List[str],
        include_implicit: bool = False
    ) -> Dict[str, Any]:
        """
        Encuentra relaciones entre tablas específicas
        
        Args:
            tables: Lista de nombres de tablas
            include_implicit: Si buscar relaciones implícitas
            
        Returns:
            Diccionario con relaciones encontradas
        """
        try:
            relationships = []
            
            # Foreign keys explícitas
            for table in tables:
                fks = self.db.get_foreign_keys(table)
                
                for fk in fks:
                    # Solo incluir si la tabla referenciada está en la lista
                    if fk.get("referred_table") in tables:
                        relationships.append({
                            "type": "explicit_fk",
                            "from_table": table,
                            "from_column": fk.get("constrained_columns", [])[0] if fk.get("constrained_columns") else None,
                            "to_table": fk.get("referred_table"),
                            "to_column": fk.get("referred_columns", [])[0] if fk.get("referred_columns") else None,
                            "confidence": 1.0
                        })
            
            # Relaciones implícitas (si se solicita)
            if include_implicit:
                # Buscar columnas con nombres similares
                for i, table1 in enumerate(tables):
                    for table2 in tables[i+1:]:
                        schema1 = self.db.get_table_schema(table1)
                        schema2 = self.db.get_table_schema(table2)
                        
                        cols1 = [col["name"] for col in schema1["columns"]]
                        cols2 = [col["name"] for col in schema2["columns"]]
                        
                        # Buscar coincidencias
                        for col1 in cols1:
                            for col2 in cols2:
                                if self._column_name_similarity(col1, col2) > 0.8:
                                    relationships.append({
                                        "type": "implicit",
                                        "from_table": table1,
                                        "from_column": col1,
                                        "to_table": table2,
                                        "to_column": col2,
                                        "confidence": 0.7
                                    })
            
            result = {
                "tables": tables,
                "relationships": relationships,
                "total_relationships": len(relationships)
            }
            
            logger.info(
                "tool_find_relationships",
                tables_count=len(tables),
                relationships_count=len(relationships)
            )
            
            return result
            
        except Exception as e:
            logger.error("tool_find_relationships_error", error=str(e))
            return {"error": str(e)}
    
    async def build_and_execute_query(
        self,
        tables: List[str],
        joins: Optional[List[str]] = None,
        filters: Optional[List[str]] = None,
        aggregations: Optional[List[str]] = None,
        group_by: Optional[List[str]] = None,
        order_by: Optional[str] = None,
        limit: int = 100
    ) -> Dict[str, Any]:
        """
        Construye y ejecuta una query SQL
        VERSION CORREGIDA con mejor manejo de JOINs y ejecución async
        """
        try:
            # Validación básica
            if not tables or len(tables) == 0:
                return {
                    "success": False,
                    "error": "Se requiere al menos una tabla"
                }
            
            # Construcción del SELECT
            if aggregations:
                select_clause = ", ".join(aggregations)
            else:
                select_clause = "*"
            
            # Tabla principal
            main_table = tables[0]
            query_parts = [f"SELECT {select_clause}", f"FROM {main_table}"]
            
            # 🔥 MEJORADO: Construcción de JOINs
            if joins and len(joins) > 0:
                for join_str in joins:
                    join_str = join_str.strip()
                    
                    # Si ya tiene JOIN al inicio, usarlo directo
                    if join_str.upper().startswith('JOIN') or join_str.upper().startswith('INNER JOIN') or join_str.upper().startswith('LEFT JOIN'):
                        query_parts.append(join_str)
                    elif ' ON ' in join_str.upper():
                        # Formato: "tabla ON condicion"
                        query_parts.append(f"JOIN {join_str}")
                    else:
                        # Formato desconocido, agregar tal cual
                        query_parts.append(join_str)
            
            # Construcción de WHERE
            if filters and len(filters) > 0:
                where_conditions = " AND ".join(f"({f})" for f in filters)
                query_parts.append(f"WHERE {where_conditions}")
            
            # Construcción de GROUP BY
            if group_by and len(group_by) > 0:
                query_parts.append(f"GROUP BY {', '.join(group_by)}")
            
            # Construcción de ORDER BY
            if order_by:
                query_parts.append(f"ORDER BY {order_by}")
            
            # LIMIT
            query_parts.append(f"LIMIT {limit}")
            
            # Query final
            query = "\n".join(query_parts)
            
            logger.info("sql_constructed", query=query[:300])
            
            # 🔥 EJECUTAR QUERY con manejo de errores mejorado
            try:
                results = await self.db.execute_query(query, limit=limit)
                
                logger.info(
                    "query_executed_successfully",
                    row_count=len(results),
                    query_preview=query[:200]
                )
                
                return {
                    "success": True,
                    "query": query,
                    "data": results,
                    "row_count": len(results),
                    "tables": tables  # 🔥 AGREGAR para que sea extraído en routes
                }
                
            except Exception as exec_error:
                error_msg = str(exec_error)
                logger.error(
                    "query_execution_error",
                    error=error_msg,
                    query=query[:300]
                )
                
                return {
                    "success": False,
                    "error": error_msg,
                    "query": query,
                    "tables": tables
                }
            
        except Exception as e:
            logger.error("build_query_error", error=str(e))
            
            return {
                "success": False,
                "error": str(e)
            }


    async def explore_k_hop_neighborhood(
        self,
        start_table: str,
        user_query: str,
        k: int = 2,
        max_tables: int = 5
    ) -> Dict[str, Any]:
        """
        Explora el vecindario K-Hop de una tabla con filtrado semántico
        
        Args:
            start_table: Tabla inicial
            user_query: Query del usuario (para filtrado semántico)
            k: Profundidad de exploración
            max_tables: Máximo de tablas a retornar
            
        Returns:
            Diccionario con tablas relevantes y sus relaciones
        """
        try:
            from app.tools.database_graph import db_graph
            
            # Obtener vecindario K-hop
            neighbors_by_level = db_graph.get_k_hop_neighbors(
                start_table=start_table,
                k=k,
                bidirectional=True
            )
            
            # Aplanar todas las tablas encontradas
            all_neighbors = []
            for level, neighbors in neighbors_by_level.items():
                for table, rel_info in neighbors:
                    all_neighbors.append({
                        "table": table,
                        "level": level,
                        "relationship": rel_info,
                        "relevance_score": 0.0  # Se calculará después
                    })
            
            # Calcular relevancia semántica basada en el query del usuario
            query_terms = set(user_query.lower().split())
            
            for neighbor in all_neighbors:
                table_name = neighbor["table"].lower()
                score = 0.0
                
                # Coincidencia exacta con término del query
                for term in query_terms:
                    if len(term) > 3:  # Ignorar palabras muy cortas
                        if term in table_name:
                            score += 50
                        elif table_name in term:
                            score += 30
                
                # Penalización por profundidad
                score -= neighbor["level"] * 10
                
                # Bonus por cardinalidad many-to-one (más útil)
                if neighbor["relationship"].get("cardinality") == "many_to_one":
                    score += 15
                
                # Bonus por confidence alta
                score += neighbor["relationship"].get("confidence", 0) * 10
                
                neighbor["relevance_score"] = score
            
            # Ordenar por relevancia y tomar top N
            all_neighbors.sort(key=lambda x: x["relevance_score"], reverse=True)
            top_neighbors = all_neighbors[:max_tables]
            
            result = {
                "start_table": start_table,
                "k": k,
                "total_found": len(all_neighbors),
                "returned": len(top_neighbors),
                "neighbors": top_neighbors,
                "neighbors_by_level": {
                    level: [
                        {"table": t, "relationship": r}
                        for t, r in neighbors
                    ]
                    for level, neighbors in neighbors_by_level.items()
                }
            }
            
            logger.info(
                "k_hop_exploration",
                start_table=start_table,
                k=k,
                total_found=len(all_neighbors),
                top_tables=[n["table"] for n in top_neighbors]
            )
            
            return result
            
        except Exception as e:
            logger.error("k_hop_exploration_error", error=str(e))
            return {"error": str(e)}
        
    def _column_name_similarity(self, col1: str, col2: str) -> float:
        """
        Calcula similitud entre nombres de columnas
        Simple: coincidencia exacta o uno contiene al otro
        """
        col1_lower = col1.lower()
        col2_lower = col2.lower()
        
        if col1_lower == col2_lower:
            return 1.0
        
        if col1_lower in col2_lower or col2_lower in col1_lower:
            return 0.8
        
        # Verificar si terminan igual (ej: customer_id y id)
        if col1_lower.endswith(col2_lower) or col2_lower.endswith(col1_lower):
            return 0.7
        
        return 0.0



# Definiciones de herramientas para OpenAI Function Calling
DATABASE_TOOLS_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": "get_table_list",
            "description": "Obtiene la lista completa de tablas en la base de datos",
            "parameters": {
                "type": "object",
                "properties": {
                    "include_row_counts": {
                        "type": "boolean",
                        "description": "Si incluir conteo estimado de filas de cada tabla"
                    }
                }
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "explore_table_schema",
            "description": "Obtiene el schema completo de una tabla específica (columnas, tipos, constraints)",
            "parameters": {
                "type": "object",
                "properties": {
                    "table_name": {
                        "type": "string",
                        "description": "Nombre de la tabla a explorar"
                    },
                    "include_sample_data": {
                        "type": "boolean",
                        "description": "Si incluir datos de ejemplo (5 filas)"
                    },
                    "include_statistics": {
                        "type": "boolean",
                        "description": "Si incluir estadísticas (valores únicos, nulls, etc.)"
                    }
                },
                "required": ["table_name"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "find_table_relationships",
            "description": "Descubre relaciones (FKs) entre tablas específicas",
            "parameters": {
                "type": "object",
                "properties": {
                    "tables": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Lista de tablas para analizar relaciones"
                    },
                    "include_implicit": {
                        "type": "boolean",
                        "description": "Si buscar relaciones implícitas (sin FK declarada)"
                    }
                },
                "required": ["tables"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "build_and_execute_query",
            "description": "Construye y ejecuta un query SQL basado en parámetros estructurados",
            "parameters": {
                "type": "object",
                "properties": {
                    "tables": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Tablas a incluir en el query (la primera es la tabla principal)"
                    },
                    "joins": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Lista de condiciones de JOIN. Formato: 'tabla ON condicion' o 'JOIN tabla ON condicion'"
                    },
                    "filters": {
                        "type": "array",  # 🔥 CAMBIO: de "object" a "array"
                        "items": {"type": "string"},
                        "description": "Lista de condiciones WHERE (sin la palabra WHERE)"
                    },
                    "aggregations": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Agregaciones en formato 'FUNCION(campo) AS alias'. Ej: 'COUNT(*) AS total', 'SUM(price) AS total_price'"
                    },
                    "group_by": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Campos para GROUP BY"
                    },
                    "order_by": {
                        "type": "string",
                        "description": "Cláusula ORDER BY (sin las palabras ORDER BY)"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Límite de filas a retornar",
                        "default": 100
                    }
                },
                "required": ["tables"]
            }
        }
    },
    {
        "type": "function",
        "function": {
            "name": "explore_k_hop_neighborhood",
            "description": "Explora el vecindario K-Hop de una tabla para encontrar tablas relacionadas de forma inteligente. Usa esto cuando necesites descubrir qué tablas están conectadas a una tabla principal.",
            "parameters": {
                "type": "object",
                "properties": {
                    "start_table": {
                        "type": "string",
                        "description": "Tabla inicial desde donde explorar"
                    },
                    "user_query": {
                        "type": "string",
                        "description": "Query original del usuario (para filtrado semántico)"
                    },
                    "k": {
                        "type": "integer",
                        "description": "Profundidad de exploración (1-3)",
                        "default": 2
                    },
                    "max_tables": {
                        "type": "integer",
                        "description": "Máximo de tablas relevantes a retornar",
                        "default": 5
                    }
                },
                "required": ["start_table", "user_query"]
            }
        }
    }
]


# Instancia global
database_tools = DatabaseTools()