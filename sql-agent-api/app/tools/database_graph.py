"""
Database Graph - Grafo de relaciones de la base de datos
Implementa algoritmo K-Hop Neighborhood para exploración eficiente
"""
from typing import Dict, List, Set, Tuple, Any, Optional
from collections import defaultdict, deque
import structlog
from app.core.database import db_manager

logger = structlog.get_logger()


class DatabaseGraph:
    """
    Grafo de la base de datos que mapea todas las relaciones
    Permite navegación K-Hop eficiente
    """
    
    def __init__(self):
        self.graph = defaultdict(list)  # tabla -> [(tabla_destino, relacion_info)]
        self.reverse_graph = defaultdict(list)  # Para relaciones inversas
        self.table_metadata = {}  # Metadata de cada tabla
        self.relationships = []  # Lista de todas las relaciones
        self.initialized = False
        
        logger.info("database_graph_created")
    
    async def initialize(self):
        """
        Inicializa el grafo explorando toda la BD
        Solo se ejecuta una vez al inicio
        """
        if self.initialized:
            logger.info("graph_already_initialized")
            return
        
        logger.info("graph_initialization_start")
        
        try:
            # Obtener todas las tablas
            tables = db_manager.get_all_tables()
            
            # Para cada tabla, obtener metadata
            for table in tables:
                await self._add_table_metadata(table)
            
            # Para cada tabla, descubrir relaciones
            for table in tables:
                await self._discover_relationships(table)
            
            self.initialized = True
            
            logger.info(
                "graph_initialization_complete",
                tables_count=len(tables),
                relationships_count=len(self.relationships)
            )
            
        except Exception as e:
            logger.error("graph_initialization_error", error=str(e))
            raise
    
    async def _add_table_metadata(self, table_name: str):
        """
        Agrega metadata de una tabla al grafo
        """
        try:
            # Obtener row count estimado
            # Obtener row count estimado
            try:
                # Usar método síncrono directamente
                row_count = 0
                if db_manager.db_type == "mysql":
                    query = f"""
                    SELECT TABLE_ROWS as estimate
                    FROM information_schema.TABLES
                    WHERE TABLE_SCHEMA = DATABASE()
                    AND TABLE_NAME = '{table_name}'
                    """
                    with db_manager.get_session() as session:
                        from sqlalchemy import text
                        result = session.execute(text(query))
                        row = result.fetchone()
                        if row:
                            row_count = int(row[0] or 0)
            except:
                row_count = 0
            
            # Obtener schema básico
            schema = db_manager.get_table_schema(table_name)
            
            self.table_metadata[table_name] = {
                "name": table_name,
                "row_count": row_count,
                "column_count": len(schema.get("columns", [])),
                "has_pk": bool(schema.get("primary_key", {}).get("constrained_columns")),
                "fk_count": len(schema.get("foreign_keys", []))
            }
            
        except Exception as e:
            logger.error("add_table_metadata_error", table=table_name, error=str(e))
    
    async def _discover_relationships(self, table_name: str):
        """
        Descubre todas las relaciones de una tabla
        """
        try:
            # Foreign Keys explícitas
            fks = db_manager.get_foreign_keys(table_name)
            
            for fk in fks:
                referred_table = fk.get("referred_table")
                
                if not referred_table:
                    continue
                
                # Extraer columnas
                constrained_cols = fk.get("constrained_columns", [])
                referred_cols = fk.get("referred_columns", [])
                
                if not constrained_cols or not referred_cols:
                    continue
                
                relationship = {
                    "type": "foreign_key",
                    "from_table": table_name,
                    "from_column": constrained_cols[0],
                    "to_table": referred_table,
                    "to_column": referred_cols[0],
                    "confidence": 1.0,
                    "cardinality": "many_to_one"
                }
                
                # Agregar al grafo (dirección forward)
                self.graph[table_name].append((referred_table, relationship))
                
                # Agregar al grafo inverso (dirección backward)
                reverse_relationship = relationship.copy()
                reverse_relationship["cardinality"] = "one_to_many"
                self.reverse_graph[referred_table].append((table_name, reverse_relationship))
                
                # Guardar en lista de relaciones
                self.relationships.append(relationship)
                
                logger.debug(
                    "relationship_discovered",
                    from_table=table_name,
                    to_table=referred_table,
                    type="fk"
                )
                
        except Exception as e:
            logger.error("discover_relationships_error", table=table_name, error=str(e))
    
    def get_k_hop_neighbors(
        self,
        start_table: str,
        k: int = 2,
        bidirectional: bool = True,
        max_neighbors_per_level: int = 10
    ) -> Dict[int, List[Tuple[str, Dict[str, Any]]]]:
        """
        Obtiene vecinos K-Hop desde una tabla inicial usando BFS
        
        Args:
            start_table: Tabla inicial
            k: Profundidad máxima (número de saltos)
            bidirectional: Si explorar en ambas direcciones (FK y reverse)
            max_neighbors_per_level: Máximo de vecinos por nivel
            
        Returns:
            Diccionario {nivel: [(tabla, relacion_info)]}
        """
        if not self.initialized:
            logger.warning("graph_not_initialized")
            return {}
        
        if start_table not in self.table_metadata:
            logger.warning("start_table_not_found", table=start_table)
            return {}
        
        # BFS
        visited = {start_table}
        queue = deque([(start_table, 0, None)])  # (tabla, nivel, relacion)
        neighbors_by_level = defaultdict(list)
        
        while queue:
            current_table, level, relationship = queue.popleft()
            
            # Límite de profundidad
            if level >= k:
                continue
            
            # Obtener vecinos directos
            direct_neighbors = []
            
            # Forward (FK)
            if current_table in self.graph:
                direct_neighbors.extend(self.graph[current_table])
            
            # Backward (reverse FK) si bidireccional
            if bidirectional and current_table in self.reverse_graph:
                direct_neighbors.extend(self.reverse_graph[current_table])
            
            # Limitar vecinos por nivel
            if len(direct_neighbors) > max_neighbors_per_level:
                # Priorizar por row_count (tablas más grandes primero)
                direct_neighbors = sorted(
                    direct_neighbors,
                    key=lambda x: self.table_metadata.get(x[0], {}).get("row_count", 0),
                    reverse=True
                )[:max_neighbors_per_level]
            
            # Agregar vecinos no visitados
            for neighbor_table, rel_info in direct_neighbors:
                if neighbor_table not in visited:
                    visited.add(neighbor_table)
                    next_level = level + 1
                    
                    # Agregar a resultado
                    neighbors_by_level[next_level].append((neighbor_table, rel_info))
                    
                    # Agregar a cola para seguir explorando
                    queue.append((neighbor_table, next_level, rel_info))
        
        logger.info(
            "k_hop_computed",
            start_table=start_table,
            k=k,
            levels=len(neighbors_by_level),
            total_neighbors=sum(len(v) for v in neighbors_by_level.values())
        )
        
        return dict(neighbors_by_level)
    
    def get_path_between_tables(
        self,
        table_a: str,
        table_b: str,
        max_depth: int = 3
    ) -> Optional[List[Dict[str, Any]]]:
        """
        Encuentra el camino más corto entre dos tablas
        
        Args:
            table_a: Tabla origen
            table_b: Tabla destino
            max_depth: Profundidad máxima de búsqueda
            
        Returns:
            Lista de relaciones que conectan las tablas, o None si no hay camino
        """
        if not self.initialized:
            return None
        
        if table_a not in self.table_metadata or table_b not in self.table_metadata:
            return None
        
        # BFS para encontrar camino más corto
        visited = {table_a}
        queue = deque([(table_a, [])])  # (tabla_actual, camino)
        
        while queue:
            current_table, path = queue.popleft()
            
            # Límite de profundidad
            if len(path) >= max_depth:
                continue
            
            # ¿Llegamos al destino?
            if current_table == table_b:
                return path
            
            # Explorar vecinos (bidireccional)
            neighbors = []
            if current_table in self.graph:
                neighbors.extend(self.graph[current_table])
            if current_table in self.reverse_graph:
                neighbors.extend(self.reverse_graph[current_table])
            
            for neighbor_table, rel_info in neighbors:
                if neighbor_table not in visited:
                    visited.add(neighbor_table)
                    new_path = path + [rel_info]
                    queue.append((neighbor_table, new_path))
        
        # No se encontró camino
        logger.info("no_path_found", table_a=table_a, table_b=table_b)
        return None
    
    def get_all_relationships(self) -> List[Dict[str, Any]]:
        """
        Retorna todas las relaciones descubiertas
        """
        return self.relationships
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene información de una tabla
        """
        return self.table_metadata.get(table_name)
    
    def get_connected_tables(self, table_name: str) -> List[str]:
        """
        Obtiene todas las tablas directamente conectadas
        """
        connected = set()
        
        # Forward
        if table_name in self.graph:
            connected.update(t for t, _ in self.graph[table_name])
        
        # Backward
        if table_name in self.reverse_graph:
            connected.update(t for t, _ in self.reverse_graph[table_name])
        
        return list(connected)
    
    def get_graph_stats(self) -> Dict[str, Any]:
        """
        Obtiene estadísticas del grafo
        """
        return {
            "tables_count": len(self.table_metadata),
            "relationships_count": len(self.relationships),
            "avg_connections_per_table": (
                len(self.relationships) / len(self.table_metadata)
                if self.table_metadata else 0
            ),
            "most_connected_tables": sorted(
                [
                    (table, len(self.get_connected_tables(table)))
                    for table in self.table_metadata.keys()
                ],
                key=lambda x: x[1],
                reverse=True
            )[:5],
            "initialized": self.initialized
        }


# Instancia global
db_graph = DatabaseGraph()