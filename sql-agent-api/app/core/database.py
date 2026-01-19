"""
Gestor de conexión a la base de datos
"""
from sqlalchemy import create_engine, text, inspect
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from typing import List, Dict, Any, Optional
import structlog
from app.core.config import settings

logger = structlog.get_logger()


class DatabaseManager:
    """
    Gestor de conexión y operaciones con la base de datos
    Compatible con MySQL y PostgreSQL
    """
    
    def __init__(self, database_url: str = None):
        """
        Inicializa el gestor de base de datos
        
        Args:
            database_url: URL de conexión (usa settings si no se provee)
        """
        self.database_url = database_url or settings.DATABASE_URL
        
        # Detectar tipo de base de datos
        self.db_type = "mysql" if "mysql" in self.database_url.lower() else "postgresql"
        
        # Engine síncrono (para exploración)
        self.engine = create_engine(
            self.database_url,
            pool_pre_ping=True,
            pool_size=5,
            max_overflow=10
        )
        
        # Session maker
        self.SessionLocal = sessionmaker(
            autocommit=False,
            autoflush=False,
            bind=self.engine
        )
        
        logger.info(
            "database_init",
            database_url=self.database_url.split("@")[-1],
            db_type=self.db_type
        )
    
    def get_session(self) -> Session:
        """
        Obtiene una sesión de base de datos
        """
        return self.SessionLocal()
    
    async def execute_query(
        self, 
        query: str, 
        params: Optional[Dict[str, Any]] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Ejecuta una query SQL y retorna resultados
        Maneja conversión de tipos especiales (date, datetime, Decimal)
        VERSION ASYNC CORREGIDA
        
        Args:
            query: Query SQL a ejecutar
            params: Parámetros de la query
            limit: Límite de filas a retornar
            
        Returns:
            Lista de diccionarios con los resultados
        """
        try:
            from decimal import Decimal
            from datetime import date, datetime
            
            # 🔥 EJECUTAR EN THREAD SEPARADO para no bloquear
            import asyncio
            
            def _execute_sync():
                with self.get_session() as session:
                    # Agregar LIMIT si no existe
                    upper_query = query.upper()

                    if (
                        "LIMIT" not in upper_query
                        and "COUNT(" not in upper_query
                        and "SUM(" not in upper_query
                        and "AVG(" not in upper_query
                        and "MAX(" not in upper_query
                        and "MIN(" not in upper_query
                    ):
                        modified_query = f"{query.rstrip(';')} LIMIT {limit}"
                    else:
                        modified_query = query

                    result = session.execute(text(modified_query), params or {})
                    
                    # Convertir a lista de diccionarios
                    columns = result.keys()
                    rows = []
                    
                    for row in result.fetchall():
                        row_dict = {}
                        for col, val in zip(columns, row):
                            # Convertir tipos especiales a formatos serializables
                            if isinstance(val, datetime):
                                row_dict[col] = val.isoformat()
                            elif isinstance(val, date):
                                row_dict[col] = val.isoformat()
                            elif isinstance(val, Decimal):
                                row_dict[col] = float(val)
                            elif isinstance(val, bytes):
                                row_dict[col] = val.decode('utf-8', errors='ignore')
                            else:
                                row_dict[col] = val
                        
                        rows.append(row_dict)
                    
                    return rows
            
            # Ejecutar en thread pool
            rows = await asyncio.to_thread(_execute_sync)
            
            logger.info(
                "query_executed",
                rows_returned=len(rows),
                query_preview=query[:100]
            )
            
            return rows
                
        except Exception as e:
            logger.error("query_error", error=str(e), query=query[:100])
            raise

    def get_all_tables(self) -> List[str]:
        """
        Obtiene lista de todas las tablas en la base de datos
        Compatible con MySQL y PostgreSQL
        
        Returns:
            Lista de nombres de tablas
        """
        try:
            inspector = inspect(self.engine)
            tables = inspector.get_table_names()
            
            logger.info("tables_retrieved", count=len(tables), db_type=self.db_type)
            return tables
            
        except Exception as e:
            logger.error("tables_error", error=str(e))
            raise
    
    def get_table_schema(self, table_name: str) -> Dict[str, Any]:
        """
        Obtiene el schema de una tabla específica
        
        Args:
            table_name: Nombre de la tabla
            
        Returns:
            Diccionario con información del schema
        """
        try:
            inspector = inspect(self.engine)
            
            # Columnas
            columns = inspector.get_columns(table_name)
            
            # Primary keys
            pk = inspector.get_pk_constraint(table_name)
            
            # Foreign keys
            fks = inspector.get_foreign_keys(table_name)
            
            # Indexes
            indexes = inspector.get_indexes(table_name)
            
            schema = {
                "table_name": table_name,
                "columns": columns,
                "primary_key": pk,
                "foreign_keys": fks,
                "indexes": indexes
            }
            
            logger.info(
                "schema_retrieved",
                table=table_name,
                columns_count=len(columns)
            )
            
            return schema
            
        except Exception as e:
            logger.error("schema_error", table=table_name, error=str(e))
            raise
    
    def get_foreign_keys(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Obtiene las foreign keys de una tabla
        
        Args:
            table_name: Nombre de la tabla
            
        Returns:
            Lista de foreign keys
        """
        try:
            inspector = inspect(self.engine)
            fks = inspector.get_foreign_keys(table_name)
            
            logger.info("fks_retrieved", table=table_name, count=len(fks))
            return fks
            
        except Exception as e:
            logger.error("fks_error", table=table_name, error=str(e))
            raise
    
    def get_sample_data(
        self, 
        table_name: str, 
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Obtiene datos de ejemplo de una tabla
        
        Args:
            table_name: Nombre de la tabla
            limit: Número de filas a obtener
            
        Returns:
            Lista de diccionarios con datos de ejemplo
        """
        try:
            query = f"SELECT * FROM {table_name} LIMIT {limit}"
            return self.execute_query(query, limit=limit)
            
        except Exception as e:
            logger.error("sample_data_error", table=table_name, error=str(e))
            raise
    
    def get_table_row_count(self, table_name: str) -> int:
        """
        Obtiene el conteo de filas de una tabla
        Optimizado según el tipo de BD
        
        Args:
            table_name: Nombre de la tabla
            
        Returns:
            Número estimado de filas
        """
        try:
            if self.db_type == "mysql":
                query = f"""
                SELECT TABLE_ROWS as estimate
                FROM information_schema.TABLES
                WHERE TABLE_SCHEMA = DATABASE()
                AND TABLE_NAME = '{table_name}'
                """
            else:  # PostgreSQL
                query = f"""
                SELECT reltuples::bigint AS estimate 
                FROM pg_class 
                WHERE relname = '{table_name}'
                """
            
            result = self.execute_query(query, limit=1)
            
            if result and len(result) > 0:
                return int(result[0].get('estimate', 0))
            
            return 0
            
        except Exception as e:
            logger.error("row_count_error", table=table_name, error=str(e))
            return 0
    
    def test_connection(self) -> bool:
        """
        Prueba la conexión a la base de datos
        
        Returns:
            True si la conexión es exitosa
        """
        try:
            with self.get_session() as session:
                session.execute(text("SELECT 1"))
            
            logger.info("connection_test", status="success", db_type=self.db_type)
            return True
            
        except Exception as e:
            logger.error("connection_test", status="failed", error=str(e))
            return False
    
    def close(self):
        """
        Cierra las conexiones a la base de datos
        """
        self.engine.dispose()
        logger.info("database_closed")


# Instancia global
db_manager = DatabaseManager()