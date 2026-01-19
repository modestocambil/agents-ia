"""
Knowledge Graph Storage - Almacenamiento de aprendizajes
"""
from typing import Dict, Any, List, Optional
import json
from datetime import datetime
import structlog

logger = structlog.get_logger()


class KnowledgeGraphStorage:
    """
    Almacenamiento en memoria de aprendizajes del sistema
    En producción esto debería usar Redis + PostgreSQL + ChromaDB
    """
    
    def __init__(self):
        # Almacenamiento en memoria (temporal)
        self.semantic_mappings = {}  # término_usuario -> término_bd
        self.field_semantics = {}     # tabla.campo -> significado
        self.query_patterns = {}      # patrón_query -> solución
        self.business_rules = {}      # regla -> definición
        
        logger.info("knowledge_graph_initialized", storage_type="in_memory")
    
    async def store_semantic_mapping(
        self,
        user_term: str,
        db_table: str,
        db_field: Optional[str] = None,
        confidence: float = 0.9,
        context: Optional[Dict[str, Any]] = None
    ) -> bool:
        """
        Almacena un mapeo semántico usuario -> BD
        SOPORTA MÚLTIPLES TABLAS POR TÉRMINO
        
        Args:
            user_term: Término que usa el usuario
            db_table: Tabla de la BD
            db_field: Campo específico (opcional)
            confidence: Nivel de confianza (0-1)
            context: Contexto adicional
            
        Returns:
            True si se almacenó correctamente
        """
        try:
            mapping_key = user_term.lower().strip()
            
            # Si no existe el término, crear lista
            if mapping_key not in self.semantic_mappings:
                self.semantic_mappings[mapping_key] = []
            
            # Verificar si ya existe este mapeo específico
            existing = False
            for mapping in self.semantic_mappings[mapping_key]:
                if mapping["db_table"] == db_table and mapping.get("db_field") == db_field:
                    # Actualizar confidence si ya existe
                    mapping["confidence"] = confidence
                    mapping["usage_count"] += 1
                    existing = True
                    break
            
            # Si no existe, agregar nuevo mapeo
            if not existing:
                new_mapping = {
                    "user_term": user_term,
                    "db_table": db_table,
                    "db_field": db_field,
                    "confidence": confidence,
                    "context": context or {},
                    "created_at": datetime.now().isoformat(),
                    "usage_count": 0
                }
                self.semantic_mappings[mapping_key].append(new_mapping)
            
            logger.info(
                "semantic_mapping_stored",
                user_term=user_term,
                db_table=db_table,
                confidence=confidence,
                total_mappings=len(self.semantic_mappings[mapping_key])
            )
            
            return True
            
        except Exception as e:
            logger.error("store_mapping_error", error=str(e))
            return False
        

    async def get_semantic_mapping(self, user_term: str) -> Optional[List[Dict[str, Any]]]:
        """
        Obtiene TODOS los mapeos semánticos de un término
        
        Args:
            user_term: Término del usuario
            
        Returns:
            Lista de diccionarios con los mapeos o None
        """
        mapping_key = user_term.lower().strip()
        mappings = self.semantic_mappings.get(mapping_key)
        
        if mappings:
            # Incrementar contador de uso en todos
            for mapping in mappings:
                mapping["usage_count"] += 1
            
            logger.info(
                "semantic_mappings_retrieved",
                user_term=user_term,
                count=len(mappings),
                tables=[m["db_table"] for m in mappings]
            )
        
        return mappings

    async def store_field_semantic(
        self,
        table_name: str,
        field_name: str,
        business_meaning: str,
        possible_values: Optional[Dict[str, str]] = None,
        confidence: float = 0.9
    ) -> bool:
        """
        Almacena el significado de negocio de un campo
        
        Args:
            table_name: Nombre de la tabla
            field_name: Nombre del campo
            business_meaning: Significado de negocio
            possible_values: Valores posibles y sus significados
            confidence: Nivel de confianza
            
        Returns:
            True si se almacenó correctamente
        """
        try:
            key = f"{table_name}.{field_name}".lower()
            
            self.field_semantics[key] = {
                "table_name": table_name,
                "field_name": field_name,
                "business_meaning": business_meaning,
                "possible_values": possible_values or {},
                "confidence": confidence,
                "created_at": datetime.now().isoformat(),
                "usage_count": 0
            }
            
            logger.info(
                "field_semantic_stored",
                table=table_name,
                field=field_name
            )
            
            return True
            
        except Exception as e:
            logger.error("store_field_semantic_error", error=str(e))
            return False
    
    async def get_field_semantic(
        self, 
        table_name: str, 
        field_name: str
    ) -> Optional[Dict[str, Any]]:
        """
        Obtiene el significado de un campo
        
        Args:
            table_name: Nombre de la tabla
            field_name: Nombre del campo
            
        Returns:
            Diccionario con el significado o None
        """
        key = f"{table_name}.{field_name}".lower()
        semantic = self.field_semantics.get(key)
        
        if semantic:
            semantic["usage_count"] += 1
        
        return semantic
    
    async def store_query_pattern(
        self,
        query_intent: str,
        tables_used: List[str],
        joins_used: List[str],
        sql_template: str,
        success: bool = True,
        confidence: float = 0.9
    ) -> bool:
        """
        Almacena un patrón de query exitoso
        
        Args:
            query_intent: Intención de la query
            tables_used: Tablas utilizadas
            joins_used: JOINs utilizados
            sql_template: Template del SQL
            success: Si fue exitoso
            confidence: Nivel de confianza
            
        Returns:
            True si se almacenó correctamente
        """
        try:
            key = query_intent.lower().strip()
            
            if key not in self.query_patterns:
                self.query_patterns[key] = {
                    "query_intent": query_intent,
                    "tables_used": tables_used,
                    "joins_used": joins_used,
                    "sql_template": sql_template,
                    "success": success,
                    "confidence": confidence,
                    "created_at": datetime.now().isoformat(),
                    "usage_count": 0
                }
            else:
                # Actualizar si existe
                self.query_patterns[key]["usage_count"] += 1
                self.query_patterns[key]["confidence"] = min(
                    1.0, 
                    self.query_patterns[key]["confidence"] + 0.05
                )
            
            logger.info(
                "query_pattern_stored",
                intent=query_intent,
                tables_count=len(tables_used)
            )
            
            return True
            
        except Exception as e:
            logger.error("store_pattern_error", error=str(e))
            return False
    
    async def get_query_pattern(self, query_intent: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene un patrón de query
        
        Args:
            query_intent: Intención de la query
            
        Returns:
            Diccionario con el patrón o None
        """
        key = query_intent.lower().strip()
        pattern = self.query_patterns.get(key)
        
        if pattern:
            pattern["usage_count"] += 1
        
        return pattern
    
    async def store_business_rule(
        self,
        rule_name: str,
        rule_definition: str,
        tables_involved: List[str],
        formula: Optional[str] = None,
        confidence: float = 0.9
    ) -> bool:
        """
        Almacena una regla de negocio
        
        Args:
            rule_name: Nombre de la regla
            rule_definition: Definición de la regla
            tables_involved: Tablas involucradas
            formula: Fórmula si aplica
            confidence: Nivel de confianza
            
        Returns:
            True si se almacenó correctamente
        """
        try:
            key = rule_name.lower().strip()
            
            self.business_rules[key] = {
                "rule_name": rule_name,
                "rule_definition": rule_definition,
                "tables_involved": tables_involved,
                "formula": formula,
                "confidence": confidence,
                "created_at": datetime.now().isoformat(),
                "usage_count": 0
            }
            
            logger.info("business_rule_stored", rule=rule_name)
            
            return True
            
        except Exception as e:
            logger.error("store_rule_error", error=str(e))
            return False
    
    async def get_business_rule(self, rule_name: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene una regla de negocio
        
        Args:
            rule_name: Nombre de la regla
            
        Returns:
            Diccionario con la regla o None
        """
        key = rule_name.lower().strip()
        rule = self.business_rules.get(key)
        
        if rule:
            rule["usage_count"] += 1
        
        return rule
    
    def get_all_mappings(self) -> Dict[str, Any]:
        """
        Obtiene todos los mapeos almacenados
        
        Returns:
            Diccionario con todos los aprendizajes
        """
        return {
            "semantic_mappings": self.semantic_mappings,
            "field_semantics": self.field_semantics,
            "query_patterns": self.query_patterns,
            "business_rules": self.business_rules,
            "total_learnings": (
                len(self.semantic_mappings) +
                len(self.field_semantics) +
                len(self.query_patterns) +
                len(self.business_rules)
            )
        }
    
    def clear_all(self):
        """
        Limpia todo el almacenamiento (para testing)
        """
        self.semantic_mappings = {}
        self.field_semantics = {}
        self.query_patterns = {}
        self.business_rules = {}
        
        logger.warning("knowledge_graph_cleared")


# Instancia global
#kg_storage = KnowledgeGraphStorage()

try:
    from app.knowledge_graph.persistent_storage import persistent_kg_storage
    kg_storage = persistent_kg_storage
    logger.info("using_persistent_storage", type="mysql")
except Exception as e:
    logger.warning("persistent_storage_unavailable", error=str(e), fallback="in_memory")
    kg_storage = KnowledgeGraphStorage()