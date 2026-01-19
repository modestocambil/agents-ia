
from typing import Dict, List, Any, Optional
from datetime import datetime
import structlog
from app.core.database import db_manager
from sqlalchemy import text

logger = structlog.get_logger()


class PersistentKnowledgeGraphStorage:
    """
    Almacenamiento persistente en MySQL del Knowledge Graph
    """
    
    def __init__(self):
        self.db = db_manager
        logger.info("persistent_knowledge_graph_initialized", storage_type="mysql")
    
    async def store_semantic_mapping(
        self,
        user_term: str,
        db_table: str,
        db_field: Optional[str] = None,
        confidence: float = 0.9,
        context: Optional[Dict[str, Any]] = None,
        created_by: Optional[str] = None
    ) -> bool:
        """
        Almacena un mapeo semántico en MySQL
        SOPORTA MÚLTIPLES TABLAS POR TÉRMINO
        """
        try:
            import json
            
            # Verificar si ya existe
            check_query = text("""
                SELECT id, usage_count 
                FROM kg_semantic_mappings 
                WHERE user_term = :user_term 
                AND db_table = :db_table 
                AND (db_field = :db_field OR (db_field IS NULL AND :db_field IS NULL))
            """)
            
            with self.db.get_session() as session:
                result = session.execute(check_query, {
                    "user_term": user_term.lower().strip(),
                    "db_table": db_table,
                    "db_field": db_field
                })
                existing = result.fetchone()
                
                if existing:
                    # Actualizar existente
                    update_query = text("""
                        UPDATE kg_semantic_mappings 
                        SET confidence = :confidence,
                            usage_count = usage_count + 1,
                            updated_at = NOW()
                        WHERE id = :id
                    """)
                    session.execute(update_query, {
                        "confidence": confidence,
                        "id": existing[0]
                    })
                else:
                    # Insertar nuevo
                    insert_query = text("""
                        INSERT INTO kg_semantic_mappings 
                        (user_term, db_table, db_field, confidence, context, created_by)
                        VALUES (:user_term, :db_table, :db_field, :confidence, :context, :created_by)
                    """)
                    session.execute(insert_query, {
                        "user_term": user_term.lower().strip(),
                        "db_table": db_table,
                        "db_field": db_field,
                        "confidence": confidence,
                        "context": json.dumps(context) if context else None,
                        "created_by": created_by
                    })
                
                session.commit()
            
            logger.info(
                "semantic_mapping_stored",
                user_term=user_term,
                db_table=db_table,
                db_field=db_field
            )
            
            return True
            
        except Exception as e:
            logger.error("store_mapping_error", error=str(e))
            return False
    
    async def get_semantic_mapping(self, user_term: str) -> Optional[List[Dict[str, Any]]]:
        """
        Obtiene TODOS los mapeos semánticos de un término desde MySQL
        """
        try:
            query = text("""
                SELECT id, user_term, db_table, db_field, confidence, context, usage_count
                FROM kg_semantic_mappings
                WHERE user_term = :user_term
                ORDER BY confidence DESC
            """)
            
            with self.db.get_session() as session:
                result = session.execute(query, {"user_term": user_term.lower().strip()})
                rows = result.fetchall()
                
                if not rows:
                    return None
                
                mappings = []
                for row in rows:
                    import json
                    mapping = {
                        "id": row[0],
                        "user_term": row[1],
                        "db_table": row[2],
                        "db_field": row[3],
                        "confidence": float(row[4]),
                        "context": json.loads(row[5]) if row[5] else {},
                        "usage_count": row[6]
                    }
                    mappings.append(mapping)
                    
                    # Incrementar usage_count
                    update_query = text("""
                        UPDATE kg_semantic_mappings 
                        SET usage_count = usage_count + 1 
                        WHERE id = :id
                    """)
                    session.execute(update_query, {"id": row[0]})
                
                session.commit()
                
                logger.info(
                    "semantic_mappings_retrieved",
                    user_term=user_term,
                    count=len(mappings),
                    tables=[m["db_table"] for m in mappings]
                )
                
                return mappings
            
        except Exception as e:
            logger.error("get_mapping_error", error=str(e))
            return None
    
    async def store_business_rule(
        self,
        rule_name: str,
        rule_definition: str,
        tables_involved: List[str],
        formula: Optional[str] = None,
        confidence: float = 0.9,
        created_by: Optional[str] = None
    ) -> bool:
        """
        Almacena una regla de negocio en MySQL
        """
        try:
            with self.db.get_session() as session:
                # Verificar si ya existe
                check_query = text("""
                    SELECT id FROM kg_business_rules WHERE rule_name = :rule_name
                """)
                result = session.execute(check_query, {"rule_name": rule_name})
                existing = result.fetchone()
                
                if existing:
                    # Actualizar existente
                    update_query = text("""
                        UPDATE kg_business_rules 
                        SET rule_definition = :rule_definition,
                            formula = :formula,
                            confidence = :confidence,
                            usage_count = usage_count + 1,
                            updated_at = NOW()
                        WHERE id = :id
                    """)
                    session.execute(update_query, {
                        "rule_definition": rule_definition,
                        "formula": formula,
                        "confidence": confidence,
                        "id": existing[0]
                    })
                    rule_id = existing[0]
                else:
                    # Insertar nueva regla
                    insert_query = text("""
                        INSERT INTO kg_business_rules 
                        (rule_name, rule_definition, formula, confidence, created_by)
                        VALUES (:rule_name, :rule_definition, :formula, :confidence, :created_by)
                    """)
                    result = session.execute(insert_query, {
                        "rule_name": rule_name,
                        "rule_definition": rule_definition,
                        "formula": formula,
                        "confidence": confidence,
                        "created_by": created_by
                    })
                    rule_id = result.lastrowid
                
                # Limpiar relaciones antiguas
                delete_query = text("""
                    DELETE FROM kg_business_rules_tables WHERE business_rule_id = :rule_id
                """)
                session.execute(delete_query, {"rule_id": rule_id})
                
                # Insertar nuevas relaciones con tablas
                if tables_involved:
                    insert_table_query = text("""
                        INSERT INTO kg_business_rules_tables (business_rule_id, table_name)
                        VALUES (:rule_id, :table_name)
                    """)
                    for table in tables_involved:
                        session.execute(insert_table_query, {
                            "rule_id": rule_id,
                            "table_name": table
                        })
                
                session.commit()
            
            logger.info("business_rule_stored", rule=rule_name)
            return True
            
        except Exception as e:
            logger.error("store_rule_error", error=str(e))
            return False
    
    async def get_business_rule(self, rule_name: str) -> Optional[Dict[str, Any]]:
        """
        Obtiene una regla de negocio específica desde MySQL
        """
        try:
            query = text("""
                SELECT br.id, br.rule_name, br.rule_definition, br.formula, 
                       br.confidence, br.usage_count
                FROM kg_business_rules br
                WHERE br.rule_name = :rule_name AND br.is_active = TRUE
            """)
            
            with self.db.get_session() as session:
                result = session.execute(query, {"rule_name": rule_name})
                row = result.fetchone()
                
                if not row:
                    return None
                
                # Obtener tablas asociadas
                tables_query = text("""
                    SELECT table_name 
                    FROM kg_business_rules_tables 
                    WHERE business_rule_id = :rule_id
                """)
                tables_result = session.execute(tables_query, {"rule_id": row[0]})
                tables = [t[0] for t in tables_result.fetchall()]
                
                rule = {
                    "rule_name": row[1],
                    "rule_definition": row[2],
                    "formula": row[3],
                    "confidence": float(row[4]),
                    "usage_count": row[5],
                    "tables_involved": tables
                }
                
                # Incrementar usage_count
                update_query = text("""
                    UPDATE kg_business_rules 
                    SET usage_count = usage_count + 1 
                    WHERE id = :id
                """)
                session.execute(update_query, {"id": row[0]})
                session.commit()
                
                return rule
            
        except Exception as e:
            logger.error("get_rule_error", error=str(e))
            return None
    
    def get_all_mappings(self) -> Dict[str, Any]:
        """
        Obtiene todos los aprendizajes desde MySQL
        """
        try:
            with self.db.get_session() as session:
                # Semantic mappings
                mappings_query = text("""
                    SELECT user_term, db_table, db_field, confidence, usage_count
                    FROM kg_semantic_mappings
                    ORDER BY user_term, confidence DESC
                """)
                mappings_result = session.execute(mappings_query)
                
                semantic_mappings = {}
                for row in mappings_result.fetchall():
                    term = row[0]
                    if term not in semantic_mappings:
                        semantic_mappings[term] = []
                    
                    semantic_mappings[term].append({
                        "db_table": row[1],
                        "db_field": row[2],
                        "confidence": float(row[3]),
                        "usage_count": row[4]
                    })
                
                # Business rules
                rules_query = text("""
                    SELECT br.rule_name, br.rule_definition, br.formula, 
                           br.confidence, br.usage_count
                    FROM kg_business_rules br
                    WHERE br.is_active = TRUE
                """)
                rules_result = session.execute(rules_query)
                
                business_rules = {}
                for row in rules_result.fetchall():
                    rule_name = row[0]
                    
                    # Obtener tablas
                    tables_query = text("""
                        SELECT table_name 
                        FROM kg_business_rules_tables brt
                        JOIN kg_business_rules br ON brt.business_rule_id = br.id
                        WHERE br.rule_name = :rule_name
                    """)
                    tables_result = session.execute(tables_query, {"rule_name": rule_name})
                    tables = [t[0] for t in tables_result.fetchall()]
                    
                    business_rules[rule_name] = {
                        "rule_name": rule_name,
                        "rule_definition": row[1],
                        "formula": row[2],
                        "confidence": float(row[3]),
                        "usage_count": row[4],
                        "tables_involved": tables
                    }
            
            return {
                "semantic_mappings": semantic_mappings,
                "field_semantics": {},  # TODO: implementar si es necesario
                "query_patterns": {},   # TODO: implementar si es necesario
                "business_rules": business_rules,
                "total_learnings": (
                    len(semantic_mappings) +
                    len(business_rules)
                )
            }
            
        except Exception as e:
            logger.error("get_all_mappings_error", error=str(e))
            return {
                "semantic_mappings": {},
                "business_rules": {},
                "total_learnings": 0
            }
    
    async def store_field_semantic(
        self,
        table_name: str,
        field_name: str,
        business_meaning: str,
        possible_values: Optional[Dict[str, str]] = None,
        confidence: float = 0.9
    ) -> bool:
        """
        Almacena la semántica de un campo en MySQL
        """
        try:
            import json
            
            with self.db.get_session() as session:
                # Verificar si existe
                check_query = text("""
                    SELECT id FROM kg_field_semantics 
                    WHERE table_name = :table_name AND field_name = :field_name
                """)
                result = session.execute(check_query, {
                    "table_name": table_name,
                    "field_name": field_name
                })
                existing = result.fetchone()
                
                if existing:
                    # Actualizar
                    update_query = text("""
                        UPDATE kg_field_semantics 
                        SET business_meaning = :business_meaning,
                            possible_values = :possible_values,
                            confidence = :confidence,
                            usage_count = usage_count + 1,
                            updated_at = NOW()
                        WHERE id = :id
                    """)
                    session.execute(update_query, {
                        "business_meaning": business_meaning,
                        "possible_values": json.dumps(possible_values) if possible_values else None,
                        "confidence": confidence,
                        "id": existing[0]
                    })
                else:
                    # Insertar
                    insert_query = text("""
                        INSERT INTO kg_field_semantics 
                        (table_name, field_name, business_meaning, possible_values, confidence)
                        VALUES (:table_name, :field_name, :business_meaning, :possible_values, :confidence)
                    """)
                    session.execute(insert_query, {
                        "table_name": table_name,
                        "field_name": field_name,
                        "business_meaning": business_meaning,
                        "possible_values": json.dumps(possible_values) if possible_values else None,
                        "confidence": confidence
                    })
                
                session.commit()
            
            logger.info("field_semantic_stored", table=table_name, field=field_name)
            return True
            
        except Exception as e:
            logger.error("store_field_semantic_error", error=str(e))
            return False
    
    def clear_all(self):
        """
        Limpia todo el almacenamiento (para testing)
        """
        try:
            with self.db.get_session() as session:
                session.execute(text("DELETE FROM kg_business_rules_tables"))
                session.execute(text("DELETE FROM kg_business_rules"))
                session.execute(text("DELETE FROM kg_semantic_mappings"))
                session.execute(text("DELETE FROM kg_field_semantics"))
                session.commit()
            
            logger.warning("knowledge_graph_cleared")
            
        except Exception as e:
            logger.error("clear_all_error", error=str(e))


# Instancia global
persistent_kg_storage = PersistentKnowledgeGraphStorage()