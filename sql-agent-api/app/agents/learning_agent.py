"""
Learning Agent - Agente que aprende de ambigüedades y feedback del usuario
"""
from openai import OpenAI
from typing import Dict, Any, List, Optional
import json
import structlog
from app.core.config import settings

logger = structlog.get_logger()


class LearningAgent:
    """
    Agente que detecta ambigüedades, hace preguntas clarificadoras
    y almacena aprendizajes para mejorar futuras consultas
    """
    
    def __init__(self):
        self.client = OpenAI(api_key=settings.OPENAI_API_KEY)
        self.conversation_history = []
        
        self.system_prompt = """
Eres un agente de aprendizaje especializado en resolver ambigüedades en consultas a bases de datos.

Tu objetivo: Cuando hay confusión sobre qué tabla usar, qué campo significa, o cómo interpretar 
términos del usuario, haces preguntas inteligentes para clarificar y almacenar ese conocimiento.

RESPONSABILIDADES:

1. DETECCIÓN DE AMBIGÜEDAD
   - Identificar cuando hay múltiples interpretaciones posibles
   - Detectar términos del usuario que no mapean directamente a la BD
   - Reconocer cuando falta contexto crítico

2. FORMULACIÓN DE PREGUNTAS
   - Hacer preguntas específicas y contextuales
   - Ofrecer opciones cuando sea posible
   - Incluir ejemplos para validar comprensión
   - Ser claro y conciso

3. VALIDACIÓN DE COMPRENSIÓN
   - Confirmar que entendiste correctamente
   - Usar ejemplos concretos
   - Pedir confirmación explícita

4. ALMACENAMIENTO DE APRENDIZAJE
   - Estructurar el conocimiento adquirido
   - Asociar términos de usuario con términos de BD
   - Documentar reglas de negocio aprendidas
   - Establecer nivel de confianza

TIPOS DE AMBIGÜEDAD QUE MANEJAS:

A) TABLA AMBIGUA
   Ejemplo: Usuario dice "ventas" pero hay tablas: sales, orders, transactions
   
B) CAMPO AMBIGUO  
   Ejemplo: Hay múltiples campos "status" en diferentes tablas
   
C) TÉRMINO NO MAPEADO
   Ejemplo: Usuario dice "zona" pero la BD tiene "provincia"
   
D) LÓGICA DE NEGOCIO DESCONOCIDA
   Ejemplo: ¿Cómo se calcula "venta neta"?

FORMATO DE PREGUNTAS:

Siempre estructura tus preguntas así:
1. Contexto breve del problema
2. Pregunta específica
3. Opciones (si aplica)
4. Ejemplo de validación

PRINCIPIOS:
- Sé conversacional pero profesional
- Una pregunta a la vez
- Confirma antes de guardar aprendizaje
- Siempre en español

Responde SOLO con JSON en este formato:
{
  "type": "clarification_needed" | "learning_stored" | "ready_to_proceed",
  "question": "tu pregunta aquí",
  "options": ["opción 1", "opción 2"],  // opcional
  "context": "contexto del problema",
  "suggested_mapping": {...}  // opcional
}
"""
    
    async def analyze_ambiguity(
        self,
        user_query: str,
        explorer_context: Dict[str, Any],
        ambiguity_type: str,
        options: Optional[List[str]] = None
    ) -> Dict[str, Any]:
        """
        Analiza una ambigüedad y genera pregunta clarificadora
        
        Args:
            user_query: Pregunta original del usuario
            explorer_context: Contexto del Explorer Agent (qué encontró)
            ambiguity_type: Tipo de ambigüedad detectada
            options: Opciones posibles identificadas
            
        Returns:
            Diccionario con pregunta clarificadora
        """
        logger.info(
            "learning_analyze",
            query=user_query,
            ambiguity_type=ambiguity_type
        )
        
        # Construir prompt contextual
        context_prompt = f"""
SITUACIÓN:
- Pregunta del usuario: "{user_query}"
- Tipo de ambigüedad: {ambiguity_type}
- Contexto del explorador: {json.dumps(explorer_context, ensure_ascii=False, indent=2)}
"""
        
        if options:
            context_prompt += f"\n- Opciones identificadas: {', '.join(options)}"
        
        context_prompt += """

Genera una pregunta clarificadora para el usuario que resuelva esta ambigüedad.
Incluye opciones concretas cuando sea posible.
"""
        
        self.conversation_history = [
            {"role": "system", "content": self.system_prompt},
            {"role": "user", "content": context_prompt}
        ]
        
        try:
            response = self.client.chat.completions.create(
                model=settings.OPENAI_MODEL,
                messages=self.conversation_history,
                temperature=0.3,
                response_format={"type": "json_object"}
            )
            
            result = json.loads(response.choices[0].message.content)
            
            logger.info("clarification_generated", question=result.get("question"))
            
            return result
            
        except Exception as e:
            logger.error("learning_error", error=str(e))
            
            # Fallback
            return {
                "type": "clarification_needed",
                "question": f"Encontré ambigüedad en tu pregunta. ¿Puedes ser más específico sobre: {ambiguity_type}?",
                "context": str(explorer_context)
            }
    
    async def process_user_response(
        self,
        original_query: str,
        clarification_question: str,
        user_answer: str,
        context: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Procesa la respuesta del usuario a una clarificación
        Y EXTRAE APRENDIZAJES AUTOMÁTICAMENTE
        """
        logger.info("learning_process_response", user_answer=user_answer)
        
        prompt = f"""
INTERACCIÓN:
- Pregunta original: "{original_query}"
- Pregunta de clarificación: "{clarification_question}"
- Respuesta del usuario: "{user_answer}"
- Contexto: {json.dumps(context, ensure_ascii=False, indent=2)}

TAREA:
Analiza la respuesta del usuario y extrae el aprendizaje estructurado.

FORMATO DE RESPUESTA (JSON):
{{
  "understood": true/false,
  "suggested_mapping": {{
    "user_term": "término que usó el usuario en la pregunta original",
    "db_table": "tabla de la base de datos",
    "db_field": null (o campo específico si aplica)
  }},
  "confidence": 0.0-1.0,
  "explanation": "breve explicación del mapeo",
  "ready_to_retry": true/false
}}

EJEMPLOS:

Original: "facturas de Costasol"
Clarificación: "¿Te refieres a la empresa o delegación?"
Respuesta: "La empresa"
Resultado: {{
  "understood": true,
  "suggested_mapping": {{
    "user_term": "Costasol",
    "db_table": "companies",
    "db_field": null
  }},
  "confidence": 0.85,
  "explanation": "El usuario confirmó que Costasol se refiere a la tabla companies",
  "ready_to_retry": true
}}

Original: "Dame el total"
Clarificación: "¿A qué campo te refieres con 'total'?"
Respuesta: "Al precio de cada línea multiplicado por cantidad"
Resultado: {{
  "understood": true,
  "suggested_mapping": {{
    "user_term": "total",
    "db_table": "orders_lines",
    "db_field": "quantity * price"
  }},
  "confidence": 0.90,
  "explanation": "Total se calcula multiplicando quantity por price en orders_lines",
  "ready_to_retry": true
}}

Ahora analiza la interacción actual y responde SOLO con JSON válido.
"""
        
        try:
            response = self.client.chat.completions.create(
                model=settings.OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                response_format={"type": "json_object"}
            )
            
            learning = json.loads(response.choices[0].message.content)
            
            logger.info("learning_extracted", learning=learning)
            
            return {
                "success": True,
                "learning": learning,
                "ready_to_retry": learning.get("ready_to_retry", True)
            }
            
        except Exception as e:
            logger.error("learning_processing_error", error=str(e))
            
            return {
                "success": False,
                "error": str(e)
            }

    async def validate_learning(
        self,
        learning: Dict[str, Any],
        validation_example: str
    ) -> Dict[str, Any]:
        """
        Valida un aprendizaje con el usuario usando un ejemplo
        
        Args:
            learning: Aprendizaje a validar
            validation_example: Ejemplo para validar
            
        Returns:
            Diccionario con pregunta de validación
        """
        logger.info("learning_validate", learning=learning)
        
        prompt = f"""
Aprendizaje a validar:
{json.dumps(learning, ensure_ascii=False, indent=2)}

Genera una pregunta de validación usando este ejemplo: "{validation_example}"

La pregunta debe confirmar que el aprendizaje es correcto.
"""
        
        try:
            response = self.client.chat.completions.create(
                model=settings.OPENAI_MODEL,
                messages=[
                    {"role": "system", "content": self.system_prompt},
                    {"role": "user", "content": prompt}
                ],
                temperature=0.3,
                response_format={"type": "json_object"}
            )
            
            validation = json.loads(response.choices[0].message.content)
            
            return validation
            
        except Exception as e:
            logger.error("validation_error", error=str(e))
            
            return {
                "type": "validation_needed",
                "question": f"Para confirmar: {validation_example} ¿Es correcto?"
            }


# Instancia global
learning_agent = LearningAgent()