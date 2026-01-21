"""
ETL Agent - LangGraph-based Intelligent ETL Orchestrator
This agent uses GenAI to reason about data, plan transformations, and orchestrate ETL.
"""

import json
from typing import Dict, Any, List, TypedDict, Annotated, Literal
from pathlib import Path
import pandas as pd

from langgraph.graph import StateGraph, END
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.prompts import ChatPromptTemplate

import sys
sys.path.insert(0, str(Path(__file__).parent.parent))

from llm_setup import get_llm
from tools.extract_tool import extract_data, analyze_schema, get_schema_summary
from tools.transform_tool import transform_data, validate_data, get_transformation_summary
from tools.load_tool import load_to_database, verify_load, get_load_summary


# ============================================================================
# STATE DEFINITION
# ============================================================================

class ETLState(TypedDict):
    """State that flows through the ETL pipeline."""
    # Input
    source_path: str
    target_db: str
    target_table: str
    user_instructions: str
    
    # Data
    raw_data: Any  # DataFrame
    transformed_data: Any  # DataFrame
    
    # Analysis
    schema_analysis: Dict[str, Any]
    schema_summary: str
    
    # Plans
    transformation_plan: List[Dict[str, Any]]
    validation_rules: List[Dict[str, Any]]
    
    # Results
    transformation_result: Dict[str, Any]
    validation_result: Dict[str, Any]
    load_result: Dict[str, Any]
    
    # Agent reasoning
    reasoning_log: List[str]
    current_step: str
    error: str
    retry_count: int
    final_status: str


# ============================================================================
# AGENT PROMPTS
# ============================================================================

SCHEMA_ANALYSIS_PROMPT = """You are an expert data engineer analyzing a dataset for an ETL pipeline.

Given the following schema analysis, provide:
1. A summary of what this data represents
2. Key observations about data types and quality
3. Potential issues that need to be addressed

SCHEMA ANALYSIS:
{schema_summary}

Respond in a structured way that will help plan the transformations."""


TRANSFORMATION_PLANNING_PROMPT = """You are an expert data engineer planning transformations for an ETL pipeline.

SCHEMA ANALYSIS:
{schema_summary}

USER INSTRUCTIONS:
{user_instructions}

DATA QUALITY ISSUES:
{quality_issues}

Based on this information, create a transformation plan. You MUST respond with a valid JSON array of transformation steps.

Available transformation actions:
- "convert_datetime": Convert a column to datetime. Params: column, format (optional)
- "fill_null": Fill null values. Params: column, strategy (value/mean/median/mode/drop), value (if strategy is "value")
- "remove_negative": Remove rows with negative values. Params: column
- "remove_invalid": Remove invalid values. Params: column, valid_values (list)
- "convert_numeric": Convert to numeric. Params: column
- "rename_column": Rename column. Params: column (old name), new_name
- "drop_column": Drop a column. Params: column
- "standardize_text": Standardize text case. Params: column, case (lower/upper/title)
- "remove_duplicates": Remove duplicate rows. Params: subset (list of columns, optional)

IMPORTANT: Respond ONLY with a JSON array, no other text. Example:
[
    {{"action": "convert_datetime", "column": "pickup_datetime"}},
    {{"action": "fill_null", "column": "passenger_count", "strategy": "median"}},
    {{"action": "remove_negative", "column": "fare_amount"}}
]

Your transformation plan (JSON array only):"""


VALIDATION_RULES_PROMPT = """You are an expert data engineer defining validation rules for transformed data.

ORIGINAL SCHEMA:
{schema_summary}

TRANSFORMATIONS APPLIED:
{transformations}

Define validation rules to ensure data quality. Respond ONLY with a JSON array.

Available validation rule types:
- "not_null": Check column has no nulls. Params: column
- "positive": Check column values are positive. Params: column
- "in_range": Check values in range. Params: column, min, max
- "row_count": Check minimum row count. Params: min

Example response:
[
    {{"type": "not_null", "column": "trip_distance"}},
    {{"type": "positive", "column": "fare_amount"}},
    {{"type": "row_count", "min": 10}}
]

Your validation rules (JSON array only):"""


ERROR_RECOVERY_PROMPT = """You are an expert data engineer handling an ETL error.

ERROR:
{error}

CURRENT STATE:
- Step: {current_step}
- Retry count: {retry_count}

PREVIOUS TRANSFORMATION PLAN:
{transformation_plan}

Analyze the error and provide a corrected transformation plan. 
If the error is unrecoverable, respond with: {{"unrecoverable": true, "reason": "explanation"}}

Otherwise, respond with a corrected JSON array of transformations."""


# ============================================================================
# AGENT NODES
# ============================================================================

class ETLAgent:
    """LangGraph-based ETL Agent that uses GenAI for intelligent orchestration."""
    
    def __init__(self, llm=None):
        self.llm = llm or get_llm()
        self.graph = self._build_graph()
    
    def _build_graph(self) -> StateGraph:
        """Build the LangGraph state machine for ETL orchestration."""
        
        # Create the graph
        workflow = StateGraph(ETLState)
        
        # Add nodes
        workflow.add_node("extract", self._extract_node)
        workflow.add_node("analyze", self._analyze_node)
        workflow.add_node("plan", self._plan_node)
        workflow.add_node("transform", self._transform_node)
        workflow.add_node("validate", self._validate_node)
        workflow.add_node("load", self._load_node)
        workflow.add_node("verify", self._verify_node)
        workflow.add_node("handle_error", self._error_node)
        
        # Set entry point
        workflow.set_entry_point("extract")
        
        # Add edges
        workflow.add_conditional_edges(
            "extract",
            self._check_extract,
            {"success": "analyze", "error": "handle_error"}
        )
        
        workflow.add_edge("analyze", "plan")
        
        workflow.add_conditional_edges(
            "plan",
            self._check_plan,
            {"success": "transform", "error": "handle_error"}
        )
        
        workflow.add_conditional_edges(
            "transform",
            self._check_transform,
            {"success": "validate", "error": "handle_error"}
        )
        
        workflow.add_conditional_edges(
            "validate",
            self._check_validation,
            {"success": "load", "retry": "plan", "error": "handle_error"}
        )
        
        workflow.add_conditional_edges(
            "load",
            self._check_load,
            {"success": "verify", "error": "handle_error"}
        )
        
        workflow.add_edge("verify", END)
        
        workflow.add_conditional_edges(
            "handle_error",
            self._check_recovery,
            {"retry": "plan", "fail": END}
        )
        
        return workflow.compile()
    
    # ========== NODE IMPLEMENTATIONS ==========
    
    def _extract_node(self, state: ETLState) -> Dict[str, Any]:
        """Extract data from source."""
        print("\n🔄 STEP 1: EXTRACTING DATA...")
        print(f"   Source: {state['source_path']}")
        
        result = extract_data(state['source_path'])
        
        if result['success']:
            print(f"   ✅ Extracted {result['row_count']} rows, {result['column_count']} columns")
            return {
                "raw_data": result['data'],
                "current_step": "extract",
                "reasoning_log": state.get('reasoning_log', []) + [
                    f"Extracted {result['row_count']} rows from {result['file_type']} file"
                ]
            }
        else:
            print(f"   ❌ Extraction failed: {result['error']}")
            return {
                "error": result['error'],
                "current_step": "extract"
            }
    
    def _analyze_node(self, state: ETLState) -> Dict[str, Any]:
        """Analyze schema and identify issues using LLM."""
        print("\n🔍 STEP 2: ANALYZING SCHEMA...")
        
        df = state['raw_data']
        analysis = analyze_schema(df)
        summary = get_schema_summary(analysis)
        
        print(summary)
        
        # Use LLM to interpret the schema
        prompt = SCHEMA_ANALYSIS_PROMPT.format(schema_summary=summary)
        response = self.llm.invoke([HumanMessage(content=prompt)])
        
        print(f"\n🤖 LLM Analysis:\n{response.content[:500]}...")
        
        return {
            "schema_analysis": analysis,
            "schema_summary": summary,
            "current_step": "analyze",
            "reasoning_log": state.get('reasoning_log', []) + [
                f"Analyzed schema: {len(analysis['columns'])} columns, {len(analysis['data_quality_issues'])} issues found"
            ]
        }
    
    def _plan_node(self, state: ETLState) -> Dict[str, Any]:
        """Use LLM to plan transformations."""
        print("\n📋 STEP 3: PLANNING TRANSFORMATIONS...")
        
        # Format quality issues
        quality_issues = json.dumps(state['schema_analysis'].get('data_quality_issues', []), indent=2)
        
        # Ask LLM to plan transformations
        prompt = TRANSFORMATION_PLANNING_PROMPT.format(
            schema_summary=state['schema_summary'],
            user_instructions=state.get('user_instructions', 'Clean and prepare data for analysis'),
            quality_issues=quality_issues
        )
        
        response = self.llm.invoke([HumanMessage(content=prompt)])
        
        # Parse the transformation plan
        try:
            # Clean up response - extract JSON array
            content = response.content.strip()
            # Find the JSON array in the response
            start_idx = content.find('[')
            end_idx = content.rfind(']') + 1
            if start_idx != -1 and end_idx > start_idx:
                json_str = content[start_idx:end_idx]
                plan = json.loads(json_str)
            else:
                raise ValueError("No JSON array found in response")
            
            print(f"\n🤖 LLM Generated Plan ({len(plan)} steps):")
            for i, step in enumerate(plan, 1):
                print(f"   {i}. {step['action']} on '{step.get('column', 'N/A')}'")
            
            return {
                "transformation_plan": plan,
                "current_step": "plan",
                "reasoning_log": state.get('reasoning_log', []) + [
                    f"LLM planned {len(plan)} transformation steps"
                ]
            }
            
        except Exception as e:
            print(f"   ❌ Failed to parse plan: {e}")
            return {
                "error": f"Failed to parse transformation plan: {e}",
                "current_step": "plan"
            }
    
    def _transform_node(self, state: ETLState) -> Dict[str, Any]:
        """Execute transformations."""
        print("\n⚙️ STEP 4: EXECUTING TRANSFORMATIONS...")
        
        df = state['raw_data']
        plan = state['transformation_plan']
        
        result = transform_data(df, plan)
        
        print(get_transformation_summary(result))
        
        # Generate validation rules using LLM
        prompt = VALIDATION_RULES_PROMPT.format(
            schema_summary=state['schema_summary'],
            transformations=json.dumps(plan, indent=2)
        )
        
        response = self.llm.invoke([HumanMessage(content=prompt)])
        
        try:
            content = response.content.strip()
            start_idx = content.find('[')
            end_idx = content.rfind(']') + 1
            if start_idx != -1 and end_idx > start_idx:
                validation_rules = json.loads(content[start_idx:end_idx])
            else:
                validation_rules = [{"type": "row_count", "min": 1}]
        except:
            validation_rules = [{"type": "row_count", "min": 1}]
        
        return {
            "transformed_data": result['transformed_data'],
            "transformation_result": result,
            "validation_rules": validation_rules,
            "current_step": "transform",
            "reasoning_log": state.get('reasoning_log', []) + [
                f"Executed {len(plan)} transformations, {result['final_row_count']} rows remaining"
            ]
        }
    
    def _validate_node(self, state: ETLState) -> Dict[str, Any]:
        """Validate transformed data."""
        print("\n✅ STEP 5: VALIDATING DATA...")
        
        df = state['transformed_data']
        rules = state['validation_rules']
        
        result = validate_data(df, rules)
        
        print(f"\n📊 Validation Results:")
        print(f"   Overall: {'✅ PASSED' if result['valid'] else '❌ FAILED'}")
        for check in result['checks']:
            icon = "✅" if check['passed'] else "❌"
            print(f"   {icon} {check['rule']}: {check['details']}")
        
        return {
            "validation_result": result,
            "current_step": "validate",
            "reasoning_log": state.get('reasoning_log', []) + [
                f"Validation {'passed' if result['valid'] else 'failed'}: {len(result['checks'])} checks"
            ]
        }
    
    def _load_node(self, state: ETLState) -> Dict[str, Any]:
        """Load data to database."""
        print("\n💾 STEP 6: LOADING TO DATABASE...")
        
        df = state['transformed_data']
        
        result = load_to_database(
            df,
            state['target_db'],
            state['target_table'],
            if_exists='replace',
            create_indexes=['VendorID'] if 'VendorID' in df.columns else None
        )
        
        print(get_load_summary(result))
        
        if result['success']:
            return {
                "load_result": result,
                "current_step": "load",
                "reasoning_log": state.get('reasoning_log', []) + [
                    f"Loaded {result['rows_loaded']} rows to {result['table']}"
                ]
            }
        else:
            return {
                "error": result['error'],
                "current_step": "load"
            }
    
    def _verify_node(self, state: ETLState) -> Dict[str, Any]:
        """Verify the load was successful."""
        print("\n🔍 STEP 7: VERIFYING LOAD...")
        
        expected = len(state['transformed_data'])
        result = verify_load(state['target_db'], state['target_table'], expected)
        
        if result['success'] and result['count_matches']:
            print(f"   ✅ Verification PASSED: {result['actual_count']} rows confirmed")
            return {
                "final_status": "SUCCESS",
                "reasoning_log": state.get('reasoning_log', []) + [
                    f"✅ ETL Pipeline completed successfully. {result['actual_count']} rows in database."
                ]
            }
        else:
            print(f"   ⚠️ Verification warning: Expected {expected}, got {result.get('actual_count', 'unknown')}")
            return {
                "final_status": "COMPLETED_WITH_WARNINGS",
                "reasoning_log": state.get('reasoning_log', []) + [
                    f"Pipeline completed with warnings"
                ]
            }
    
    def _error_node(self, state: ETLState) -> Dict[str, Any]:
        """Handle errors with LLM-based recovery."""
        print("\n🚨 ERROR RECOVERY...")
        
        retry_count = state.get('retry_count', 0)
        
        if retry_count >= 2:
            print("   ❌ Max retries exceeded")
            return {
                "final_status": "FAILED",
                "retry_count": retry_count
            }
        
        # Use LLM to suggest recovery
        prompt = ERROR_RECOVERY_PROMPT.format(
            error=state.get('error', 'Unknown error'),
            current_step=state.get('current_step', 'unknown'),
            retry_count=retry_count,
            transformation_plan=json.dumps(state.get('transformation_plan', []), indent=2)
        )
        
        response = self.llm.invoke([HumanMessage(content=prompt)])
        print(f"   🤖 LLM Recovery suggestion: {response.content[:200]}...")
        
        try:
            content = response.content.strip()
            if '"unrecoverable": true' in content.lower():
                return {
                    "final_status": "FAILED",
                    "retry_count": retry_count + 1
                }
            
            start_idx = content.find('[')
            end_idx = content.rfind(']') + 1
            if start_idx != -1 and end_idx > start_idx:
                new_plan = json.loads(content[start_idx:end_idx])
                return {
                    "transformation_plan": new_plan,
                    "retry_count": retry_count + 1,
                    "error": "",
                    "reasoning_log": state.get('reasoning_log', []) + [
                        f"Retry {retry_count + 1}: LLM suggested recovery plan"
                    ]
                }
        except:
            pass
        
        return {
            "final_status": "FAILED",
            "retry_count": retry_count + 1
        }
    
    # ========== CONDITIONAL EDGES ==========
    
    def _check_extract(self, state: ETLState) -> Literal["success", "error"]:
        return "error" if state.get('error') else "success"
    
    def _check_plan(self, state: ETLState) -> Literal["success", "error"]:
        return "error" if state.get('error') else "success"
    
    def _check_transform(self, state: ETLState) -> Literal["success", "error"]:
        return "error" if state.get('error') else "success"
    
    def _check_validation(self, state: ETLState) -> Literal["success", "retry", "error"]:
        if state.get('error'):
            return "error"
        if not state['validation_result'].get('valid', True):
            if state.get('retry_count', 0) < 2:
                return "retry"
        return "success"
    
    def _check_load(self, state: ETLState) -> Literal["success", "error"]:
        return "error" if state.get('error') else "success"
    
    def _check_recovery(self, state: ETLState) -> Literal["retry", "fail"]:
        if state.get('final_status') == "FAILED":
            return "fail"
        if state.get('retry_count', 0) < 2:
            return "retry"
        return "fail"
    
    # ========== PUBLIC INTERFACE ==========
    
    def run(
        self,
        source_path: str,
        target_db: str,
        target_table: str,
        user_instructions: str = "Clean and prepare data for analysis"
    ) -> Dict[str, Any]:
        """
        Run the ETL pipeline.
        
        Args:
            source_path: Path to source data file
            target_db: Path to target SQLite database
            target_table: Name of target table
            user_instructions: Natural language instructions for the agent
            
        Returns:
            Final state dictionary with results
        """
        initial_state: ETLState = {
            "source_path": source_path,
            "target_db": target_db,
            "target_table": target_table,
            "user_instructions": user_instructions,
            "raw_data": None,
            "transformed_data": None,
            "schema_analysis": {},
            "schema_summary": "",
            "transformation_plan": [],
            "validation_rules": [],
            "transformation_result": {},
            "validation_result": {},
            "load_result": {},
            "reasoning_log": [],
            "current_step": "",
            "error": "",
            "retry_count": 0,
            "final_status": ""
        }
        
        print("=" * 60)
        print("🚀 GENAI ETL AGENT STARTING")
        print("=" * 60)
        print(f"Source: {source_path}")
        print(f"Target: {target_db} -> {target_table}")
        print(f"Instructions: {user_instructions}")
        print("=" * 60)
        
        # Run the graph
        final_state = self.graph.invoke(initial_state)
        
        # Print summary
        print("\n" + "=" * 60)
        print("📊 ETL PIPELINE SUMMARY")
        print("=" * 60)
        print(f"Final Status: {final_state.get('final_status', 'UNKNOWN')}")
        print("\n📝 Reasoning Log:")
        for i, log in enumerate(final_state.get('reasoning_log', []), 1):
            print(f"   {i}. {log}")
        print("=" * 60)
        
        return final_state


def create_etl_agent(llm=None) -> ETLAgent:
    """Factory function to create an ETL agent."""
    return ETLAgent(llm)


if __name__ == "__main__":
    # Test the agent
    agent = create_etl_agent()
    
    result = agent.run(
        source_path="./data/nyc_taxi_sample.csv",
        target_db="./output/taxi_data.db",
        target_table="taxi_trips",
        user_instructions="Clean the NYC taxi data: handle missing values, remove invalid records, and prepare for analysis"
    )
