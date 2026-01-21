"""
GenAI ETL Agent - Main Entry Point
Demonstrates the intelligent ETL pipeline powered by LangGraph and Azure OpenAI
"""

import os
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent))

from agent.etl_agent import create_etl_agent
from tools.load_tool import get_database_info, query_database


def run_basic_demo():
    """
    Demo 1: Basic ETL Pipeline
    Shows the agent extracting, analyzing, transforming, and loading NYC taxi data.
    """
    print("\n" + "=" * 70)
    print("🎯 DEMO 1: BASIC ETL PIPELINE")
    print("=" * 70)
    
    # Get the project root directory (parent of src)
    project_root = Path(__file__).parent.parent
    
    agent = create_etl_agent()
    
    result = agent.run(
        source_path=str(project_root / "data" / "nyc_taxi_sample.csv"),
        target_db=str(project_root / "output" / "taxi_data.db"),
        target_table="taxi_trips",
        user_instructions="""
        Clean the NYC taxi trip data for analysis:
        1. Convert datetime columns to proper datetime types
        2. Handle missing passenger counts
        3. Remove any invalid records (negative values, invalid codes)
        4. Prepare data for analytics queries
        """
    )
    
    return result


def run_schema_change_demo():
    """
    Demo 2: Schema Change Adaptation (Bonus)
    Shows how the agent handles a new data source with different schema.
    """
    print("\n" + "=" * 70)
    print("🎯 DEMO 2: SCHEMA CHANGE ADAPTATION")
    print("=" * 70)
    
    # Get the project root directory (parent of src)
    project_root = Path(__file__).parent.parent
    
    agent = create_etl_agent()
    
    # Run with the new schema file
    result = agent.run(
        source_path=str(project_root / "data" / "nyc_taxi_new_schema.csv"),
        target_db=str(project_root / "output" / "taxi_data_new.db"),
        target_table="taxi_trips_v2",
        user_instructions="""
        This is a NEW version of the taxi data with DIFFERENT column names.
        The agent should:
        1. Detect the schema change automatically
        2. Adapt transformations to new column names
        3. Perform appropriate data cleaning
        4. Load to database successfully
        """
    )
    
    return result


def explore_database(db_path: str):
    """
    Explore the loaded database.
    """
    print("\n" + "=" * 70)
    print("🔍 DATABASE EXPLORATION")
    print("=" * 70)
    
    info = get_database_info(db_path)
    
    if info['success']:
        print(f"\nDatabase: {info['database']}")
        print(f"File Size: {info['file_size_bytes']:,} bytes")
        print(f"\nTables: {info['tables']}")
        
        for table, details in info['table_info'].items():
            print(f"\n📋 Table: {table}")
            print(f"   Rows: {details['row_count']}")
            print(f"   Columns:")
            for col in details['columns']:
                print(f"      - {col['name']}: {col['type']}")
        
        # Run a sample query
        print("\n📊 Sample Query Results:")
        query_result = query_database(
            db_path,
            f"SELECT * FROM {info['tables'][0]} LIMIT 5"
        )
        
        if query_result['success']:
            import pandas as pd
            df = pd.DataFrame(query_result['data'])
            print(df.to_string(index=False))
    else:
        print(f"Error: {info['error']}")


def print_architecture():
    """Print the architecture diagram."""
    architecture = """
    ╔══════════════════════════════════════════════════════════════════════════╗
    ║                    GENAI ETL AGENT ARCHITECTURE                          ║
    ╠══════════════════════════════════════════════════════════════════════════╣
    ║                                                                          ║
    ║    ┌─────────────────────────────────────────────────────────────────┐   ║
    ║    │                    LangGraph State Machine                       │   ║
    ║    │                                                                  │   ║
    ║    │   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │   ║
    ║    │   │ EXTRACT  │───▶│ ANALYZE  │───▶│   PLAN   │───▶│TRANSFORM │  │   ║
    ║    │   └──────────┘    └──────────┘    └──────────┘    └──────────┘  │   ║
    ║    │        │               │               │               │        │   ║
    ║    │        │               │               │               │        │   ║
    ║    │        ▼               ▼               ▼               ▼        │   ║
    ║    │   ┌──────────┐    ┌──────────┐    ┌──────────┐    ┌──────────┐  │   ║
    ║    │   │   Tool   │    │   LLM    │    │   LLM    │    │   Tool   │  │   ║
    ║    │   │ extract  │    │ analyze  │    │  plan    │    │transform │  │   ║
    ║    │   └──────────┘    └──────────┘    └──────────┘    └──────────┘  │   ║
    ║    │                                                                  │   ║
    ║    │                         ┌───────────────────┐                    │   ║
    ║    │   ┌──────────┐         │   ERROR HANDLER   │                    │   ║
    ║    │   │ VALIDATE │◀────────│   (LLM Recovery)  │◀────── on error    │   ║
    ║    │   └──────────┘         └───────────────────┘                    │   ║
    ║    │        │                        │                               │   ║
    ║    │        ▼                        │ retry                         │   ║
    ║    │   ┌──────────┐    ┌──────────┐  │                               │   ║
    ║    │   │   LOAD   │───▶│  VERIFY  │──┴──▶ END                        │   ║
    ║    │   └──────────┘    └──────────┘                                  │   ║
    ║    │                                                                  │   ║
    ║    └─────────────────────────────────────────────────────────────────┘   ║
    ║                                                                          ║
    ║    ┌─────────────────────────────────────────────────────────────────┐   ║
    ║    │                    Azure OpenAI (LLM)                            │   ║
    ║    │  • Schema analysis & understanding                               │   ║
    ║    │  • Transformation planning                                       │   ║
    ║    │  • Validation rule generation                                    │   ║
    ║    │  • Error recovery & re-planning                                  │   ║
    ║    └─────────────────────────────────────────────────────────────────┘   ║
    ║                                                                          ║
    ╚══════════════════════════════════════════════════════════════════════════╝
    """
    print(architecture)


def main():
    """Main entry point."""
    print("""
    ╔══════════════════════════════════════════════════════════════════════════╗
    ║                                                                          ║
    ║             🤖 GENAI-POWERED ETL AGENT                                   ║
    ║                                                                          ║
    ║             Intelligent Data Pipeline Orchestration                      ║
    ║             Using LangGraph + Azure OpenAI                               ║
    ║                                                                          ║
    ╚══════════════════════════════════════════════════════════════════════════╝
    """)
    
    print_architecture()
    
    print("\n" + "=" * 70)
    print("🎮 AVAILABLE DEMOS")
    print("=" * 70)
    print("1. Basic ETL Pipeline - NYC Taxi Data")
    print("2. Schema Change Adaptation (Bonus)")
    print("3. Run Both Demos")
    print("4. Just Show Architecture")
    print("=" * 70)
    
    choice = input("\nSelect demo (1-4) [default: 1]: ").strip() or "1"
    
    if choice == "1":
        result = run_basic_demo()
        if result.get('final_status') == 'SUCCESS':
            project_root = Path(__file__).parent.parent
            explore_database(str(project_root / "output" / "taxi_data.db"))
            
    elif choice == "2":
        result = run_schema_change_demo()
        if result.get('final_status') == 'SUCCESS':
            project_root = Path(__file__).parent.parent
            explore_database(str(project_root / "output" / "taxi_data_new.db"))
            
    elif choice == "3":
        result1 = run_basic_demo()
        project_root = Path(__file__).parent.parent
        if result1.get('final_status') == 'SUCCESS':
            explore_database(str(project_root / "output" / "taxi_data.db"))
        
        result2 = run_schema_change_demo()
        if result2.get('final_status') == 'SUCCESS':
            explore_database(str(project_root / "output" / "taxi_data_new.db"))
            
    elif choice == "4":
        print_architecture()
        
    else:
        print("Invalid choice. Running basic demo...")
        run_basic_demo()


if __name__ == "__main__":
    main()
