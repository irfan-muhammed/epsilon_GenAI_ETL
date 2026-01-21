"""
Load Tool - Database Loading Operations
Handles loading transformed data to SQLite database
"""

import pandas as pd
import sqlite3
from typing import Dict, Any, List, Optional
from pathlib import Path
from datetime import datetime


def load_to_database(
    df: pd.DataFrame,
    db_path: str,
    table_name: str,
    if_exists: str = 'replace',
    create_indexes: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Load DataFrame to SQLite database.
    
    Args:
        df: DataFrame to load
        db_path: Path to SQLite database file
        table_name: Name of the target table
        if_exists: How to behave if table exists ('fail', 'replace', 'append')
        create_indexes: List of columns to create indexes on
        
    Returns:
        Load result dictionary
    """
    try:
        # Ensure directory exists
        Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        
        # Connect to database
        conn = sqlite3.connect(db_path)
        
        # Get table info before (if exists)
        cursor = conn.cursor()
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        table_existed = cursor.fetchone() is not None
        
        existing_count = 0
        if table_existed:
            cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
            existing_count = cursor.fetchone()[0]
        
        # Load data
        df.to_sql(table_name, conn, if_exists=if_exists, index=False)
        
        # Create indexes if specified
        indexes_created = []
        if create_indexes:
            for col in create_indexes:
                if col in df.columns:
                    index_name = f"idx_{table_name}_{col}"
                    try:
                        cursor.execute(f"CREATE INDEX IF NOT EXISTS {index_name} ON {table_name}({col})")
                        indexes_created.append(index_name)
                    except Exception as e:
                        pass  # Index might already exist or column not suitable
        
        # Verify load
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        final_count = cursor.fetchone()[0]
        
        # Get table schema
        cursor.execute(f"PRAGMA table_info({table_name})")
        schema = cursor.fetchall()
        
        conn.commit()
        conn.close()
        
        return {
            "success": True,
            "database": db_path,
            "table": table_name,
            "action": if_exists,
            "rows_loaded": len(df),
            "table_existed": table_existed,
            "previous_row_count": existing_count if table_existed else 0,
            "current_row_count": final_count,
            "indexes_created": indexes_created,
            "schema": [{"name": s[1], "type": s[2], "nullable": not s[3]} for s in schema],
            "load_timestamp": datetime.now().isoformat()
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "database": db_path,
            "table": table_name
        }


def verify_load(db_path: str, table_name: str, expected_count: int) -> Dict[str, Any]:
    """
    Verify data was loaded correctly.
    
    Args:
        db_path: Path to SQLite database
        table_name: Name of the table to verify
        expected_count: Expected number of rows
        
    Returns:
        Verification result dictionary
    """
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check table exists
        cursor.execute(f"SELECT name FROM sqlite_master WHERE type='table' AND name='{table_name}'")
        if not cursor.fetchone():
            return {
                "success": False,
                "error": f"Table '{table_name}' does not exist"
            }
        
        # Get row count
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        actual_count = cursor.fetchone()[0]
        
        # Sample some data
        cursor.execute(f"SELECT * FROM {table_name} LIMIT 5")
        sample_rows = cursor.fetchall()
        
        cursor.execute(f"PRAGMA table_info({table_name})")
        columns = [col[1] for col in cursor.fetchall()]
        
        # Basic stats
        stats = {}
        for col in columns:
            try:
                cursor.execute(f"SELECT COUNT(DISTINCT {col}) FROM {table_name}")
                stats[col] = {"unique_values": cursor.fetchone()[0]}
            except:
                pass
        
        conn.close()
        
        return {
            "success": True,
            "table": table_name,
            "expected_count": expected_count,
            "actual_count": actual_count,
            "count_matches": actual_count == expected_count,
            "columns": columns,
            "sample_data": [dict(zip(columns, row)) for row in sample_rows],
            "column_stats": stats
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def query_database(db_path: str, query: str) -> Dict[str, Any]:
    """
    Execute a read-only query on the database.
    
    Args:
        db_path: Path to SQLite database
        query: SQL query to execute (SELECT only)
        
    Returns:
        Query result dictionary
    """
    # Safety check - only allow SELECT
    if not query.strip().upper().startswith("SELECT"):
        return {
            "success": False,
            "error": "Only SELECT queries are allowed"
        }
    
    try:
        conn = sqlite3.connect(db_path)
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return {
            "success": True,
            "query": query,
            "row_count": len(df),
            "columns": list(df.columns),
            "data": df.to_dict(orient='records')
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "query": query
        }


def get_database_info(db_path: str) -> Dict[str, Any]:
    """
    Get information about the database.
    
    Args:
        db_path: Path to SQLite database
        
    Returns:
        Database info dictionary
    """
    try:
        if not Path(db_path).exists():
            return {
                "success": False,
                "error": f"Database file not found: {db_path}"
            }
        
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Get all tables
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        
        table_info = {}
        for table in tables:
            cursor.execute(f"SELECT COUNT(*) FROM {table}")
            row_count = cursor.fetchone()[0]
            
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [{"name": col[1], "type": col[2]} for col in cursor.fetchall()]
            
            table_info[table] = {
                "row_count": row_count,
                "columns": columns
            }
        
        conn.close()
        
        return {
            "success": True,
            "database": db_path,
            "tables": tables,
            "table_info": table_info,
            "file_size_bytes": Path(db_path).stat().st_size
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }


def get_load_summary(result: Dict[str, Any]) -> str:
    """Generate a human-readable load summary."""
    lines = []
    lines.append("💾 DATABASE LOAD SUMMARY")
    lines.append("=" * 50)
    
    if result["success"]:
        lines.append(f"✅ Load successful!")
        lines.append(f"Database: {result['database']}")
        lines.append(f"Table: {result['table']}")
        lines.append(f"Action: {result['action']}")
        lines.append(f"Rows loaded: {result['rows_loaded']}")
        lines.append(f"Current total rows: {result['current_row_count']}")
        lines.append(f"Timestamp: {result['load_timestamp']}")
        
        if result['indexes_created']:
            lines.append(f"Indexes created: {', '.join(result['indexes_created'])}")
        
        lines.append("")
        lines.append("📋 TABLE SCHEMA:")
        for col in result['schema']:
            nullable = "NULL" if col['nullable'] else "NOT NULL"
            lines.append(f"  • {col['name']}: {col['type']} ({nullable})")
    else:
        lines.append(f"❌ Load failed!")
        lines.append(f"Error: {result['error']}")
    
    return "\n".join(lines)


if __name__ == "__main__":
    # Test loading
    import sys
    sys.path.append('..')
    from extract_tool import extract_data
    
    result = extract_data("../data/nyc_taxi_sample.csv")
    if result["success"]:
        df = result["data"]
        
        load_result = load_to_database(
            df,
            "../output/test.db",
            "taxi_trips",
            if_exists='replace',
            create_indexes=['VendorID', 'tpep_pickup_datetime']
        )
        
        print(get_load_summary(load_result))
        
        # Verify
        verify_result = verify_load("../output/test.db", "taxi_trips", len(df))
        print(f"\nVerification: {'✅ Passed' if verify_result['count_matches'] else '❌ Failed'}")
