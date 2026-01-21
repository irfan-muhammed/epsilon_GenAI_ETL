"""
Extract Tool - Data Extraction and Schema Analysis
Handles reading data from various sources and analyzing its structure
"""

import pandas as pd
import json
from typing import Dict, Any, Optional
from pathlib import Path


def extract_data(file_path: str) -> Dict[str, Any]:
    """
    Extract data from a file (CSV, JSON, etc.)
    
    Args:
        file_path: Path to the data file
        
    Returns:
        Dictionary containing extraction results
    """
    try:
        path = Path(file_path)
        
        if not path.exists():
            return {
                "success": False,
                "error": f"File not found: {file_path}",
                "data": None
            }
        
        # Determine file type and read accordingly
        if path.suffix.lower() == '.csv':
            df = pd.read_csv(file_path)
        elif path.suffix.lower() == '.json':
            df = pd.read_json(file_path)
        elif path.suffix.lower() == '.parquet':
            df = pd.read_parquet(file_path)
        else:
            return {
                "success": False,
                "error": f"Unsupported file type: {path.suffix}",
                "data": None
            }
        
        return {
            "success": True,
            "file_path": file_path,
            "file_type": path.suffix.lower(),
            "row_count": len(df),
            "column_count": len(df.columns),
            "columns": list(df.columns),
            "data": df
        }
        
    except Exception as e:
        return {
            "success": False,
            "error": str(e),
            "data": None
        }


def analyze_schema(df: pd.DataFrame) -> Dict[str, Any]:
    """
    Analyze the schema and data quality of a DataFrame.
    This provides rich context for the LLM to reason about transformations.
    
    Args:
        df: Pandas DataFrame to analyze
        
    Returns:
        Detailed schema analysis dictionary
    """
    analysis = {
        "row_count": len(df),
        "column_count": len(df.columns),
        "columns": {},
        "data_quality_issues": [],
        "sample_data": df.head(3).to_dict(orient='records')
    }
    
    for col in df.columns:
        col_analysis = {
            "dtype": str(df[col].dtype),
            "non_null_count": int(df[col].notna().sum()),
            "null_count": int(df[col].isna().sum()),
            "null_percentage": round(df[col].isna().sum() / len(df) * 100, 2),
            "unique_count": int(df[col].nunique()),
            "sample_values": df[col].dropna().head(3).tolist()
        }
        
        # Additional analysis for numeric columns
        if pd.api.types.is_numeric_dtype(df[col]):
            col_analysis["min"] = float(df[col].min()) if df[col].notna().any() else None
            col_analysis["max"] = float(df[col].max()) if df[col].notna().any() else None
            col_analysis["mean"] = round(float(df[col].mean()), 2) if df[col].notna().any() else None
            
            # Check for potential issues
            if df[col].min() < 0 and col in ['passenger_count', 'trip_distance', 'fare_amount']:
                analysis["data_quality_issues"].append({
                    "column": col,
                    "issue": "negative_values",
                    "description": f"Column '{col}' contains negative values which may be invalid",
                    "affected_rows": int((df[col] < 0).sum())
                })
        
        # Check for string columns that should be numeric
        if df[col].dtype == 'object':
            # Try to convert to numeric
            numeric_test = pd.to_numeric(df[col], errors='coerce')
            non_numeric = df[col].notna() & numeric_test.isna()
            if non_numeric.any():
                analysis["data_quality_issues"].append({
                    "column": col,
                    "issue": "mixed_types",
                    "description": f"Column '{col}' contains non-numeric values in a potentially numeric field",
                    "affected_rows": int(non_numeric.sum()),
                    "sample_invalid": df[col][non_numeric].head(3).tolist()
                })
        
        # Check for null values
        if col_analysis["null_percentage"] > 0:
            analysis["data_quality_issues"].append({
                "column": col,
                "issue": "null_values",
                "description": f"Column '{col}' has {col_analysis['null_percentage']}% null values",
                "affected_rows": col_analysis["null_count"]
            })
        
        analysis["columns"][col] = col_analysis
    
    # Detect potential datetime columns
    for col in df.columns:
        if df[col].dtype == 'object':
            try:
                pd.to_datetime(df[col].head(5))
                analysis["columns"][col]["potential_datetime"] = True
            except:
                pass
    
    return analysis


def get_schema_summary(analysis: Dict[str, Any]) -> str:
    """
    Generate a human-readable summary of the schema analysis.
    This summary is fed to the LLM for decision making.
    
    Args:
        analysis: Schema analysis dictionary
        
    Returns:
        Formatted string summary
    """
    summary = []
    summary.append(f"📊 DATA SCHEMA ANALYSIS")
    summary.append(f"=" * 50)
    summary.append(f"Total Rows: {analysis['row_count']}")
    summary.append(f"Total Columns: {analysis['column_count']}")
    summary.append("")
    
    summary.append("📋 COLUMN DETAILS:")
    summary.append("-" * 50)
    
    for col_name, col_info in analysis['columns'].items():
        summary.append(f"\n  [{col_name}]")
        summary.append(f"    Type: {col_info['dtype']}")
        summary.append(f"    Non-null: {col_info['non_null_count']} | Null: {col_info['null_count']} ({col_info['null_percentage']}%)")
        summary.append(f"    Unique values: {col_info['unique_count']}")
        summary.append(f"    Sample: {col_info['sample_values'][:2]}")
        
        if col_info.get('potential_datetime'):
            summary.append(f"    ⚠️  Potential datetime column (currently string)")
        
        if 'min' in col_info:
            summary.append(f"    Range: [{col_info['min']} - {col_info['max']}], Mean: {col_info['mean']}")
    
    if analysis['data_quality_issues']:
        summary.append("")
        summary.append("⚠️  DATA QUALITY ISSUES DETECTED:")
        summary.append("-" * 50)
        for issue in analysis['data_quality_issues']:
            summary.append(f"  • {issue['column']}: {issue['issue']}")
            summary.append(f"    {issue['description']}")
            summary.append(f"    Affected rows: {issue['affected_rows']}")
    
    return "\n".join(summary)


if __name__ == "__main__":
    # Test extraction
    result = extract_data("../data/nyc_taxi_sample.csv")
    if result["success"]:
        analysis = analyze_schema(result["data"])
        print(get_schema_summary(analysis))
    else:
        print(f"Error: {result['error']}")
