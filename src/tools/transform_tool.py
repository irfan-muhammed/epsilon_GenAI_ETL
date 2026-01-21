"""
Transform Tool - Data Transformation Execution
Executes transformations based on LLM-generated plans
"""

import pandas as pd
import numpy as np
from typing import Dict, Any, List, Optional
from datetime import datetime
import json


class TransformationExecutor:
    """
    Executes data transformations based on a transformation plan.
    The LLM generates the plan, this class executes it safely.
    """
    
    def __init__(self, df: pd.DataFrame):
        self.original_df = df.copy()
        self.df = df.copy()
        self.transformation_log = []
        
    def execute_plan(self, plan: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Execute a list of transformation steps.
        
        Args:
            plan: List of transformation dictionaries
            
        Returns:
            Result dictionary with transformed data and log
        """
        for i, step in enumerate(plan):
            try:
                result = self._execute_step(step)
                self.transformation_log.append({
                    "step": i + 1,
                    "action": step.get("action"),
                    "column": step.get("column"),
                    "status": "success",
                    "details": result
                })
            except Exception as e:
                self.transformation_log.append({
                    "step": i + 1,
                    "action": step.get("action"),
                    "column": step.get("column"),
                    "status": "failed",
                    "error": str(e)
                })
                # Continue with other transformations
                
        return {
            "success": True,
            "transformed_data": self.df,
            "original_row_count": len(self.original_df),
            "final_row_count": len(self.df),
            "transformation_log": self.transformation_log
        }
    
    def _execute_step(self, step: Dict[str, Any]) -> str:
        """Execute a single transformation step."""
        action = step.get("action")
        column = step.get("column")
        
        if action == "convert_datetime":
            return self._convert_datetime(column, step.get("format"))
            
        elif action == "fill_null":
            return self._fill_null(column, step.get("strategy"), step.get("value"))
            
        elif action == "remove_negative":
            return self._remove_negative(column)
            
        elif action == "remove_invalid":
            return self._remove_invalid(column, step.get("valid_values"))
            
        elif action == "convert_numeric":
            return self._convert_numeric(column)
            
        elif action == "rename_column":
            return self._rename_column(column, step.get("new_name"))
            
        elif action == "drop_column":
            return self._drop_column(column)
            
        elif action == "add_derived_column":
            return self._add_derived_column(step.get("new_column"), step.get("expression"))
            
        elif action == "standardize_text":
            return self._standardize_text(column, step.get("case"))
            
        elif action == "remove_duplicates":
            return self._remove_duplicates(step.get("subset"))
            
        elif action == "filter_rows":
            return self._filter_rows(step.get("condition"))
            
        else:
            raise ValueError(f"Unknown transformation action: {action}")
    
    def _convert_datetime(self, column: str, fmt: Optional[str] = None) -> str:
        """Convert column to datetime."""
        before_null = self.df[column].isna().sum()
        if fmt:
            self.df[column] = pd.to_datetime(self.df[column], format=fmt, errors='coerce')
        else:
            self.df[column] = pd.to_datetime(self.df[column], errors='coerce')
        after_null = self.df[column].isna().sum()
        return f"Converted to datetime. New nulls from conversion: {after_null - before_null}"
    
    def _fill_null(self, column: str, strategy: str, value: Any = None) -> str:
        """Fill null values using specified strategy."""
        null_count = self.df[column].isna().sum()
        
        if strategy == "value":
            self.df[column].fillna(value, inplace=True)
        elif strategy == "mean":
            self.df[column].fillna(self.df[column].mean(), inplace=True)
        elif strategy == "median":
            self.df[column].fillna(self.df[column].median(), inplace=True)
        elif strategy == "mode":
            self.df[column].fillna(self.df[column].mode()[0], inplace=True)
        elif strategy == "forward_fill":
            self.df[column].fillna(method='ffill', inplace=True)
        elif strategy == "drop":
            self.df = self.df.dropna(subset=[column])
            return f"Dropped {null_count} rows with null values"
            
        return f"Filled {null_count} null values using {strategy}"
    
    def _remove_negative(self, column: str) -> str:
        """Remove rows with negative values in specified column."""
        negative_count = (self.df[column] < 0).sum()
        self.df = self.df[self.df[column] >= 0]
        return f"Removed {negative_count} rows with negative values"
    
    def _remove_invalid(self, column: str, valid_values: List[Any]) -> str:
        """Remove rows with values not in valid_values list."""
        invalid_count = (~self.df[column].isin(valid_values)).sum()
        self.df = self.df[self.df[column].isin(valid_values)]
        return f"Removed {invalid_count} rows with invalid values"
    
    def _convert_numeric(self, column: str) -> str:
        """Convert column to numeric, coercing errors to NaN."""
        before_dtype = str(self.df[column].dtype)
        self.df[column] = pd.to_numeric(self.df[column], errors='coerce')
        return f"Converted from {before_dtype} to {self.df[column].dtype}"
    
    def _rename_column(self, old_name: str, new_name: str) -> str:
        """Rename a column."""
        self.df = self.df.rename(columns={old_name: new_name})
        return f"Renamed '{old_name}' to '{new_name}'"
    
    def _drop_column(self, column: str) -> str:
        """Drop a column."""
        self.df = self.df.drop(columns=[column])
        return f"Dropped column '{column}'"
    
    def _add_derived_column(self, new_column: str, expression: str) -> str:
        """Add a derived column using a safe expression."""
        # Only allow safe operations
        safe_expressions = {
            "trip_duration_minutes": "(self.df['tpep_dropoff_datetime'] - self.df['tpep_pickup_datetime']).dt.total_seconds() / 60",
            "fare_per_mile": "self.df['fare_amount'] / self.df['trip_distance'].replace(0, np.nan)",
            "tip_percentage": "(self.df['tip_amount'] / self.df['fare_amount'].replace(0, np.nan)) * 100"
        }
        
        if new_column in safe_expressions:
            self.df[new_column] = eval(safe_expressions[new_column])
            return f"Added derived column '{new_column}'"
        else:
            raise ValueError(f"Unknown derived column: {new_column}")
    
    def _standardize_text(self, column: str, case: str = "lower") -> str:
        """Standardize text case."""
        if case == "lower":
            self.df[column] = self.df[column].str.lower()
        elif case == "upper":
            self.df[column] = self.df[column].str.upper()
        elif case == "title":
            self.df[column] = self.df[column].str.title()
        return f"Standardized '{column}' to {case}case"
    
    def _remove_duplicates(self, subset: Optional[List[str]] = None) -> str:
        """Remove duplicate rows."""
        before_count = len(self.df)
        self.df = self.df.drop_duplicates(subset=subset)
        removed = before_count - len(self.df)
        return f"Removed {removed} duplicate rows"
    
    def _filter_rows(self, condition: str) -> str:
        """Filter rows based on condition (limited safe conditions)."""
        before_count = len(self.df)
        # Only allow predefined safe conditions
        if condition == "valid_trip_distance":
            self.df = self.df[self.df['trip_distance'] > 0]
        elif condition == "valid_fare":
            self.df = self.df[self.df['fare_amount'] > 0]
        elif condition == "valid_passengers":
            self.df = self.df[self.df['passenger_count'] > 0]
        else:
            raise ValueError(f"Unknown filter condition: {condition}")
        
        removed = before_count - len(self.df)
        return f"Filtered out {removed} rows using condition: {condition}"


def transform_data(df: pd.DataFrame, transformation_plan: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Transform data according to the provided plan.
    
    Args:
        df: Input DataFrame
        transformation_plan: List of transformation steps
        
    Returns:
        Transformation result dictionary
    """
    executor = TransformationExecutor(df)
    return executor.execute_plan(transformation_plan)


def validate_data(df: pd.DataFrame, validation_rules: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Validate transformed data against rules.
    
    Args:
        df: DataFrame to validate
        validation_rules: List of validation rules
        
    Returns:
        Validation result dictionary
    """
    results = {
        "valid": True,
        "checks": [],
        "warnings": [],
        "errors": []
    }
    
    for rule in validation_rules:
        rule_type = rule.get("type")
        column = rule.get("column")
        
        if rule_type == "not_null":
            null_count = df[column].isna().sum()
            passed = null_count == 0
            results["checks"].append({
                "rule": f"{column} should not have nulls",
                "passed": passed,
                "details": f"Found {null_count} null values"
            })
            if not passed:
                results["warnings"].append(f"Column {column} still has {null_count} null values")
                
        elif rule_type == "positive":
            negative_count = (df[column] < 0).sum()
            passed = negative_count == 0
            results["checks"].append({
                "rule": f"{column} should be positive",
                "passed": passed,
                "details": f"Found {negative_count} negative values"
            })
            if not passed:
                results["errors"].append(f"Column {column} has {negative_count} negative values")
                results["valid"] = False
                
        elif rule_type == "in_range":
            min_val, max_val = rule.get("min"), rule.get("max")
            out_of_range = ((df[column] < min_val) | (df[column] > max_val)).sum()
            passed = out_of_range == 0
            results["checks"].append({
                "rule": f"{column} should be in range [{min_val}, {max_val}]",
                "passed": passed,
                "details": f"Found {out_of_range} out of range values"
            })
            if not passed:
                results["warnings"].append(f"Column {column} has {out_of_range} values out of range")
                
        elif rule_type == "unique":
            duplicate_count = len(df) - df[column].nunique()
            passed = duplicate_count == 0
            results["checks"].append({
                "rule": f"{column} should be unique",
                "passed": passed,
                "details": f"Found {duplicate_count} duplicate values"
            })
            
        elif rule_type == "row_count":
            min_rows = rule.get("min", 0)
            passed = len(df) >= min_rows
            results["checks"].append({
                "rule": f"Should have at least {min_rows} rows",
                "passed": passed,
                "details": f"Current row count: {len(df)}"
            })
            if not passed:
                results["errors"].append(f"Row count {len(df)} is below minimum {min_rows}")
                results["valid"] = False
    
    return results


def get_transformation_summary(result: Dict[str, Any]) -> str:
    """Generate a human-readable transformation summary."""
    lines = []
    lines.append("🔄 TRANSFORMATION SUMMARY")
    lines.append("=" * 50)
    lines.append(f"Original rows: {result['original_row_count']}")
    lines.append(f"Final rows: {result['final_row_count']}")
    lines.append(f"Rows affected: {result['original_row_count'] - result['final_row_count']}")
    lines.append("")
    lines.append("📝 TRANSFORMATION LOG:")
    lines.append("-" * 50)
    
    for log in result['transformation_log']:
        status_icon = "✅" if log['status'] == 'success' else "❌"
        lines.append(f"  {status_icon} Step {log['step']}: {log['action']} on '{log.get('column', 'N/A')}'")
        if log['status'] == 'success':
            lines.append(f"      {log['details']}")
        else:
            lines.append(f"      Error: {log['error']}")
    
    return "\n".join(lines)


if __name__ == "__main__":
    # Test transformations
    import sys
    sys.path.append('..')
    from extract_tool import extract_data
    
    result = extract_data("../data/nyc_taxi_sample.csv")
    if result["success"]:
        df = result["data"]
        
        # Sample transformation plan
        plan = [
            {"action": "convert_datetime", "column": "tpep_pickup_datetime"},
            {"action": "convert_datetime", "column": "tpep_dropoff_datetime"},
            {"action": "fill_null", "column": "passenger_count", "strategy": "median"},
            {"action": "remove_negative", "column": "passenger_count"},
        ]
        
        transform_result = transform_data(df, plan)
        print(get_transformation_summary(transform_result))
