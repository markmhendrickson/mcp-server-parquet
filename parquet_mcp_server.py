#!/usr/bin/env python3
"""
MCP Server for Parquet File Interactions

Provides tools for reading, querying, and modifying parquet files
in a repository data directory.
"""

import json
import os
import re
import sys
import uuid
from datetime import date, datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from mcp.server import Server
from mcp.server.stdio import stdio_server
from mcp.types import Tool, TextContent, Prompt, PromptArgument, PromptMessage

# Try to load dotenv for .env file support
try:
    from dotenv import load_dotenv
    DOTENV_AVAILABLE = True
except ImportError:
    DOTENV_AVAILABLE = False

# Optional OpenAI for embeddings
try:
    from openai import OpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    OpenAI = None

# Optional fuzzy matching
try:
    from difflib import SequenceMatcher
    FUZZY_AVAILABLE = True
except ImportError:
    FUZZY_AVAILABLE = False

# Data directory configuration
# Loads from environment variable or .env file (relative to repo root)
DATA_DIR_ENV = os.environ.get("DATA_DIR")
if not DATA_DIR_ENV:
    # Try loading from .env file in repo root
    if DOTENV_AVAILABLE:
        # Find repo root (parent of truth/mcp-servers/parquet)
        script_dir = Path(__file__).parent
        repo_root = script_dir.parent.parent.parent
        env_file = repo_root / ".env"
        if env_file.exists():
            load_dotenv(env_file)
            DATA_DIR_ENV = os.environ.get("DATA_DIR")

if not DATA_DIR_ENV:
    raise RuntimeError(
        "DATA_DIR environment variable is not set. "
        "Please set DATA_DIR in your .env file or as an environment variable. "
        "Example: DATA_DIR=\"/absolute/path/to/data\" in .env file"
    )
DATA_DIR = Path(DATA_DIR_ENV)

SCHEMAS_DIR = DATA_DIR / "schemas"
SNAPSHOTS_DIR = DATA_DIR / "snapshots"
LOGS_DIR = DATA_DIR / "logs"
AUDIT_LOG_PATH = LOGS_DIR / "audit_log.parquet"

# Backup configuration
FULL_SNAPSHOT_ENABLED = os.environ.get("MCP_FULL_SNAPSHOTS", "false").lower() == "true"
SNAPSHOT_FREQUENCY = os.environ.get("MCP_SNAPSHOT_FREQUENCY", "weekly")  # daily, weekly, monthly, never

# Embeddings directory
EMBEDDINGS_DIR = DATA_DIR / "embeddings"
EMBEDDINGS_DIR.mkdir(parents=True, exist_ok=True)

# Ensure directories exist
SNAPSHOTS_DIR.mkdir(parents=True, exist_ok=True)
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# OpenAI client for embeddings (lazy initialization)
_openai_client = None

def get_openai_client():
    """Get or create OpenAI client for embeddings."""
    global _openai_client
    if not OPENAI_AVAILABLE:
        return None
    if _openai_client is None:
        api_key = os.getenv("OPENAI_API_KEY")
        if api_key:
            _openai_client = OpenAI(api_key=api_key)
    return _openai_client

# Initialize MCP server
app = Server("parquet")


def get_parquet_file_path(data_type: str) -> Path:
    """Get the parquet file path for a data type."""
    return DATA_DIR / data_type / f"{data_type}.parquet"


def get_embeddings_file_path(data_type: str) -> Path:
    """Get the embeddings parquet file path for a data type."""
    return EMBEDDINGS_DIR / f"{data_type}_embeddings.parquet"


def apply_enhanced_filter(df: pd.DataFrame, column: str, filter_spec: Any) -> pd.DataFrame:
    """
    Apply enhanced filtering with support for multiple operators.
    
    Filter spec can be:
    - Simple value: exact match
    - List: in list
    - Dict with operator: {"$contains": "text"}, {"$starts_with": "text"}, etc.
    
    Supported operators:
    - $contains: substring match (case-insensitive)
    - $starts_with: prefix match (case-insensitive)
    - $ends_with: suffix match (case-insensitive)
    - $regex: regex pattern match
    - $fuzzy: fuzzy string matching (similarity threshold 0-1)
    - $gt, $gte, $lt, $lte: numeric comparisons
    - $in: list membership (same as passing list directly)
    - $ne: not equal
    """
    if column not in df.columns:
        return df
    
    # Simple value or list
    if not isinstance(filter_spec, dict):
        if isinstance(filter_spec, list):
            return df[df[column].isin(filter_spec)]
        else:
            return df[df[column] == filter_spec]
    
    # Dict with operators
    mask = pd.Series([True] * len(df))
    
    for op, value in filter_spec.items():
        if op == "$contains":
            mask = mask & df[column].astype(str).str.contains(str(value), case=False, na=False, regex=False)
        elif op == "$starts_with":
            mask = mask & df[column].astype(str).str.startswith(str(value), na=False)
        elif op == "$ends_with":
            mask = mask & df[column].astype(str).str.endswith(str(value), na=False)
        elif op == "$regex":
            try:
                mask = mask & df[column].astype(str).str.contains(value, case=False, na=False, regex=True)
            except re.error:
                # Invalid regex, return empty result
                return df.iloc[0:0]
        elif op == "$fuzzy":
            if not FUZZY_AVAILABLE:
                # Fallback to contains if fuzzy not available
                mask = mask & df[column].astype(str).str.contains(str(value), case=False, na=False, regex=False)
            else:
                threshold = value.get("threshold", 0.6) if isinstance(value, dict) else 0.6
                search_text = value.get("text", value) if isinstance(value, dict) else str(value)
                search_text_lower = search_text.lower()
                
                def fuzzy_match(val):
                    if pd.isna(val):
                        return False
                    val_str = str(val).lower()
                    similarity = SequenceMatcher(None, search_text_lower, val_str).ratio()
                    return similarity >= threshold
                
                mask = mask & df[column].apply(fuzzy_match)
        elif op == "$gt":
            mask = mask & (df[column] > value)
        elif op == "$gte":
            mask = mask & (df[column] >= value)
        elif op == "$lt":
            mask = mask & (df[column] < value)
        elif op == "$lte":
            mask = mask & (df[column] <= value)
        elif op == "$in":
            mask = mask & df[column].isin(value if isinstance(value, list) else [value])
        elif op == "$ne":
            mask = mask & (df[column] != value)
        else:
            # Unknown operator, skip
            continue
    
    return df[mask]


def get_schema_path(data_type: str) -> Optional[Path]:
    """Get the schema file path for a data type."""
    schema_file = SCHEMAS_DIR / f"{data_type}_schema.json"
    if schema_file.exists():
        return schema_file
    return None


def load_json_schema(data_type: str) -> Optional[Dict[str, Any]]:
    """Load JSON schema definition for a data type, if available."""
    schema_path = get_schema_path(data_type)
    if not schema_path:
        return None
    with open(schema_path, "r", encoding="utf-8") as f:
        return json.load(f)


def get_date_fields(data_type: str) -> List[str]:
    """Return list of fields declared as date/datetime/timestamp for this data type."""
    schema = load_json_schema(data_type)
    if not schema:
        return []
    result: List[str] = []
    for name, type_name in schema.get("schema", {}).items():
        if type_name in {"date", "datetime", "timestamp"}:
            result.append(name)
    return result


def coerce_record_dates(data_type: str, record: Dict[str, Any]) -> Dict[str, Any]:
    """Coerce date/datetime fields in a single record according to schema."""
    date_fields = get_date_fields(data_type)
    if not date_fields:
        return record

    new_record = dict(record)
    for field in date_fields:
        if field not in new_record:
            continue
        value = new_record[field]
        if value is None:
            continue
        if isinstance(value, (datetime, date)):
            continue
        if isinstance(value, str):
            try:
                dt = datetime.fromisoformat(value)
            except ValueError:
                try:
                    dt = datetime.strptime(value, "%Y-%m-%d")
                except Exception:
                    # Leave as-is if parsing fails
                    continue
            # For 'date' fields, downcast to date; for 'timestamp'/'datetime', keep as datetime
            schema = load_json_schema(data_type) or {}
            type_name = schema.get("schema", {}).get(field)
            if type_name == "date":
                new_record[field] = dt.date()
            elif type_name in {"datetime", "timestamp"}:
                new_record[field] = dt
            else:
                new_record[field] = dt
    return new_record

def coerce_date_columns(df: pd.DataFrame, data_type: Optional[str] = None) -> pd.DataFrame:
    """Coerce any *date*/*time* columns according to JSON schema for stable parquet writes."""
    if df.empty:
        return df
    
    if data_type:
        json_schema = load_json_schema(data_type)
        if json_schema:
            schema_def = json_schema.get("schema", {})
            for col_name, type_name in schema_def.items():
                if col_name not in df.columns:
                    continue
                if type_name == "date":
                    # Convert to datetime.date objects (not pd.Timestamp, not strings)
                    # Use apply for robust conversion that handles all types
                    def to_date_safe(val):
                        if val is None or pd.isna(val):
                            return None
                        if isinstance(val, date) and not isinstance(val, datetime):
                            return val
                        if isinstance(val, datetime):
                            return val.date()
                        if isinstance(val, pd.Timestamp):
                            return val.date()
                        if isinstance(val, str):
                            try:
                                dt = pd.to_datetime(val, errors="coerce")
                                if pd.isna(dt):
                                    return None
                                return dt.date()
                            except:
                                return None
                        # Unknown type, try to convert
                        try:
                            dt = pd.to_datetime(val, errors="coerce")
                            if pd.isna(dt):
                                return None
                            return dt.date()
                        except:
                            return None
                    
                    df[col_name] = df[col_name].apply(to_date_safe)
                    
                elif type_name in {"datetime", "timestamp"}:
                    # Convert to pd.Timestamp with UTC timezone to match schema
                    df[col_name] = pd.to_datetime(df[col_name], errors="coerce", utc=True)
            return df
    
    # Fallback: generic coercion for date/time columns
    for col in df.columns:
        name = str(col).lower()
        if "date" in name or "time" in name:
            try:
                df[col] = pd.to_datetime(df[col], errors="coerce")
            except Exception:
                continue
    return df

def get_pyarrow_schema_for_write(data_type: str) -> Optional[pa.Schema]:
    """Get pyarrow schema for writing, if available."""
    try:
        # Try to import from scripts module
        import sys
        # Try to find scripts directory (optional, for backward compatibility)
        scripts_path = None
        server_dir = Path(__file__).parent
        possible_roots = [
            server_dir.parent.parent.parent.parent,  # truth/mcp-servers/parquet -> truth -> personal
            server_dir.parent.parent.parent,  # mcp-servers/parquet -> mcp-servers -> truth
        ]
        
        for root in possible_roots:
            test_scripts = root / "execution" / "scripts"
            if test_scripts.exists():
                scripts_path = test_scripts
                break
        
        if scripts_path and str(scripts_path) not in sys.path:
            sys.path.insert(0, str(scripts_path))
        from parquet_schema_definitions import get_pyarrow_schema
        return get_pyarrow_schema(data_type)
    except (ImportError, AttributeError):
        # Fallback: read existing schema from parquet file
        file_path = get_parquet_file_path(data_type)
        if file_path.exists():
            try:
                return pq.read_schema(file_path)
            except Exception:
                pass
    return None

def write_parquet_with_schema(df: pd.DataFrame, file_path: Path, data_type: str) -> None:
    """Write parquet file using explicit schema if available, otherwise fallback to default."""
    # For now, write without explicit schema to avoid timestamp conversion issues
    # PyArrow will preserve existing column types when appending to existing files
    # TODO: Fix timestamp schema conversion issue
    try:
        df.to_parquet(file_path, index=False, engine="pyarrow")
    except Exception as e:
        print(f"Error writing parquet file: {e}", file=sys.stderr)
        raise

def create_snapshot(file_path: Path) -> Path:
    """Create a timestamped snapshot of a parquet file."""
    timestamp = datetime.now().strftime('%Y-%m-%d-%H%M%S')
    filename = file_path.stem
    snapshot_path = SNAPSHOTS_DIR / f"{filename}-{timestamp}.parquet"
    
    if file_path.exists():
        df = pd.read_parquet(file_path)
        # Infer data_type from file path
        data_type = file_path.parent.name
        df = coerce_date_columns(df, data_type)
        # Add missing schema columns before creating snapshot
        json_schema = load_json_schema(data_type)
        if json_schema:
            schema_def = json_schema.get("schema", {})
            for key in schema_def.keys():
                if key not in df.columns:
                    df[key] = None
        # Use explicit schema for snapshot too
        schema = get_pyarrow_schema_for_write(data_type)
        if schema:
            table = pa.Table.from_pandas(df, schema=schema, preserve_index=False, safe=False)
            pq.write_table(table, snapshot_path)
        else:
            df.to_parquet(snapshot_path, index=False, engine="pyarrow")
        return snapshot_path
    return None


def should_create_full_snapshot(data_type: str) -> bool:
    """Determine if a full snapshot should be created based on configuration."""
    if not FULL_SNAPSHOT_ENABLED:
        return False
    
    if SNAPSHOT_FREQUENCY == "never":
        return False
    
    # Check if recent snapshot exists
    file_path = get_parquet_file_path(data_type)
    if not file_path.exists():
        return False
    
    filename = file_path.stem
    # Find most recent snapshot for this data type
    snapshots = sorted(SNAPSHOTS_DIR.glob(f"{filename}-*.parquet"), reverse=True)
    
    if not snapshots:
        return True  # No snapshot exists, create one
    
    most_recent = snapshots[0]
    # Extract timestamp from filename: {filename}-YYYY-MM-DD-HHMMSS.parquet
    try:
        timestamp_str = most_recent.stem.split('-', 1)[1]  # Get everything after first dash
        snapshot_time = datetime.strptime(timestamp_str, '%Y-%m-%d-%H%M%S')
        
        now = datetime.now()
        age = now - snapshot_time
        
        if SNAPSHOT_FREQUENCY == "daily":
            return age.days >= 1
        elif SNAPSHOT_FREQUENCY == "weekly":
            return age.days >= 7
        elif SNAPSHOT_FREQUENCY == "monthly":
            return age.days >= 30
    except (ValueError, IndexError):
        return True  # If we can't parse, create snapshot
    
    return False


def create_audit_entry(
    operation: str,
    data_type: str,
    record_id: str,
    affected_fields: List[str] = None,
    old_values: Dict[str, Any] = None,
    new_values: Dict[str, Any] = None,
    snapshot_reference: str = None,
    notes: str = None
) -> Dict[str, Any]:
    """Create an audit log entry for a data modification."""
    audit_entry = {
        "audit_id": str(uuid.uuid4())[:16],
        "timestamp": datetime.now(),
        "operation": operation,
        "data_type": data_type,
        "record_id": record_id,
        "affected_fields": json.dumps(affected_fields) if affected_fields else None,
        "old_values": json.dumps(old_values, default=str) if old_values else None,
        "new_values": json.dumps(new_values, default=str) if new_values else None,
        "user": "mcp_server",
        "snapshot_reference": snapshot_reference,
        "notes": notes
    }
    
    # Append to audit log
    try:
        if AUDIT_LOG_PATH.exists():
            df_audit = pd.read_parquet(AUDIT_LOG_PATH)
            df_audit = pd.concat([df_audit, pd.DataFrame([audit_entry])], ignore_index=True)
        else:
            df_audit = pd.DataFrame([audit_entry])
        
        df_audit = coerce_date_columns(df_audit, "audit_log")
        # Write audit log without explicit schema to avoid conversion issues
        df_audit.to_parquet(AUDIT_LOG_PATH, index=False, engine="pyarrow")
    except Exception as e:
        # Log error but don't fail the operation
        print(f"Warning: Failed to create audit entry: {e}", file=sys.stderr)
    
    return audit_entry


def list_available_data_types() -> List[str]:
    """List all available data types (directories with parquet files)."""
    data_types = []
    for item in DATA_DIR.iterdir():
        if item.is_dir() and not item.name.startswith('_'):
            parquet_file = item / f"{item.name}.parquet"
            if parquet_file.exists():
                data_types.append(item.name)
    return sorted(data_types)


def get_enum_order_from_schema(data_type: str, column: str) -> Optional[List[str]]:
    """Get enum order from schema metadata if available."""
    schema = load_json_schema(data_type)
    if not schema:
        return None
    
    schema_def = schema.get("schema", {})
    if column not in schema_def:
        return None
    
    column_def = schema_def[column]
    if isinstance(column_def, dict) and "enum_order" in column_def:
        return column_def["enum_order"]
    elif isinstance(column_def, dict) and "enum" in column_def:
        # If enum is defined but no order, return enum values as-is
        enum_vals = column_def.get("enum")
        if isinstance(enum_vals, list):
            return enum_vals
    return None


def apply_sorting(df: pd.DataFrame, sort_by: List[Dict[str, Any]], data_type: Optional[str] = None) -> pd.DataFrame:
    """
    Apply sorting to dataframe with support for custom enum ordering and null handling.
    
    Args:
        df: DataFrame to sort
        sort_by: List of sort specifications, each with:
            - column: column name
            - ascending: bool (default: True)
            - na_position: str (default: 'last') - where to place nulls: 'first' or 'last'
            - custom_order: optional list for enum ordering
        data_type: Optional data type for schema lookup
    """
    if not sort_by:
        return df
    
    df = df.copy()
    sort_keys = []
    sort_ascending = []
    na_positions = []
    
    for sort_spec in sort_by:
        column = sort_spec["column"]
        if column not in df.columns:
            continue
        
        ascending = sort_spec.get("ascending", True)
        na_position = sort_spec.get("na_position", "last")
        custom_order = sort_spec.get("custom_order")
        
        # Try to get enum order from schema if not provided
        if custom_order is None and data_type:
            custom_order = get_enum_order_from_schema(data_type, column)
        
        if custom_order:
            # Create categorical with custom order
            df[f"{column}_sort"] = pd.Categorical(
                df[column],
                categories=custom_order,
                ordered=True
            )
            sort_keys.append(f"{column}_sort")
        else:
            sort_keys.append(column)
        
        sort_ascending.append(ascending)
        na_positions.append(na_position)
    
    if sort_keys:
        # Use the na_position from the first sort column (pandas only supports one na_position)
        # If multiple columns have different na_position, use the first one
        primary_na_position = na_positions[0] if na_positions else 'last'
        df = df.sort_values(sort_keys, ascending=sort_ascending, na_position=primary_na_position)
        # Remove temporary sort columns
        for col in df.columns:
            if col.endswith("_sort"):
                df = df.drop(columns=[col])
    
    return df


@app.list_tools()
async def list_tools() -> List[Tool]:
    """List available MCP tools."""
    return [
        Tool(
            name="list_data_types",
            description="List all available data types (parquet files) in the data directory",
            inputSchema={
                "type": "object",
                "properties": {},
            },
        ),
        Tool(
            name="get_schema",
            description="Get the schema definition for a data type. Use only when you need schema information (field names, types, constraints, valid enum values) to build queries, filters, or add/update records. Do not call for routine data retrieval - read_parquet returns all necessary field information.",
            inputSchema={
                "type": "object",
                "properties": {
                    "data_type": {
                        "type": "string",
                        "description": "The data type name (e.g., 'flows', 'transactions', 'tasks')",
                    },
                },
                "required": ["data_type"],
            },
        ),
        Tool(
            name="read_parquet",
            description="Read and query a parquet file with optional filters and sorting. Supports enhanced filtering operators: $contains, $starts_with, $ends_with, $regex, $fuzzy, $gt, $gte, $lt, $lte, $in, $ne",
            inputSchema={
                "type": "object",
                "properties": {
                    "data_type": {
                        "type": "string",
                        "description": "The data type name (e.g., 'flows', 'transactions', 'tasks')",
                    },
                    "filters": {
                        "type": "object",
                        "description": "Optional filters to apply. Can use enhanced operators: {\"title\": {\"$contains\": \"therapy\"}} or {\"title\": {\"$fuzzy\": {\"text\": \"therapy\", \"threshold\": 0.7}}}",
                        "additionalProperties": True,
                    },
                    "sort_by": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column": {"type": "string"},
                                "ascending": {"type": "boolean", "default": True},
                                "na_position": {
                                    "type": "string",
                                    "enum": ["last", "first"],
                                    "default": "last",
                                    "description": "Where to place null/NaN values when sorting: 'last' (default) or 'first'"
                                },
                                "custom_order": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Custom order for enum values (e.g., [\"critical\", \"high\", \"medium\", \"low\"]). If not provided, will try to get from schema metadata."
                                }
                            },
                            "required": ["column"]
                        },
                        "description": "Optional list of sort specifications. Each can have column, ascending (default: true), na_position (default: 'last'), and custom_order for enum columns."
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of rows to return (default: 1000)",
                        "default": 1000,
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of column names to return (default: all columns)",
                    },
                },
                "required": ["data_type"],
            },
        ),
        Tool(
            name="search_parquet",
            description="Semantic search using embeddings. Searches text fields (title, description, notes, etc.) for semantically similar records.",
            inputSchema={
                "type": "object",
                "properties": {
                    "data_type": {
                        "type": "string",
                        "description": "The data type name (e.g., 'tasks', 'contacts', 'transactions')",
                    },
                    "query": {
                        "type": "string",
                        "description": "Search query text to find semantically similar records",
                    },
                    "text_fields": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of text fields to search (default: auto-detect common text fields like title, description, name, notes)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of results to return (default: 10)",
                        "default": 10,
                    },
                    "min_similarity": {
                        "type": "number",
                        "description": "Minimum cosine similarity threshold (0-1, default: 0.7)",
                        "default": 0.7,
                    },
                    "additional_filters": {
                        "type": "object",
                        "description": "Optional additional filters to apply (same format as read_parquet filters)",
                        "additionalProperties": True,
                    },
                },
                "required": ["data_type", "query"],
            },
        ),
        Tool(
            name="generate_embeddings",
            description="Generate and store embeddings for text fields in a data type. Creates or updates embeddings parquet file.",
            inputSchema={
                "type": "object",
                "properties": {
                    "data_type": {
                        "type": "string",
                        "description": "The data type name (e.g., 'tasks', 'contacts')",
                    },
                    "text_fields": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of text fields to generate embeddings for (default: auto-detect common text fields)",
                    },
                    "force_regenerate": {
                        "type": "boolean",
                        "description": "Force regeneration of all embeddings even if they exist (default: false)",
                        "default": False,
                    },
                },
                "required": ["data_type"],
            },
        ),
        Tool(
            name="add_record",
            description="Add a new record to a parquet file. Creates audit log entry and optionally creates full snapshot based on configuration. MANDATORY: Proactively use this tool to persist any relevant structured data that emerges during conversations—do not wait for explicit user requests. Persist data when user mentions: financial transactions/expenses/income/flows, tasks/projects/outcomes, contacts/companies/relationships, purchases/orders/contracts, events/appointments, health/workout/food/meal info, properties/equipment/locations, investments/holdings/crypto/tax events, beliefs/goals, or any other structured information matching existing data types. Check for existing records first to avoid duplicates.",
            inputSchema={
                "type": "object",
                "properties": {
                    "data_type": {
                        "type": "string",
                        "description": "The data type name (e.g., 'flows', 'transactions', 'tasks')",
                    },
                    "record": {
                        "type": "object",
                        "description": "The record data as a JSON object matching the schema",
                        "additionalProperties": True,
                    },
                },
                "required": ["data_type", "record"],
            },
        ),
        Tool(
            name="update_records",
            description="Update existing records in a parquet file. Creates audit log entry and optionally creates full snapshot based on configuration. MANDATORY: Proactively use this tool to update any relevant structured data that emerges during conversations—do not wait for explicit user requests. Use when updating existing records matching specific criteria (use upsert_record if you want to create if not found).",
            inputSchema={
                "type": "object",
                "properties": {
                    "data_type": {
                        "type": "string",
                        "description": "The data type name (e.g., 'flows', 'transactions', 'tasks')",
                    },
                    "filters": {
                        "type": "object",
                        "description": "Filters to identify records to update (key-value pairs)",
                        "additionalProperties": True,
                    },
                    "updates": {
                        "type": "object",
                        "description": "Fields to update (key-value pairs)",
                        "additionalProperties": True,
                    },
                },
                "required": ["data_type", "filters", "updates"],
            },
        ),
        Tool(
            name="upsert_record",
            description="Insert or update a record (upsert). Checks for existing records using enhanced filters (supports $contains, $fuzzy, etc.). If found, updates matching records. If not found, creates a new record. Returns whether it created or updated. MANDATORY: Proactively use this tool to persist any relevant structured data that emerges during conversations—do not wait for explicit user requests. RECOMMENDED for contacts, tasks, and other records where duplicates should be avoided. Use this instead of add_record when you want to check for and update existing records automatically.",
            inputSchema={
                "type": "object",
                "properties": {
                    "data_type": {
                        "type": "string",
                        "description": "The data type name (e.g., 'contacts', 'tasks', 'transactions')",
                    },
                    "filters": {
                        "type": "object",
                        "description": "Enhanced filters to identify existing records (supports all read_parquet filter operators: $contains, $fuzzy, $starts_with, $ends_with, $regex, $gt, $gte, $lt, $lte, $in, $ne, or simple values)",
                        "additionalProperties": True,
                    },
                    "record": {
                        "type": "object",
                        "description": "The record data to insert or update. If updating, this data will be merged with existing record(s).",
                        "additionalProperties": True,
                    },
                },
                "required": ["data_type", "filters", "record"],
            },
        ),
        Tool(
            name="delete_records",
            description="Delete records from a parquet file. Creates audit log entry and optionally creates full snapshot based on configuration.",
            inputSchema={
                "type": "object",
                "properties": {
                    "data_type": {
                        "type": "string",
                        "description": "The data type name (e.g., 'flows', 'transactions', 'tasks')",
                    },
                    "filters": {
                        "type": "object",
                        "description": "Filters to identify records to delete (key-value pairs)",
                        "additionalProperties": True,
                    },
                },
                "required": ["data_type", "filters"],
            },
        ),
        Tool(
            name="get_statistics",
            description="Get comprehensive statistics about a parquet file including numeric summaries, date ranges, and categorical distributions",
            inputSchema={
                "type": "object",
                "properties": {
                    "data_type": {
                        "type": "string",
                        "description": "The data type name (e.g., 'flows', 'transactions', 'tasks')",
                    },
                    "filters": {
                        "type": "object",
                        "description": "Optional filters to apply before calculating statistics (same format as read_parquet filters)",
                        "additionalProperties": True,
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of specific columns to analyze (default: all numeric/date columns)",
                    },
                    "include_distribution": {
                        "type": "boolean",
                        "description": "Include percentile distribution for numeric columns (default: false)",
                        "default": False,
                    },
                },
                "required": ["data_type"],
            },
        ),
        Tool(
            name="aggregate_parquet",
            description="Perform generic groupby and aggregation operations on any data type. Supports sum, count, avg, mean, min, max, std aggregations.",
            inputSchema={
                "type": "object",
                "properties": {
                    "data_type": {
                        "type": "string",
                        "description": "The data type name (e.g., 'flows', 'transactions', 'tasks')",
                    },
                    "group_by": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "List of columns to group by",
                    },
                    "aggregations": {
                        "type": "object",
                        "description": "Dictionary of column → aggregation function(s). Functions: sum, count, avg, mean, min, max, std, first, last. Can specify multiple: {\"amount_usd\": [\"sum\", \"count\"]}",
                        "additionalProperties": True,
                    },
                    "filters": {
                        "type": "object",
                        "description": "Optional pre-aggregation filters (same format as read_parquet filters)",
                        "additionalProperties": True,
                    },
                    "sort_by": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column": {"type": "string"},
                                "ascending": {"type": "boolean", "default": True}
                            },
                            "required": ["column"]
                        },
                        "description": "Optional sort specifications for aggregated results"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of groups to return (default: 1000)",
                        "default": 1000,
                    },
                },
                "required": ["data_type", "aggregations"],
            },
        ),
        Tool(
            name="query_with_date_range",
            description="Query with date range filtering on any date column. Supports grouping by time period (day, week, month, quarter, year).",
            inputSchema={
                "type": "object",
                "properties": {
                    "data_type": {
                        "type": "string",
                        "description": "The data type name (e.g., 'flows', 'transactions', 'tasks')",
                    },
                    "date_column": {
                        "type": "string",
                        "description": "Name of date column to filter on",
                    },
                    "date_start": {
                        "type": "string",
                        "description": "Start date in ISO format (YYYY-MM-DD)",
                    },
                    "date_end": {
                        "type": "string",
                        "description": "End date in ISO format (YYYY-MM-DD)",
                    },
                    "filters": {
                        "type": "object",
                        "description": "Optional additional filters (same format as read_parquet filters)",
                        "additionalProperties": True,
                    },
                    "group_by_period": {
                        "type": "string",
                        "enum": ["day", "week", "month", "quarter", "year"],
                        "description": "Optional: Group results by time period",
                    },
                    "aggregations": {
                        "type": "object",
                        "description": "Optional aggregations to apply when grouping by period (same format as aggregate_parquet)",
                        "additionalProperties": True,
                    },
                    "sort_by": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column": {"type": "string"},
                                "ascending": {"type": "boolean", "default": True}
                            },
                            "required": ["column"]
                        },
                        "description": "Optional sort specifications"
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of rows to return (default: 1000)",
                        "default": 1000,
                    },
                },
                "required": ["data_type", "date_column"],
            },
        ),
        Tool(
            name="sort_parquet",
            description="Sort query results by any columns with support for custom enum ordering. Uses schema metadata for enum orders when available.",
            inputSchema={
                "type": "object",
                "properties": {
                    "data_type": {
                        "type": "string",
                        "description": "The data type name (e.g., 'flows', 'transactions', 'tasks')",
                    },
                    "sort_by": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "column": {"type": "string"},
                                "ascending": {"type": "boolean", "default": True},
                                "custom_order": {
                                    "type": "array",
                                    "items": {"type": "string"},
                                    "description": "Custom order for enum values (e.g., [\"critical\", \"high\", \"medium\", \"low\"]). If not provided, will try to get from schema metadata."
                                }
                            },
                            "required": ["column"]
                        },
                        "description": "List of sort specifications. Each can have column, ascending (default: true), and custom_order for enum columns."
                    },
                    "filters": {
                        "type": "object",
                        "description": "Optional filters to apply before sorting (same format as read_parquet filters)",
                        "additionalProperties": True,
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of rows to return (default: 1000)",
                        "default": 1000,
                    },
                    "columns": {
                        "type": "array",
                        "items": {"type": "string"},
                        "description": "Optional list of column names to return (default: all columns)",
                    },
                },
                "required": ["data_type", "sort_by"],
            },
        ),
        Tool(
            name="read_audit_log",
            description="Read audit log entries with optional filters",
            inputSchema={
                "type": "object",
                "properties": {
                    "data_type": {
                        "type": "string",
                        "description": "Filter by data type (optional)",
                    },
                    "operation": {
                        "type": "string",
                        "description": "Filter by operation: add, update, delete (optional)",
                    },
                    "record_id": {
                        "type": "string",
                        "description": "Filter by specific record ID (optional)",
                    },
                    "limit": {
                        "type": "integer",
                        "description": "Maximum number of entries to return (default: 100)",
                        "default": 100,
                    },
                },
            },
        ),
        Tool(
            name="rollback_operation",
            description="Rollback a specific operation using its audit ID. Creates inverse operation to undo changes.",
            inputSchema={
                "type": "object",
                "properties": {
                    "audit_id": {
                        "type": "string",
                        "description": "The audit ID of the operation to rollback",
                    },
                },
                "required": ["audit_id"],
            },
        ),
    ]


@app.list_prompts()
async def list_prompts() -> List[Prompt]:
    """List available prompts for agent guidance."""
    return [
        Prompt(
            name="data_persistence_guidance",
            description="Guidance on proactively persisting structured data from conversations",
            arguments=[
                PromptArgument(
                    name="context",
                    description="Brief context about the conversation or task",
                    required=False,
                ),
            ],
        ),
    ]


@app.get_prompt()
async def get_prompt(name: str, arguments: Dict[str, Any]) -> List[PromptMessage]:
    """Get prompt content."""
    if name == "data_persistence_guidance":
        context = arguments.get("context", "")
        guidance = """# MANDATORY: Proactive Memory Storage via MCP Server

**CRITICAL:** You MUST use this MCP server proactively as a general memory store. Do NOT wait for explicit user requests to store data.

## Core Requirements

1. **MANDATORY:** Always use MCP server tools (`add_record`, `update_records`, `upsert_record`) to create and update records—never access parquet files directly.

2. **MANDATORY:** Proactively identify and persist any relevant structured data that emerges during conversations, including:
   - Personal context, preferences, and behavioral patterns
   - Financial transactions, expenses, income, or flows
   - Tasks, projects, outcomes, and workstreams
   - Contacts, companies, relationships, and interactions
   - Purchases, orders, contracts, or agreements
   - Events, appointments, or recurring events
   - Health, workout, food, or meal information
   - Properties, equipment, locations, or assets
   - Investments, holdings, crypto transactions, or tax events
   - Beliefs, goals, preferences, or decision-making patterns
   - Any other structured information that would be useful for future reference

3. **MANDATORY:** Use existing data types when available, or create new data types as needed. Use `list_data_types` to discover available types, `get_schema` to understand structure, and `read_parquet` to check for existing records before adding.

4. **MANDATORY:** Do not wait for explicit user requests. If data would be useful for remembering personal context or future reference, store it immediately using MCP tools.

## Process

1. **Identify relevant data** during conversation—any structured information that would help remember user context
2. **Check existing data types** using `list_data_types` to find appropriate type
3. **Review schema** using `get_schema` to understand required fields and structure
4. **Check for duplicates** using `read_parquet` with appropriate filters
5. **Create or update records** using:
   - `upsert_record` (recommended for contacts, tasks, and other records where duplicates should be avoided)
   - `add_record` (for new entries when duplicates are acceptable)
   - `update_records` (when updating existing records matching specific criteria)
6. **Link related data** via appropriate ID fields when relationships exist

## Examples

- User mentions subscription cost → **Immediately** add to `fixed_costs` using `add_record`
- User discusses property maintenance expense → **Immediately** add to `flows` using `add_record`
- User shares contact details → **Immediately** update `contacts` using `upsert_record` (check for existing contact first)
- User mentions workout → **Immediately** add to `workouts` using `add_record`
- User discusses task → **Immediately** create/update `tasks` with outcome/project links using `upsert_record`
- User expresses preference or behavioral pattern → **Immediately** store in appropriate data type or create new type if needed
- User mentions account creation → **Immediately** add to `user_accounts` using `upsert_record`

## Prohibited Actions

- ❌ **NEVER** access parquet files directly with Python scripts or file operations
- ❌ **NEVER** wait for explicit user request to store relevant data
- ❌ **NEVER** skip storing data because "user didn't ask for it"
- ❌ **NEVER** bypass MCP server tools for any data operations

## Default Behavior

Treat every conversation as an opportunity to build and maintain the user's personal context memory. Store data proactively, not reactively.

Context: {context}
""".format(context=context or "General conversation")
        
        return [
            PromptMessage(
                role="user",
                content=TextContent(type="text", text=guidance),
            ),
        ]
    
    raise ValueError(f"Unknown prompt: {name}")


@app.call_tool()
async def call_tool(name: str, arguments: Dict[str, Any]) -> List[TextContent]:
    """Handle tool calls."""
    
    if name == "list_data_types":
        data_types = list_available_data_types()
        return [TextContent(
            type="text",
            text=json.dumps({
                "data_types": data_types,
                "count": len(data_types),
            }, indent=2)
        )]
    
    elif name == "get_schema":
        data_type = arguments["data_type"]
        schema_path = get_schema_path(data_type)
        
        if not schema_path:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": f"Schema not found for data type: {data_type}",
                }, indent=2)
            )]
        
        with open(schema_path, 'r') as f:
            schema = json.load(f)
        
        return [TextContent(
            type="text",
            text=json.dumps(schema, indent=2)
        )]
    
    elif name == "read_parquet":
        data_type = arguments["data_type"]
        filters = arguments.get("filters", {})
        limit = arguments.get("limit", 1000)
        columns = arguments.get("columns")
        
        file_path = get_parquet_file_path(data_type)
        
        if not file_path.exists():
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": f"Parquet file not found: {file_path}",
                }, indent=2)
            )]
        
        try:
            df = pd.read_parquet(file_path)
            
            # Apply enhanced filters
            for key, value in filters.items():
                df = apply_enhanced_filter(df, key, value)
            
            # Apply sorting if specified
            sort_by = arguments.get("sort_by")
            if sort_by:
                df = apply_sorting(df, sort_by, data_type)
            
            # Select columns if specified
            if columns:
                missing_cols = [c for c in columns if c not in df.columns]
                if missing_cols:
                    return [TextContent(
                        type="text",
                        text=json.dumps({
                            "error": f"Columns not found: {missing_cols}",
                            "available_columns": list(df.columns),
                        }, indent=2)
                    )]
                df = df[columns]
            
            # Apply limit
            df = df.head(limit)
            
            # Convert to JSON-serializable format
            result = df.to_dict(orient='records')
            
            # Convert date/datetime objects to strings
            for record in result:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif isinstance(value, (pd.Timestamp, datetime)):
                        record[key] = value.isoformat()
                    elif isinstance(value, date):
                        record[key] = value.isoformat()
            
            return [TextContent(
                type="text",
                text=json.dumps({
                    "count": len(result),
                    "total_rows": len(df) if limit >= len(df) else f"{len(df)}+",
                    "data": result,
                }, indent=2, default=str)
            )]
        
        except Exception as e:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": str(e),
                }, indent=2)
            )]
    
    elif name == "add_record":
        data_type = arguments["data_type"]
        record = arguments["record"]
        
        file_path = get_parquet_file_path(data_type)
        
        if not file_path.exists():
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": f"Parquet file not found: {file_path}",
                }, indent=2)
            )]
        
        try:
            # Always create snapshot before modification (per policy)
            snapshot_path = create_snapshot(file_path)
            
            # Load existing data
            df = pd.read_parquet(file_path)
            
            # Generate ID if needed (check for common ID patterns)
            id_field = f"{data_type.rstrip('s')}_id"
            if id_field not in record and id_field in df.columns:
                record[id_field] = str(uuid.uuid4())[:16]
            
            # Set import_date if not provided, or normalize if provided as string
            if "import_date" not in record and "import_date" in df.columns:
                record["import_date"] = date.today()
            elif "import_date" in record and isinstance(record["import_date"], str):
                try:
                    record["import_date"] = date.fromisoformat(record["import_date"])
                except ValueError:
                    # Leave as-is if it cannot be parsed
                    pass
            
            # Set import_source_file if not provided
            if "import_source_file" not in record and "import_source_file" in df.columns:
                record["import_source_file"] = "mcp_manual_entry"
            
            # Set timestamp fields if not provided or None (for tasks and similar)
            schema = load_json_schema(data_type)
            if schema:
                schema_def = schema.get("schema", {})
                # Use UTC timezone-aware datetime for timestamps
                from datetime import timezone
                now = datetime.now(timezone.utc)
                for field, type_name in schema_def.items():
                    if type_name == "timestamp" and field in df.columns:
                        # Set created_at/updated_at to current time if not provided or None
                        if field not in record or record.get(field) is None:
                            record[field] = now
            
            # Normalize date fields according to schema
            record = coerce_record_dates(data_type, record)
            
            # Get record ID for audit log
            record_id = record.get(id_field, "unknown")

            # Ensure all columns exist in record (set to None for missing ones)
            for col in df.columns:
                if col not in record:
                    record[col] = None
            
            # Create new row DataFrame
            new_row = pd.DataFrame([record])
            
            # Concatenate
            df_new = pd.concat([df, new_row], ignore_index=True)
            
            # Coerce date/timestamp columns according to schema
            df_new = coerce_date_columns(df_new, data_type)
            
            # Ensure timestamp columns match existing schema (handle timezone)
            # Read existing file schema to match format
            file_path = get_parquet_file_path(data_type)
            if file_path.exists():
                try:
                    existing_schema = pq.read_schema(file_path)
                    for field in existing_schema:
                        col_name = field.name
                        if col_name in df_new.columns and isinstance(field.type, pa.TimestampType):
                            # Ensure timezone-aware UTC timestamps to match schema
                            # Convert to datetime64[ns, UTC] dtype (not object)
                            df_new[col_name] = pd.to_datetime(df_new[col_name], utc=True, errors="coerce")
                            # Ensure dtype is datetime64, not object
                            if df_new[col_name].dtype == 'object':
                                # Force conversion if still object dtype
                                df_new[col_name] = pd.to_datetime(df_new[col_name], utc=True, errors="coerce")
                except Exception:
                    pass
            
            # Save parquet file (without explicit schema to avoid conversion issues)
            write_parquet_with_schema(df_new, file_path, data_type)
            
            # Create audit log entry (errors here won't fail the operation)
            try:
                audit_entry = create_audit_entry(
                    operation="add",
                    data_type=data_type,
                    record_id=record_id,
                    affected_fields=list(record.keys()),
                    new_values=record,
                    snapshot_reference=str(snapshot_path) if snapshot_path else None,
                    notes=f"Added new record via MCP"
                )
            except Exception as audit_error:
                # Don't fail the operation if audit log fails
                audit_entry = {"audit_id": "failed", "error": str(audit_error)}
            
            return [TextContent(
                type="text",
                text=json.dumps({
                    "success": True,
                    "audit_id": audit_entry["audit_id"],
                    "snapshot_created": str(snapshot_path) if snapshot_path else None,
                    "total_records": len(df_new),
                    "added_record": record,
                }, indent=2, default=str)
            )]
        
        except Exception as e:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": str(e),
                }, indent=2)
            )]
    
    elif name == "update_records":
        data_type = arguments["data_type"]
        filters = arguments["filters"]
        updates = arguments["updates"]
        
        file_path = get_parquet_file_path(data_type)
        
        if not file_path.exists():
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": f"Parquet file not found: {file_path}",
                }, indent=2)
            )]
        
        try:
            # Always create snapshot before modification (per policy)
            snapshot_path = create_snapshot(file_path)
            
            # Load existing data
            df = pd.read_parquet(file_path)
            
            # Create mask for matching records
            mask = pd.Series([True] * len(df))
            for key, value in filters.items():
                if key in df.columns:
                    if isinstance(value, list):
                        mask = mask & df[key].isin(value)
                    else:
                        mask = mask & (df[key] == value)
            
            # Count matches
            match_count = mask.sum()
            
            if match_count == 0:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "success": False,
                        "error": "No records matched the filters",
                        "filters": filters,
                    }, indent=2)
                )]
            
            # Get old values for audit log (for affected records)
            id_field = f"{data_type.rstrip('s')}_id"
            affected_records = df[mask]
            
            # Create audit entries for each updated record
            audit_ids = []
            for idx, row in affected_records.iterrows():
                record_id = row.get(id_field, f"idx_{idx}")
                old_values = {k: row[k] for k in updates.keys() if k in row.index}
                
                audit_entry = create_audit_entry(
                    operation="update",
                    data_type=data_type,
                    record_id=str(record_id),
                    affected_fields=list(updates.keys()),
                    old_values=old_values,
                    new_values=updates,
                    snapshot_reference=str(snapshot_path) if snapshot_path else None,
                    notes=f"Updated record via MCP with filters: {json.dumps(filters)}"
                )
                audit_ids.append(audit_entry["audit_id"])
            
            # Apply updates
            updates = coerce_record_dates(data_type, updates)
            
            # Check schema for missing columns and add them (all schema columns, not just update keys)
            schema = load_json_schema(data_type)
            if schema:
                schema_def = schema.get("schema", {})
                # Add all missing schema columns, not just those in updates
                for key in schema_def.keys():
                    if key not in df.columns:
                        # Add new column with None values
                        df[key] = None
            
            for key, value in updates.items():
                if key in df.columns:
                    df.loc[mask, key] = value
                elif key in schema_def if schema else False:
                    # Column was just added, set values
                    df.loc[mask, key] = value
            
            # Update updated_date if column exists
            if "updated_date" in df.columns:
                df.loc[mask, "updated_date"] = date.today()
            
            # Coerce date columns before saving
            df = coerce_date_columns(df, data_type)
            
            # Save with explicit schema
            write_parquet_with_schema(df, file_path, data_type)
            
            return [TextContent(
                type="text",
                text=json.dumps({
                    "success": True,
                    "audit_ids": audit_ids,
                    "snapshot_created": str(snapshot_path) if snapshot_path else None,
                    "records_updated": int(match_count),
                    "filters": filters,
                    "updates": updates,
                }, indent=2, default=str)
            )]
        
        except Exception as e:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": str(e),
                }, indent=2)
            )]
    
    elif name == "upsert_record":
        data_type = arguments["data_type"]
        filters = arguments["filters"]
        record = arguments["record"]
        
        file_path = get_parquet_file_path(data_type)
        
        if not file_path.exists():
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": f"Parquet file not found: {file_path}",
                }, indent=2)
            )]
        
        try:
            # Always create snapshot before modification (per policy)
            snapshot_path = create_snapshot(file_path)
            
            # Load existing data
            df = pd.read_parquet(file_path)
            
            # Apply enhanced filters to find matching records
            df_filtered = df.copy()
            for key, value in filters.items():
                df_filtered = apply_enhanced_filter(df_filtered, key, value)
            
            match_count = len(df_filtered)
            id_field = f"{data_type.rstrip('s')}_id"
            
            if match_count == 0:
                # No match found - create new record (use add_record logic)
                # Generate ID if needed
                if id_field not in record and id_field in df.columns:
                    record[id_field] = str(uuid.uuid4())[:16]
                
                # Set import_date if not provided
                if "import_date" not in record and "import_date" in df.columns:
                    record["import_date"] = date.today()
                elif "import_date" in record and isinstance(record["import_date"], str):
                    try:
                        record["import_date"] = date.fromisoformat(record["import_date"])
                    except ValueError:
                        pass
                
                # Set import_source_file if not provided
                if "import_source_file" not in record and "import_source_file" in df.columns:
                    record["import_source_file"] = "mcp_upsert_entry"
                
                # Set timestamp fields if needed
                schema = load_json_schema(data_type)
                if schema:
                    schema_def = schema.get("schema", {})
                    from datetime import timezone
                    now = datetime.now(timezone.utc)
                    for field, type_name in schema_def.items():
                        if type_name == "timestamp" and field in df.columns:
                            if field not in record or record[field] is None:
                                if field in ["created_at", "updated_at"]:
                                    record[field] = now
                
                # Coerce dates
                record = coerce_record_dates(data_type, record)
                
                # Add missing schema columns
                if schema:
                    schema_def = schema.get("schema", {})
                    for key in schema_def.keys():
                        if key not in record and key in df.columns:
                            record[key] = None
                
                # Set created_date/updated_date if columns exist
                if "created_date" in df.columns and "created_date" not in record:
                    record["created_date"] = date.today()
                if "updated_date" in df.columns and "updated_date" not in record:
                    record["updated_date"] = date.today()
                
                # Append new record
                df_new = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
                
                # Coerce date columns
                df_new = coerce_date_columns(df_new, data_type)
                
                # Save
                write_parquet_with_schema(df_new, file_path, data_type)
                
                # Create audit entry
                record_id = record.get(id_field, "unknown")
                audit_entry = create_audit_entry(
                    operation="add",
                    data_type=data_type,
                    record_id=str(record_id),
                    affected_fields=list(record.keys()),
                    new_values=record,
                    snapshot_reference=str(snapshot_path) if snapshot_path else None,
                    notes=f"Created record via upsert (no match found) with filters: {json.dumps(filters)}"
                )
                
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "success": True,
                        "action": "created",
                        "audit_id": audit_entry["audit_id"],
                        "record_id": str(record_id),
                        "snapshot_created": str(snapshot_path) if snapshot_path else None,
                        "filters": filters,
                        "record": record,
                    }, indent=2, default=str)
                )]
            
            else:
                # Match found - update existing record(s) (use update_records logic)
                # Merge record data with existing data (record takes precedence)
                updates = dict(record)
                
                # Coerce dates
                updates = coerce_record_dates(data_type, updates)
                
                # Get old values for audit log
                affected_records = df_filtered
                audit_ids = []
                
                # Update each matched record
                for idx, row in affected_records.iterrows():
                    record_id = row.get(id_field, f"idx_{idx}")
                    
                    # Get old values for fields being updated
                    old_values = {k: row[k] for k in updates.keys() if k in row.index}
                    
                    # Create audit entry
                    audit_entry = create_audit_entry(
                        operation="update",
                        data_type=data_type,
                        record_id=str(record_id),
                        affected_fields=list(updates.keys()),
                        old_values=old_values,
                        new_values=updates,
                        snapshot_reference=str(snapshot_path) if snapshot_path else None,
                        notes=f"Updated record via upsert (match found) with filters: {json.dumps(filters)}"
                    )
                    audit_ids.append(audit_entry["audit_id"])
                
                # Apply updates to matched records
                # Create mask for matched records using index intersection
                matched_indices = set(df_filtered.index)
                mask = df.index.isin(matched_indices)
                
                # Check schema for missing columns
                schema = load_json_schema(data_type)
                if schema:
                    schema_def = schema.get("schema", {})
                    for key in schema_def.keys():
                        if key not in df.columns:
                            df[key] = None
                
                # Apply updates
                for key, value in updates.items():
                    if key in df.columns:
                        df.loc[mask, key] = value
                
                # Update updated_date if column exists
                if "updated_date" in df.columns:
                    df.loc[mask, "updated_date"] = date.today()
                
                # Coerce date columns before saving
                df = coerce_date_columns(df, data_type)
                
                # Save
                write_parquet_with_schema(df, file_path, data_type)
                
                # Return first record ID for convenience
                first_record_id = affected_records.iloc[0].get(id_field, "unknown")
                
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "success": True,
                        "action": "updated",
                        "audit_ids": audit_ids,
                        "records_updated": int(match_count),
                        "record_id": str(first_record_id),
                        "snapshot_created": str(snapshot_path) if snapshot_path else None,
                        "filters": filters,
                        "updates": updates,
                    }, indent=2, default=str)
                )]
        
        except Exception as e:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": str(e),
                }, indent=2)
            )]
    
    elif name == "delete_records":
        data_type = arguments["data_type"]
        filters = arguments["filters"]
        
        file_path = get_parquet_file_path(data_type)
        
        if not file_path.exists():
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": f"Parquet file not found: {file_path}",
                }, indent=2)
            )]
        
        try:
            # Always create snapshot before modification (per policy)
            snapshot_path = create_snapshot(file_path)
            
            # Load existing data
            df = pd.read_parquet(file_path)
            
            # Create mask for matching records
            mask = pd.Series([True] * len(df))
            for key, value in filters.items():
                if key in df.columns:
                    if isinstance(value, list):
                        mask = mask & df[key].isin(value)
                    else:
                        mask = mask & (df[key] == value)
            
            # Count matches
            match_count = mask.sum()
            
            if match_count == 0:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "success": False,
                        "error": "No records matched the filters",
                        "filters": filters,
                    }, indent=2)
                )]
            
            # Get old values for audit log (for deleted records)
            id_field = f"{data_type.rstrip('s')}_id"
            deleted_records = df[mask]
            
            # Create audit entries for each deleted record
            audit_ids = []
            for idx, row in deleted_records.iterrows():
                record_id = row.get(id_field, f"idx_{idx}")
                old_values = row.to_dict()
                
                audit_entry = create_audit_entry(
                    operation="delete",
                    data_type=data_type,
                    record_id=str(record_id),
                    affected_fields=list(old_values.keys()),
                    old_values=old_values,
                    snapshot_reference=str(snapshot_path) if snapshot_path else None,
                    notes=f"Deleted record via MCP with filters: {json.dumps(filters)}"
                )
                audit_ids.append(audit_entry["audit_id"])
            
            # Delete records
            df_new = df[~mask]
            
            # Coerce date columns before saving
            df_new = coerce_date_columns(df_new, data_type)
            
            # Save parquet file
            try:
                write_parquet_with_schema(df_new, file_path, data_type)
            except Exception as write_error:
                # If write fails, provide detailed error
                error_msg = f"Failed to write parquet file: {write_error}"
                print(error_msg, file=sys.stderr)
                import traceback
                traceback.print_exc(file=sys.stderr)
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "error": error_msg,
                        "traceback": traceback.format_exc()
                    }, indent=2)
                )]
            
            return [TextContent(
                type="text",
                text=json.dumps({
                    "success": True,
                    "audit_ids": audit_ids,
                    "snapshot_created": str(snapshot_path) if snapshot_path else None,
                    "records_deleted": int(match_count),
                    "remaining_records": len(df_new),
                    "filters": filters,
                }, indent=2, default=str)
            )]
        
        except Exception as e:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": str(e),
                }, indent=2)
            )]
    
    elif name == "get_statistics":
        data_type = arguments["data_type"]
        file_path = get_parquet_file_path(data_type)
        filters = arguments.get("filters", {})
        columns = arguments.get("columns")
        include_distribution = arguments.get("include_distribution", False)
        
        if not file_path.exists():
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": f"Parquet file not found: {file_path}",
                }, indent=2)
            )]
        
        try:
            df = pd.read_parquet(file_path)
            
            # Apply filters if provided
            for key, value in filters.items():
                df = apply_enhanced_filter(df, key, value)
            
            stats = {
                "data_type": data_type,
                "file_path": str(file_path),
                "row_count": len(df),
                "column_count": len(df.columns),
                "columns": list(df.columns),
                "dtypes": {col: str(dtype) for col, dtype in df.dtypes.items()},
                "memory_usage_mb": df.memory_usage(deep=True).sum() / 1024 / 1024,
            }
            
            # Analyze numeric columns
            numeric_cols = df.select_dtypes(include=[np.number]).columns
            if columns:
                numeric_cols = [c for c in numeric_cols if c in columns]
            
            if len(numeric_cols) > 0:
                stats["numeric_statistics"] = {}
                for col in numeric_cols:
                    col_stats = {
                        "count": int(df[col].count()),
                        "sum": float(df[col].sum()) if df[col].dtype in [np.float64, np.float32, np.int64, np.int32] else None,
                        "mean": float(df[col].mean()) if df[col].dtype in [np.float64, np.float32] else None,
                        "min": float(df[col].min()) if df[col].dtype in [np.float64, np.float32] else None,
                        "max": float(df[col].max()) if df[col].dtype in [np.float64, np.float32] else None,
                        "std": float(df[col].std()) if df[col].dtype in [np.float64, np.float32] else None,
                    }
                    if include_distribution:
                        col_stats["percentiles"] = {
                            "25": float(df[col].quantile(0.25)),
                            "50": float(df[col].quantile(0.50)),
                            "75": float(df[col].quantile(0.75)),
                            "90": float(df[col].quantile(0.90)),
                            "95": float(df[col].quantile(0.95)),
                            "99": float(df[col].quantile(0.99)),
                        }
                    stats["numeric_statistics"][col] = col_stats
            
            # Analyze date columns
            date_columns = [col for col in df.columns if 'date' in col.lower() or 'time' in col.lower() or df[col].dtype.name.startswith('datetime')]
            if columns:
                date_columns = [c for c in date_columns if c in columns]
            
            if len(date_columns) > 0:
                stats["date_statistics"] = {}
                for col in date_columns:
                    try:
                        df_date = pd.to_datetime(df[col], errors='coerce')
                        if df_date.notna().any():
                            stats["date_statistics"][col] = {
                                "min": str(df_date.min()),
                                "max": str(df_date.max()),
                                "range_days": int((df_date.max() - df_date.min()).days) if df_date.notna().sum() > 1 else 0,
                                "unique_count": int(df_date.nunique()),
                                "null_count": int(df_date.isna().sum()),
                            }
                    except:
                        pass
            
            # Analyze categorical columns
            categorical_cols = df.select_dtypes(include=['object', 'category']).columns
            if columns:
                categorical_cols = [c for c in categorical_cols if c in columns]
            
            if len(categorical_cols) > 0:
                stats["categorical_statistics"] = {}
                for col in categorical_cols[:10]:  # Limit to first 10 to avoid huge output
                    value_counts = df[col].value_counts()
                    stats["categorical_statistics"][col] = {
                        "unique_count": int(value_counts.count()),
                        "null_count": int(df[col].isna().sum()),
                        "most_common": value_counts.head(10).to_dict(),
                    }
            
            return [TextContent(
                type="text",
                text=json.dumps(stats, indent=2, default=str)
            )]
        
        except Exception as e:
            import traceback
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                }, indent=2)
            )]
    
    elif name == "read_audit_log":
        filters = {}
        if "data_type" in arguments:
            filters["data_type"] = arguments["data_type"]
        if "operation" in arguments:
            filters["operation"] = arguments["operation"]
        if "record_id" in arguments:
            filters["record_id"] = arguments["record_id"]
        limit = arguments.get("limit", 100)
        
        if not AUDIT_LOG_PATH.exists():
            return [TextContent(
                type="text",
                text=json.dumps({
                    "count": 0,
                    "message": "No audit log exists yet",
                    "data": [],
                }, indent=2)
            )]
        
        try:
            df = pd.read_parquet(AUDIT_LOG_PATH)
            
            # Apply filters
            for key, value in filters.items():
                if key in df.columns:
                    if isinstance(value, list):
                        df = df[df[key].isin(value)]
                    else:
                        df = df[df[key] == value]
            
            # Sort by timestamp descending (most recent first)
            if "timestamp" in df.columns:
                df = df.sort_values("timestamp", ascending=False)
            
            # Apply limit
            df = df.head(limit)
            
            # Convert to JSON-serializable format
            result = df.to_dict(orient='records')
            
            # Convert date/datetime objects to strings
            for record in result:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif isinstance(value, (pd.Timestamp, datetime)):
                        record[key] = value.isoformat()
                    elif isinstance(value, date):
                        record[key] = value.isoformat()
            
            return [TextContent(
                type="text",
                text=json.dumps({
                    "count": len(result),
                    "filters": filters,
                    "data": result,
                }, indent=2, default=str)
            )]
        
        except Exception as e:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": str(e),
                }, indent=2)
            )]
    
    elif name == "rollback_operation":
        audit_id = arguments["audit_id"]
        
        if not AUDIT_LOG_PATH.exists():
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": "No audit log exists",
                }, indent=2)
            )]
        
        try:
            # Find the audit entry
            df_audit = pd.read_parquet(AUDIT_LOG_PATH)
            audit_entry = df_audit[df_audit["audit_id"] == audit_id]
            
            if len(audit_entry) == 0:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "error": f"Audit entry not found: {audit_id}",
                    }, indent=2)
                )]
            
            entry = audit_entry.iloc[0].to_dict()
            operation = entry["operation"]
            data_type = entry["data_type"]
            record_id = entry["record_id"]
            
            file_path = get_parquet_file_path(data_type)
            
            if not file_path.exists():
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "error": f"Parquet file not found: {file_path}",
                    }, indent=2)
                )]
            
            # Perform inverse operation
            if operation == "add":
                # Rollback add = delete the record
                id_field = f"{data_type.rstrip('s')}_id"
                df = pd.read_parquet(file_path)
                df_new = df[df[id_field] != record_id]
                
                # Save
                write_parquet_with_schema(df_new, file_path, data_type)
                
                # Create audit entry for rollback
                rollback_audit = create_audit_entry(
                    operation="delete",
                    data_type=data_type,
                    record_id=record_id,
                    notes=f"Rollback of add operation {audit_id}"
                )
                
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "success": True,
                        "rollback_audit_id": rollback_audit["audit_id"],
                        "action": "deleted record (rollback of add)",
                        "record_id": record_id,
                    }, indent=2)
                )]
            
            elif operation == "update":
                # Rollback update = restore old values
                old_values = json.loads(entry["old_values"]) if entry["old_values"] else {}
                
                if not old_values:
                    return [TextContent(
                        type="text",
                        text=json.dumps({
                            "error": "Cannot rollback: no old values stored",
                        }, indent=2)
                    )]
                
                id_field = f"{data_type.rstrip('s')}_id"
                df = pd.read_parquet(file_path)
                mask = df[id_field] == record_id
                
                # Apply old values
                old_values = coerce_record_dates(data_type, old_values)
                for key, value in old_values.items():
                    if key in df.columns:
                        df.loc[mask, key] = value
                
                # Save
                df = coerce_date_columns(df, data_type)
                write_parquet_with_schema(df, file_path, data_type)
                
                # Create audit entry for rollback
                new_values = json.loads(entry["new_values"]) if entry["new_values"] else {}
                rollback_audit = create_audit_entry(
                    operation="update",
                    data_type=data_type,
                    record_id=record_id,
                    affected_fields=list(old_values.keys()),
                    old_values=new_values,
                    new_values=old_values,
                    notes=f"Rollback of update operation {audit_id}"
                )
                
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "success": True,
                        "rollback_audit_id": rollback_audit["audit_id"],
                        "action": "restored old values (rollback of update)",
                        "record_id": record_id,
                        "restored_values": old_values,
                    }, indent=2, default=str)
                )]
            
            elif operation == "delete":
                # Rollback delete = restore the record
                old_values = json.loads(entry["old_values"]) if entry["old_values"] else {}
                
                if not old_values:
                    return [TextContent(
                        type="text",
                        text=json.dumps({
                            "error": "Cannot rollback: no old values stored",
                        }, indent=2)
                    )]
                
                df = pd.read_parquet(file_path)
                
                # Normalize date fields
                old_values = coerce_record_dates(data_type, old_values)
                
                # Add record back
                new_row = pd.DataFrame([old_values])
                for col in df.columns:
                    if col not in new_row.columns:
                        new_row[col] = None
                
                df_new = pd.concat([df, new_row], ignore_index=True)
                df_new = coerce_date_columns(df_new, data_type)
                
                # Save
                write_parquet_with_schema(df_new, file_path, data_type)
                
                # Create audit entry for rollback
                rollback_audit = create_audit_entry(
                    operation="add",
                    data_type=data_type,
                    record_id=record_id,
                    affected_fields=list(old_values.keys()),
                    new_values=old_values,
                    notes=f"Rollback of delete operation {audit_id}"
                )
                
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "success": True,
                        "rollback_audit_id": rollback_audit["audit_id"],
                        "action": "restored record (rollback of delete)",
                        "record_id": record_id,
                        "restored_record": old_values,
                    }, indent=2, default=str)
                )]
            
            else:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "error": f"Unknown operation type: {operation}",
                    }, indent=2)
                )]
        
        except Exception as e:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": str(e),
                }, indent=2)
            )]
    
    elif name == "search_parquet":
        data_type = arguments["data_type"]
        query = arguments["query"]
        text_fields = arguments.get("text_fields")
        limit = arguments.get("limit", 10)
        min_similarity = arguments.get("min_similarity", 0.7)
        additional_filters = arguments.get("additional_filters", {})
        
        file_path = get_parquet_file_path(data_type)
        embeddings_path = get_embeddings_file_path(data_type)
        
        if not file_path.exists():
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": f"Parquet file not found: {file_path}",
                }, indent=2)
            )]
        
        if not embeddings_path.exists():
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": f"Embeddings not found for {data_type}. Run generate_embeddings first.",
                }, indent=2)
            )]
        
        try:
            # Load data and embeddings
            df = pd.read_parquet(file_path)
            df_embeddings = pd.read_parquet(embeddings_path)
            
            # Get ID field
            id_field = f"{data_type.rstrip('s')}_id"
            if id_field not in df.columns:
                id_field = df.index.name or "index"
            
            # Apply additional filters first
            for key, value in additional_filters.items():
                df = apply_enhanced_filter(df, key, value)
            
            # Generate query embedding
            client = get_openai_client()
            if not client:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "error": "OpenAI API not available. Set OPENAI_API_KEY environment variable.",
                    }, indent=2)
                )]
            
            query_response = client.embeddings.create(
                model="text-embedding-3-small",
                input=query
            )
            query_embedding = np.array(query_response.data[0].embedding)
            
            # Calculate similarities
            similarities = []
            for idx, row in df.iterrows():
                record_id = row.get(id_field, idx)
                
                # Find matching embedding row
                emb_row = df_embeddings[df_embeddings[id_field] == record_id]
                if emb_row.empty:
                    continue
                
                # Get combined embedding (average of all text field embeddings)
                emb_values = []
                for col in df_embeddings.columns:
                    if col.endswith("_embedding") and col in emb_row.columns:
                        emb_val = emb_row[col].iloc[0]
                        if emb_val is not None and not pd.isna(emb_val):
                            # Handle both list and array formats
                            if isinstance(emb_val, (list, np.ndarray)):
                                emb_values.append(np.array(emb_val))
                            elif isinstance(emb_val, str):
                                # Try to parse JSON string
                                try:
                                    emb_values.append(np.array(json.loads(emb_val)))
                                except:
                                    continue
                
                if not emb_values:
                    continue
                
                # Average embeddings
                combined_embedding = np.mean(emb_values, axis=0)
                
                # Calculate cosine similarity
                similarity = np.dot(query_embedding, combined_embedding) / (
                    np.linalg.norm(query_embedding) * np.linalg.norm(combined_embedding)
                )
                
                if similarity >= min_similarity:
                    similarities.append((idx, similarity, row))
            
            # Sort by similarity descending
            similarities.sort(key=lambda x: x[1], reverse=True)
            
            # Take top results
            results = []
            for idx, similarity, row in similarities[:limit]:
                record = row.to_dict()
                record["_similarity"] = float(similarity)
                results.append(record)
            
            # Convert to JSON-serializable format
            for record in results:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif isinstance(value, (pd.Timestamp, datetime)):
                        record[key] = value.isoformat()
                    elif isinstance(value, date):
                        record[key] = value.isoformat()
                    elif isinstance(value, np.floating):
                        record[key] = float(value)
                    elif isinstance(value, np.integer):
                        record[key] = int(value)
            
            return [TextContent(
                type="text",
                text=json.dumps({
                    "count": len(results),
                    "query": query,
                    "min_similarity": min_similarity,
                    "data": results,
                }, indent=2, default=str)
            )]
        
        except Exception as e:
            import traceback
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                }, indent=2)
            )]
    
    elif name == "generate_embeddings":
        data_type = arguments["data_type"]
        text_fields = arguments.get("text_fields")
        force_regenerate = arguments.get("force_regenerate", False)
        
        file_path = get_parquet_file_path(data_type)
        embeddings_path = get_embeddings_file_path(data_type)
        
        if not file_path.exists():
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": f"Parquet file not found: {file_path}",
                }, indent=2)
            )]
        
        client = get_openai_client()
        if not client:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": "OpenAI API not available. Set OPENAI_API_KEY environment variable.",
                }, indent=2)
            )]
        
        try:
            df = pd.read_parquet(file_path)
            
            # Auto-detect text fields if not provided
            if not text_fields:
                text_fields = []
                for col in df.columns:
                    col_lower = col.lower()
                    if col_lower in ["title", "description", "name", "notes", "summary", "content", "text"]:
                        text_fields.append(col)
                    elif df[col].dtype == "object" and col not in ["id", f"{data_type.rstrip('s')}_id"]:
                        # Check if column contains mostly strings
                        sample = df[col].dropna().head(100)
                        if len(sample) > 0 and all(isinstance(x, str) for x in sample):
                            text_fields.append(col)
            
            if not text_fields:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "error": "No text fields found. Specify text_fields parameter.",
                    }, indent=2)
                )]
            
            # Get ID field
            id_field = f"{data_type.rstrip('s')}_id"
            if id_field not in df.columns:
                id_field = df.index.name or "index"
            
            # Load existing embeddings if they exist
            existing_embeddings = {}
            if embeddings_path.exists() and not force_regenerate:
                df_existing = pd.read_parquet(embeddings_path)
                for _, row in df_existing.iterrows():
                    record_id = row[id_field]
                    existing_embeddings[record_id] = {}
                    for col in df_existing.columns:
                        if col.endswith("_embedding") and not pd.isna(row[col]):
                            existing_embeddings[record_id][col] = row[col]
            
            # Generate embeddings
            embeddings_data = []
            total_records = len(df)
            
            for idx, row in df.iterrows():
                record_id = row.get(id_field, idx)
                record_embeddings = {id_field: record_id}
                
                # Copy other fields from original record
                for col in df.columns:
                    if col not in text_fields and col != id_field:
                        record_embeddings[col] = row[col]
                
                # Generate embeddings for each text field
                for field in text_fields:
                    if field not in df.columns:
                        continue
                    
                    text_value = row[field]
                    if pd.isna(text_value) or not isinstance(text_value, str) or not text_value.strip():
                        record_embeddings[f"{field}_embedding"] = None
                        continue
                    
                    # Check if we already have this embedding
                    emb_key = f"{field}_embedding"
                    if record_id in existing_embeddings and emb_key in existing_embeddings[record_id]:
                        # Preserve existing embedding format
                        existing_emb = existing_embeddings[record_id][emb_key]
                        if isinstance(existing_emb, (list, np.ndarray)):
                            record_embeddings[emb_key] = list(existing_emb) if isinstance(existing_emb, np.ndarray) else existing_emb
                        else:
                            record_embeddings[emb_key] = existing_emb
                        continue
                    
                    # Generate new embedding
                    try:
                        response = client.embeddings.create(
                            model="text-embedding-3-small",
                            input=text_value
                        )
                        embedding = response.data[0].embedding
                        # Store as list for parquet compatibility
                        record_embeddings[emb_key] = list(embedding)
                    except Exception as e:
                        record_embeddings[emb_key] = None
                        print(f"Warning: Failed to generate embedding for {field} in record {record_id}: {e}", file=sys.stderr)
                
                embeddings_data.append(record_embeddings)
            
            # Create embeddings dataframe
            df_embeddings = pd.DataFrame(embeddings_data)
            
            # Save embeddings
            df_embeddings.to_parquet(embeddings_path, index=False)
            
            # Count generated vs reused
            generated_count = 0
            reused_count = 0
            for record in embeddings_data:
                record_id_val = record.get(id_field)
                for key, value in record.items():
                    if key.endswith("_embedding") and value is not None:
                        if record_id_val and record_id_val in existing_embeddings and key in existing_embeddings[record_id_val]:
                            reused_count += 1
                        else:
                            generated_count += 1
            
            return [TextContent(
                type="text",
                text=json.dumps({
                    "success": True,
                    "data_type": data_type,
                    "text_fields": text_fields,
                    "total_records": total_records,
                    "embeddings_generated": generated_count,
                    "embeddings_reused": reused_count,
                    "embeddings_file": str(embeddings_path),
                }, indent=2, default=str)
            )]
        
        except Exception as e:
            import traceback
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                }, indent=2)
            )]
    
    elif name == "aggregate_parquet":
        data_type = arguments["data_type"]
        aggregations = arguments["aggregations"]
        group_by = arguments.get("group_by", [])
        filters = arguments.get("filters", {})
        sort_by = arguments.get("sort_by")
        limit = arguments.get("limit", 1000)
        
        file_path = get_parquet_file_path(data_type)
        
        if not file_path.exists():
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": f"Parquet file not found: {file_path}",
                }, indent=2)
            )]
        
        try:
            df = pd.read_parquet(file_path)
            
            # Apply filters
            for key, value in filters.items():
                df = apply_enhanced_filter(df, key, value)
            
            # Validate group_by columns
            if group_by:
                missing_cols = [c for c in group_by if c not in df.columns]
                if missing_cols:
                    return [TextContent(
                        type="text",
                        text=json.dumps({
                            "error": f"Group by columns not found: {missing_cols}",
                            "available_columns": list(df.columns),
                        }, indent=2)
                    )]
            
            # Validate aggregation columns
            agg_cols = list(aggregations.keys())
            missing_agg_cols = [c for c in agg_cols if c not in df.columns]
            if missing_agg_cols:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "error": f"Aggregation columns not found: {missing_agg_cols}",
                        "available_columns": list(df.columns),
                    }, indent=2)
                )]
            
            # Build aggregation dict
            agg_dict = {}
            for col, funcs in aggregations.items():
                if isinstance(funcs, str):
                    funcs = [funcs]
                elif not isinstance(funcs, list):
                    funcs = [funcs]
                
                for func in funcs:
                    if func in ['sum', 'count', 'mean', 'avg', 'min', 'max', 'std', 'first', 'last']:
                        if func == 'avg':
                            func = 'mean'
                        agg_dict[col] = agg_dict.get(col, []) + [func]
            
            # Perform aggregation
            if group_by:
                grouped = df.groupby(group_by)
                result_df = grouped.agg(agg_dict)
                # Flatten MultiIndex column names
                if isinstance(result_df.columns, pd.MultiIndex):
                    result_df.columns = [f"{col}_{func}" if col != func else col for col, func in result_df.columns]
                result_df = result_df.reset_index()
            else:
                # Aggregate entire dataframe
                result_df = df.agg(agg_dict)
                # Handle both Series and DataFrame results
                if isinstance(result_df, pd.Series):
                    result_df = result_df.to_frame().T
                else:
                    # Already a DataFrame - reshape to single row with flattened columns
                    # result_df structure: index=original columns, columns=aggregation functions
                    # Convert to Series with MultiIndex, then to DataFrame row
                    result_series = result_df.stack()
                    # Flatten MultiIndex to column names like 'col1_sum', 'col1_count'
                    result_series.index = [f"{col}_{func}" for col, func in result_series.index]
                    result_df = result_series.to_frame().T
                # Flatten column names - build from agg_dict structure
                flat_cols = []
                for col, funcs in agg_dict.items():
                    for func in funcs:
                        flat_cols.append(f"{col}_{func}")
                # Only set columns if they match expected count
                if len(result_df.columns) == len(flat_cols):
                    result_df.columns = flat_cols
            
            # Apply sorting
            if sort_by:
                result_df = apply_sorting(result_df, sort_by, data_type)
            
            # Apply limit
            result_df = result_df.head(limit)
            
            # Convert to JSON
            result = result_df.to_dict(orient='records')
            
            # Convert date/datetime objects to strings
            for record in result:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif isinstance(value, (pd.Timestamp, datetime)):
                        record[key] = value.isoformat()
                    elif isinstance(value, date):
                        record[key] = value.isoformat()
                    elif isinstance(value, (np.integer, np.floating)):
                        record[key] = float(value) if isinstance(value, np.floating) else int(value)
            
            return [TextContent(
                type="text",
                text=json.dumps({
                    "count": len(result),
                    "total_groups": len(result_df) if limit >= len(result_df) else f"{len(result_df)}+",
                    "data": result,
                }, indent=2, default=str)
            )]
        
        except Exception as e:
            import traceback
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                }, indent=2)
            )]
    
    elif name == "query_with_date_range":
        data_type = arguments["data_type"]
        date_column = arguments["date_column"]
        date_start = arguments.get("date_start")
        date_end = arguments.get("date_end")
        filters = arguments.get("filters", {})
        group_by_period = arguments.get("group_by_period")
        aggregations = arguments.get("aggregations")
        sort_by = arguments.get("sort_by")
        limit = arguments.get("limit", 1000)
        
        file_path = get_parquet_file_path(data_type)
        
        if not file_path.exists():
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": f"Parquet file not found: {file_path}",
                }, indent=2)
            )]
        
        try:
            df = pd.read_parquet(file_path)
            
            # Validate date column
            if date_column not in df.columns:
                return [TextContent(
                    type="text",
                    text=json.dumps({
                        "error": f"Date column not found: {date_column}",
                        "available_columns": list(df.columns),
                    }, indent=2)
                )]
            
            # Convert date column to datetime
            df[date_column] = pd.to_datetime(df[date_column], errors='coerce')
            
            # Apply date range filters
            if date_start:
                date_start_dt = pd.to_datetime(date_start)
                df = df[df[date_column] >= date_start_dt]
            
            if date_end:
                date_end_dt = pd.to_datetime(date_end)
                df = df[df[date_column] <= date_end_dt]
            
            # Apply additional filters
            for key, value in filters.items():
                df = apply_enhanced_filter(df, key, value)
            
            # Group by period if requested
            if group_by_period:
                if group_by_period == "day":
                    df['_period'] = df[date_column].dt.date
                elif group_by_period == "week":
                    df['_period'] = df[date_column].dt.to_period('W')
                elif group_by_period == "month":
                    df['_period'] = df[date_column].dt.to_period('M')
                elif group_by_period == "quarter":
                    df['_period'] = df[date_column].dt.to_period('Q')
                elif group_by_period == "year":
                    df['_period'] = df[date_column].dt.to_period('Y')
                
                if aggregations:
                    # Build aggregation dict
                    agg_dict = {}
                    for col, funcs in aggregations.items():
                        if isinstance(funcs, str):
                            funcs = [funcs]
                        elif not isinstance(funcs, list):
                            funcs = [funcs]
                        
                        for func in funcs:
                            if func in ['sum', 'count', 'mean', 'avg', 'min', 'max', 'std']:
                                if func == 'avg':
                                    func = 'mean'
                                agg_dict[col] = agg_dict.get(col, []) + [func]
                    
                    grouped = df.groupby('_period')
                    result_df = grouped.agg(agg_dict)
                    # Flatten MultiIndex column names
                    if isinstance(result_df.columns, pd.MultiIndex):
                        result_df.columns = [f"{col}_{func}" if col != func else col for col, func in result_df.columns]
                    result_df = result_df.reset_index()
                    result_df['period'] = result_df['_period'].astype(str)
                    result_df = result_df.drop(columns=['_period'])
                else:
                    # Just group by period, return count
                    result_df = df.groupby('_period').size().reset_index(name='count')
                    result_df['period'] = result_df['_period'].astype(str)
                    result_df = result_df.drop(columns=['_period'])
            else:
                result_df = df.copy()
            
            # Apply sorting
            if sort_by:
                result_df = apply_sorting(result_df, sort_by, data_type)
            
            # Apply limit
            result_df = result_df.head(limit)
            
            # Convert to JSON
            result = result_df.to_dict(orient='records')
            
            # Convert date/datetime objects to strings
            for record in result:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif isinstance(value, (pd.Timestamp, datetime)):
                        record[key] = value.isoformat()
                    elif isinstance(value, date):
                        record[key] = value.isoformat()
                    elif isinstance(value, (np.integer, np.floating)):
                        record[key] = float(value) if isinstance(value, np.floating) else int(value)
            
            return [TextContent(
                type="text",
                text=json.dumps({
                    "count": len(result),
                    "total_rows": len(result_df) if limit >= len(result_df) else f"{len(result_df)}+",
                    "data": result,
                }, indent=2, default=str)
            )]
        
        except Exception as e:
            import traceback
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                }, indent=2)
            )]
    
    elif name == "sort_parquet":
        data_type = arguments["data_type"]
        sort_by = arguments["sort_by"]
        filters = arguments.get("filters", {})
        limit = arguments.get("limit", 1000)
        columns = arguments.get("columns")
        
        file_path = get_parquet_file_path(data_type)
        
        if not file_path.exists():
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": f"Parquet file not found: {file_path}",
                }, indent=2)
            )]
        
        try:
            df = pd.read_parquet(file_path)
            
            # Apply filters
            for key, value in filters.items():
                df = apply_enhanced_filter(df, key, value)
            
            # Apply sorting
            df = apply_sorting(df, sort_by, data_type)
            
            # Select columns if specified
            if columns:
                missing_cols = [c for c in columns if c not in df.columns]
                if missing_cols:
                    return [TextContent(
                        type="text",
                        text=json.dumps({
                            "error": f"Columns not found: {missing_cols}",
                            "available_columns": list(df.columns),
                        }, indent=2)
                    )]
                df = df[columns]
            
            # Apply limit
            df = df.head(limit)
            
            # Convert to JSON
            result = df.to_dict(orient='records')
            
            # Convert date/datetime objects to strings
            for record in result:
                for key, value in record.items():
                    if pd.isna(value):
                        record[key] = None
                    elif isinstance(value, (pd.Timestamp, datetime)):
                        record[key] = value.isoformat()
                    elif isinstance(value, date):
                        record[key] = value.isoformat()
            
            return [TextContent(
                type="text",
                text=json.dumps({
                    "count": len(result),
                    "total_rows": len(df) if limit >= len(df) else f"{len(df)}+",
                    "data": result,
                }, indent=2, default=str)
            )]
        
        except Exception as e:
            import traceback
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": str(e),
                    "traceback": traceback.format_exc(),
                }, indent=2)
            )]
    
    elif name == "validate_schema":
        data_type = arguments["data_type"]
        file_path = get_parquet_file_path(data_type)

        if not file_path.exists():
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": f"Parquet file not found: {file_path}",
                }, indent=2)
            )]

        schema = load_json_schema(data_type)
        if not schema:
            return [TextContent(
                type="text",
                text=json.dumps({
                    "error": f"JSON schema not found for data type: {data_type}",
                }, indent=2)
            )]

        df = pd.read_parquet(file_path)
        declared = schema.get("schema", {})

        issues: Dict[str, Any] = {
            "missing_columns": [],
            "extra_columns": [],
            "date_parse_errors": {},
        }

        for col in declared.keys():
            if col not in df.columns:
                issues["missing_columns"].append(col)
        for col in df.columns:
            if col not in declared.keys():
                issues["extra_columns"].append(col)

        for col, type_name in declared.items():
            if type_name in {"date", "datetime"} and col in df.columns:
                try:
                    _ = pd.to_datetime(df[col], errors="raise")
                except Exception as e:
                    issues["date_parse_errors"][col] = str(e)

        return [TextContent(
            type="text",
            text=json.dumps({
                "data_type": data_type,
                "file_path": str(file_path),
                "issues": issues,
            }, indent=2, default=str)
        )]
    
    else:
        return [TextContent(
            type="text",
            text=json.dumps({
                "error": f"Unknown tool: {name}",
            }, indent=2)
        )]


async def main():
    """Run the MCP server."""
    async with stdio_server() as (read_stream, write_stream):
        await app.run(
            read_stream,
            write_stream,
            app.create_initialization_options()
        )


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())

