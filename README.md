# Parquet MCP Server

MCP server for interacting with parquet files in a repository. Provides comprehensive data management with audit logging, rollback capabilities, and semantic search.

## Credits

This is a custom MCP server implementation for parquet file management with audit trail support.

## Features

- **Read/Query**: Query parquet files with filters, column selection, and limits
- **Add Records**: Add new records to parquet files with audit trail
- **Update Records**: Update existing records matching filters with audit trail
- **Upsert Records**: Insert or update records (supports enhanced filters for duplicate detection)
- **Delete Records**: Delete records matching filters with audit trail
- **Audit Log**: Complete change history with old/new values for all modifications
- **Rollback**: Undo specific operations using audit IDs
- **Schema Discovery**: Get schema definitions for data types
- **Statistics**: Get basic statistics about parquet files
- **Efficient Backups**: Audit log entries (~1 KB) instead of full snapshots (99%+ storage reduction)
- **Optional Full Snapshots**: Configurable periodic snapshots for additional safety

## Installation

```bash
cd truth/mcp-servers/parquet
pip install -r requirements.txt
```

## Configuration

### Data Directory

The server uses the following priority to locate the data directory:

1. **Environment Variable** (highest priority):
   ```bash
   export DATA_DIR="/path/to/your/data/directory"
   ```

2. **Auto-detection** (for backward compatibility):
   - Automatically detects parent repository structure
   - Looks for `$DATA_DIR/` directory in common locations

3. **Default** (if no structure found):
   - Uses `~/.config/parquet-mcp/data/` as fallback

**Note:** The MCP server is portable and can be used with any data directory structure.

### Cursor Configuration

Add to your Cursor MCP settings (typically `~/.cursor/mcp.json` or Cursor settings):

**Development (Audit Log Only):**
```json
{
  "mcpServers": {
    "parquet": {
      "command": "python3",
      "args": [
        "/path/to/parquet_mcp_server.py"
      ],
      "env": {
        "DATA_DIR": "/path/to/your/data/directory"
      }
    }
  }
}
```

**Production (With Periodic Snapshots):**
```json
{
  "mcpServers": {
    "parquet": {
      "command": "python3",
      "args": [
        "/path/to/parquet_mcp_server.py"
      ],
      "env": {
        "DATA_DIR": "/path/to/your/data/directory",
        "MCP_FULL_SNAPSHOTS": "true",
        "MCP_SNAPSHOT_FREQUENCY": "weekly"
      }
    }
  }
}
```

**Note:** Replace `/path/to/parquet_mcp_server.py` with the actual path to the server file.

### Claude Desktop Configuration

Add to `claude_desktop_config.json` (typically `~/Library/Application Support/Claude/claude_desktop_config.json` on macOS):

```json
{
  "mcpServers": {
    "parquet": {
      "command": "python3",
      "args": [
        "/path/to/parquet_mcp_server.py"
      ],
      "env": {
        "DATA_DIR": "/path/to/your/data/directory"
      }
    }
  }
}
```

**Note:** Replace `/path/to/parquet_mcp_server.py` with the actual path to the server file.

## Available Tools

### Tool Selection Guide

**For counting records:** Use `get_statistics` - returns `row_count` efficiently without loading data.

**For retrieving data:** Use `read_parquet` - returns actual records with filtering, sorting, and column selection.
- **Do not call `get_schema`** unless you need schema information (field names, types, constraints) to complete the task. Data queries via `read_parquet` return all necessary field information.

**For metadata:** Use `get_schema` only when you need to understand the schema structure (required fields, valid values, data types) to build queries, filters, or add/update records. Do not call it for routine data retrieval tasks.

**For modifications:** Use `add_record`, `update_records`, `upsert_record`, or `delete_records` with audit logging.

---

## Proactive Memory Storage

**Source of Truth:** Instructions for proactive memory storage are embedded in the MCP server:
- **Tool descriptions** - `add_record`, `update_records`, and `upsert_record` descriptions include requirements
- **Prompt** - `data_persistence_guidance` prompt (via `list_prompts`/`get_prompt`) provides detailed guidance

Agents see these instructions when listing/using tools. This README is for human reference only.

---

### `list_data_types`
List all available data types (parquet files) in the data directory.

### `get_schema`
Get the schema definition for a data type.

**Parameters:**
- `data_type` (required): The data type name (e.g., 'flows', 'transactions', 'tasks')

### `read_parquet`
Read and query a parquet file with optional filters. Supports enhanced filtering operators.

**Use when:** You need to retrieve actual data records (rows) from the file.

**Do not use for:** Counting records - use `get_statistics` instead, which returns row count efficiently.

**Parameters:**
- `data_type` (required): The data type name
- `filters` (optional): Key-value pairs to filter records. Supports enhanced operators:
  - Simple value: exact match
  - List: in list (`["value1", "value2"]`)
  - `{"$contains": "text"}`: substring match (case-insensitive)
  - `{"$starts_with": "text"}`: prefix match (case-insensitive)
  - `{"$ends_with": "text"}`: suffix match (case-insensitive)
  - `{"$regex": "pattern"}`: regex pattern match
  - `{"$fuzzy": {"text": "query", "threshold": 0.7}}`: fuzzy string matching (0-1 similarity)
  - `{"$gt": 100}`, `{"$gte": 100}`, `{"$lt": 100}`, `{"$lte": 100}`: numeric comparisons
  - `{"$ne": "value"}`: not equal
- `limit` (optional): Maximum number of rows to return (default: 1000)
- `columns` (optional): List of column names to return (default: all columns)
- `sort_by` (optional): List of sort specifications, each with:
  - `column` (required): Column name to sort by
  - `ascending` (optional): Sort direction, `true` for ascending (default), `false` for descending
  - `na_position` (optional): Where to place null/NaN values: `"last"` (default) or `"first"`
  - `custom_order` (optional): Custom order for enum values (e.g., `["critical", "high", "medium", "low"]`)

**Examples:**
```json
{
  "data_type": "flows",
  "filters": {
    "category": "property_maintenance",
    "year": 2025
  },
  "limit": 100
}
```

```json
{
  "data_type": "tasks",
  "filters": {
    "title": {"$contains": "therapy"},
    "status": {"$ne": "completed"}
  }
}
```

```json
{
  "data_type": "tasks",
  "filters": {
    "title": {"$fuzzy": {"text": "therapy session", "threshold": 0.7}}
  }
}
```

**Example with sorting:**
```json
{
  "data_type": "tasks",
  "filters": {
    "sync_log": "exported"
  },
  "sort_by": [
    {
      "column": "sync_datetime",
      "ascending": false,
      "na_position": "last"
    }
  ],
  "limit": 10
}
```

### `add_record`
Add a new record to a parquet file. Creates audit log entry and optional snapshot.

**Parameters:**
- `data_type` (required): The data type name
- `record` (required): The record data as a JSON object matching the schema

**Example:**
```json
{
  "data_type": "flows",
  "record": {
    "flow_name": "Monthly Rent",
    "flow_date": "2025-01-15",
    "amount_usd": 1500.00,
    "category": "housing",
    "flow_type": "recurring_expense"
  }
}
```

### `update_records`
Update existing records in a parquet file. Creates audit log entry and optional snapshot.

**Parameters:**
- `data_type` (required): The data type name
- `filters` (required): Filters to identify records to update
- `updates` (required): Fields to update

**Example:**
```json
{
  "data_type": "tasks",
  "filters": {
    "task_id": "abc123"
  },
  "updates": {
    "status": "completed",
    "completed_date": "2025-01-15"
  }
}
```

### `upsert_record`
Insert or update a record (upsert). Checks for existing records using enhanced filters (supports all `read_parquet` filter operators including `$contains`, `$fuzzy`, etc.). If found, updates matching records. If not found, creates a new record. Returns whether it created or updated. Useful for preventing duplicates when adding contacts, tasks, or other records.

**Parameters:**
- `data_type` (required): The data type name
- `filters` (required): Enhanced filters to identify existing records (supports all `read_parquet` filter operators)
- `record` (required): The record data to insert or update

**Returns:**
- `action`: "created" or "updated"
- `audit_id` or `audit_ids`: Audit log entry ID(s)
- `record_id`: The ID of the created/updated record

**Example (exact match):**
```json
{
  "data_type": "contacts",
  "filters": {
    "email": "galina@secod.com"
  },
  "record": {
    "name": "Galina Semakova",
    "email": "galina@secod.com",
    "category": "legal",
    "last_contact_date": "2025-12-24"
  }
}
```

**Example (fuzzy match):**
```json
{
  "data_type": "contacts",
  "filters": {
    "name": {"$fuzzy": {"text": "Galina Semakova", "threshold": 0.8}}
  },
  "record": {
    "name": "Galina Semakova",
    "email": "galina@secod.com",
    "category": "legal",
    "last_contact_date": "2025-12-24"
  }
}
```

**Example (contains match):**
```json
{
  "data_type": "tasks",
  "filters": {
    "title": {"$contains": "therapy payment"}
  },
  "record": {
    "title": "Pay for therapy session",
    "status": "pending",
    "due_date": "2025-12-25"
  }
}
```

### `delete_records`
Delete records from a parquet file. Creates audit log entry and optional snapshot.

**Parameters:**
- `data_type` (required): The data type name
- `filters` (required): Filters to identify records to delete

**Example:**
```json
{
  "data_type": "tasks",
  "filters": {
    "status": "canceled"
  }
}
```

### `get_statistics`
Get comprehensive statistics about a parquet file including row count, column information, date ranges, and categorical distributions.

**Use when:** You need to know how many records exist, get file metadata, or analyze data distributions. This is the efficient way to answer "how many" questions.

**Returns:**
- `row_count`: Total number of records (use this for count queries)
- `column_count`: Number of columns
- `date_statistics`: Min/max dates and ranges for date columns
- `categorical_statistics`: Value distributions for categorical columns
- Column types and memory usage

**Parameters:**
- `data_type` (required): The data type name

### `read_audit_log`
Read audit log entries with optional filters. View complete history of all data modifications.

**Parameters:**
- `data_type` (optional): Filter by data type
- `operation` (optional): Filter by operation (add, update, delete)
- `record_id` (optional): Filter by specific record ID
- `limit` (optional): Maximum number of entries to return (default: 100)

**Example:**
```json
{
  "data_type": "transactions",
  "operation": "update",
  "limit": 50
}
```

### `rollback_operation`
Rollback a specific operation using its audit ID. Creates inverse operation to undo changes.

**Parameters:**
- `audit_id` (required): The audit ID of the operation to rollback

**Rollback Logic:**
- `add` operation → Delete the record
- `update` operation → Restore old values
- `delete` operation → Restore the record

**Example:**
```json
{
  "audit_id": "abc123def456"
}
```

### `search_parquet`
Semantic search using embeddings. Searches text fields for semantically similar records.

**Parameters:**
- `data_type` (required): The data type name
- `query` (required): Search query text
- `text_fields` (optional): List of text fields to search (default: auto-detect)
- `limit` (optional): Maximum number of results (default: 10)
- `min_similarity` (optional): Minimum cosine similarity threshold 0-1 (default: 0.7)
- `additional_filters` (optional): Additional filters to apply (same format as read_parquet)

**Prerequisites:**
- Must run `generate_embeddings` first to create embeddings for the data type
- Requires `OPENAI_API_KEY` environment variable

**Example:**
```json
{
  "data_type": "tasks",
  "query": "pay for therapy session",
  "limit": 5,
  "min_similarity": 0.7
}
```

### `generate_embeddings`
Generate and store embeddings for text fields in a data type. Creates embeddings parquet file for semantic search.

**Parameters:**
- `data_type` (required): The data type name
- `text_fields` (optional): List of text fields to generate embeddings for (default: auto-detect)
- `force_regenerate` (optional): Force regeneration of all embeddings (default: false)

**Prerequisites:**
- Requires `OPENAI_API_KEY` environment variable

**Example:**
```json
{
  "data_type": "tasks",
  "text_fields": ["title", "description", "notes"]
}
```

**Note:** Embeddings are cached. Only missing embeddings are generated unless `force_regenerate` is true.

## Backup & Recovery

### Audit Log (Default)

All write operations create lightweight audit log entries in `data/logs/audit_log.parquet`:
- **Storage**: ~1 KB per operation (99%+ reduction vs full snapshots)
- **Content**: Operation type, record ID, affected fields, old/new values, timestamp
- **Recovery**: Rollback specific operations using `rollback_operation` tool

### Automatic Snapshots

**All write operations automatically create timestamped snapshots before modification** (per policy requirement). This ensures rollback capability for every data change.

**Snapshot Location:**
```
data/snapshots/[data_type]-[YYYY-MM-DD-HHMMSS].parquet
```

**Note:** The `MCP_FULL_SNAPSHOTS` and `MCP_SNAPSHOT_FREQUENCY` environment variables are no longer used - snapshots are always created for all write operations.

### Storage Comparison

| Approach | Storage per Operation | 100 Operations |
|----------|----------------------|----------------|
| Full snapshots (old) | 10 MB | 1 GB |
| Audit log (new) | ~1 KB | ~100 KB |
| **Savings** | **99.99%** | **99.99%** |

### Recovery Options

1. **Recent Changes**: Use `rollback_operation` with audit ID
2. **Multiple Changes**: Rollback operations in reverse chronological order
3. **Full Restore**: Restore from periodic snapshot (if enabled)
4. **Point-in-Time**: Restore snapshot + replay audit log to specific timestamp

See [AUDIT_LOG_GUIDE.md](AUDIT_LOG_GUIDE.md) for detailed documentation.

## Data Types

The server automatically discovers data types by scanning `data/` for directories containing `[type].parquet` files. Common data types include:

- `flows` - Cash flow and expense data
- `transactions` - Transaction data
- `tasks` - Task management data
- `contacts` - Contact/merchant information
- `income` - Income data
- `fixed_costs` - Fixed cost data
- And many more...

## Error Handling

The server returns structured error messages in JSON format when operations fail. Common errors include:

- File not found errors
- Schema validation errors
- Column not found errors
- Filter matching errors

## Security Notes

- All write operations create audit log entries for traceability
- Audit logs are stored in `data/logs/audit_log.parquet`
- Optional full snapshots can be configured for additional safety
- Never commit sensitive data files to version control

## Troubleshooting

1. **File Not Found Errors**
   - Verify the data type exists in `data/[type]/[type].parquet`
   - Check file permissions

2. **Schema Validation Errors**
   - Ensure records match the schema defined in `data/schemas/[type]_schema.json`
   - Check required fields are present

3. **Filter Matching Errors**
   - Verify filter syntax matches supported operators
   - Check column names exist in the schema

## Testing

After installation/updates, run the test script:

```bash
python3 mcp-servers/parquet/test_audit_log.py
```

This validates:
- Audit log creation
- Schema compliance
- Operation tracking

See [IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md) for manual testing procedures.

## Documentation

- **[README.md](README.md)** - This file, overview and quick reference
- **[AUDIT_LOG_GUIDE.md](AUDIT_LOG_GUIDE.md)** - Complete audit log documentation
- **[IMPLEMENTATION_SUMMARY.md](IMPLEMENTATION_SUMMARY.md)** - Implementation details and testing
- **[SETUP.md](SETUP.md)** - Setup and configuration instructions

## Notes

- The server uses audit log for efficient change tracking (99%+ storage reduction)
- All date fields are automatically converted to ISO format strings in responses
- Null/NaN values are converted to `null` in JSON responses
- The server runs in stdio mode for MCP communication
- Audit log entries are never automatically deleted (manual archival if needed)

## License

MIT

## Support

- [GitHub Issues](https://github.com/markmhendrickson/mcp-server-parquet/issues)

