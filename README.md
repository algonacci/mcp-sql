# mcp-sql

MCP server to give client the ability to access SQL databases (MySQL and PostgreSQL supported)

# Usage

For this MCP server to work, add the following configuration to your MCP config file:

```json
{
  "mcpServers": {
    "sql_access": {
      "command": "uv",
      "args": [
        "--directory",
        "%USERPROFILE%/Documents/GitHub/mcp-sql",
        "run",
        "python",
        "main.py"
      ]
    }
  }
}
```
