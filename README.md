# Data 360 MCP Server

A Model Context Protocol (MCP) server that connects Cursor (or any MCP-compatible AI client) to Salesforce Data Cloud / Data 360, enabling natural-language-driven exploration, querying, and full configuration management of your Data 360 instance.

> **Built on** [forcedotcom/datacloud-mcp-query](https://github.com/forcedotcom/datacloud-mcp-query) — extended with a full admin CRUD layer, a runtime Swagger spec client, and AI-guided API schema lookup.

---

## What's New vs. the Original

The upstream [forcedotcom/datacloud-mcp-query](https://github.com/forcedotcom/datacloud-mcp-query) provides Scope 1 (catalog) and Scope 2 (query). This fork adds:

| Addition | Description |
|---|---|
| **Scope 2b — `get_api_schema`** | New tool that fetches the live Salesforce Data 360 Connect API Swagger spec at runtime and returns the exact request-body schema + canonical example for any `create_*` or `update_*` tool. Eliminates guessing field names. |
| **Scope 3 — Admin CRUD** | 40+ new tools covering full lifecycle management of Data Streams, Data Lake Objects (DLO), Data Model Objects (DMO), Segments, Identity Resolution Rulesets, Calculated Insights, Activation Targets, and Activations. |
| **`swagger_client.py`** | Runtime Swagger spec loader. Fetches from `SWAGGER_URL` (Salesforce static CDN) with a local file fallback via `SWAGGER_PATH`. Parsed once, cached in-process. |
| **`connect_api_dc_admin.py`** | REST API client for all Scope 3 admin operations against the `/services/data/v63.0/ssot/` endpoint family. |
| **`pyyaml` dependency** | Added to `requirements.txt` to support YAML parsing of the Swagger spec. |

> **No changes to Salesforce org setup.** The same Connected App, OAuth scopes, and authentication flow from the original repo apply without modification.

---

## Features

### Scope 1 — Catalog / Metadata
- **`list_tables`** — List all queryable Data Cloud tables
- **`describe_table`** — Describe the columns of a table

### Scope 2 — Query
- **`query`** — Execute SQL queries against Data Cloud (PostgreSQL dialect)

### Scope 2b — API Schema Lookup *(new)*
- **`get_api_schema`** — Fetches the live [Data 360 Connect API Swagger spec](https://developer.salesforce.com/docs/data/connectapi/references/spec) at runtime and returns the correct request-body JSON Schema + example for any create/update tool. **Always call this before a `create_*` or `update_*` tool** to get exact field names, types, and required fields.

### Scope 3 — Data & Process Configuration *(new)*

| Resource | Operations |
|---|---|
| **Data Streams** | list, get, create, update, delete, refresh, deploy |
| **Data Lake Objects (DLO)** | list, get, create, update, delete, deploy |
| **Data Model Objects (DMO)** | list, get, create, update, delete |
| **Segments** | list, get, create, update, delete, publish, refresh |
| **Identity Resolution Rulesets** | list, get, create, update, delete, run |
| **Calculated Insights** | list, get, create, update, delete, refresh |
| **Activation Targets** | list, get, create, update, delete |
| **Activations** | list, get, create, update, delete, publish |

---

## Adding to Cursor

1. Clone this repository to your local machine.
2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
3. *(Optional but recommended)* Download the Salesforce Connect API Swagger spec locally for offline use:
   ```bash
   # Obtain cdp-connect-api-Swagger.yaml from the Salesforce developer portal
   # and place it anywhere accessible, e.g. ~/Downloads/cdp-connect-api-Swagger.yaml
   ```
4. Open **Cursor Settings → MCP → Add new global MCP server** and paste:
   ```json
   "mcpServers": {
     "datacloud": {
       "command": "<path to python>",
       "args": ["<full path to>/server.py"],
       "env": {
         "SF_CLIENT_ID": "<Client Id>",
         "SF_CLIENT_SECRET": "<Client Secret>",
         "SWAGGER_PATH": "<absolute path to>/cdp-connect-api-Swagger.yaml"
       },
       "disabled": false,
       "autoApprove": [
         "list_tables", "describe_table",
         "get_api_schema",
         "list_data_streams", "get_data_stream",
         "list_dlos", "get_dlo",
         "list_dmos", "get_dmo",
         "list_segments", "get_segment",
         "list_identity_resolution_rulesets", "get_identity_resolution_ruleset",
         "list_calculated_insights", "get_calculated_insight",
         "list_activation_targets", "get_activation_target",
         "list_activations", "get_activation"
       ]
     }
   }
   ```
5. Enable the MCP server and click **Refresh** to see the full tool list.

---

## Configuration

### Required Environment Variables

| Variable | Description |
|---|---|
| `SF_CLIENT_ID` | Salesforce OAuth Connected App Client ID |
| `SF_CLIENT_SECRET` | Salesforce OAuth Connected App Client Secret |

### Optional Environment Variables

| Variable | Default | Description |
|---|---|---|
| `SF_LOGIN_URL` | `login.salesforce.com` | Salesforce login URL (use your My Domain for sandboxes) |
| `SF_CALLBACK_URL` | `http://localhost:55556/Callback` | OAuth callback URL — must be registered in the Connected App |
| `DEFAULT_LIST_TABLE_FILTER` | `%` | SQL LIKE pattern to filter `list_tables` results |
| `SWAGGER_URL` | Salesforce CDN URL | Override the remote URL for the Connect API Swagger spec |
| `SWAGGER_PATH` | *(none)* | Absolute path to a local copy of `cdp-connect-api-Swagger.yaml`. Used as fallback when the remote URL is unreachable, or as primary source for offline use |

See [Connected App Setup Guide](CONNECTED_APP_SETUP.md) for instructions on creating the Connected App.

### Required OAuth Scopes

The Connected App must have the following OAuth scopes enabled:

| Scope | Purpose |
|---|---|
| `api` | Core Salesforce REST API access |
| `cdp_query_api` | Data Cloud SQL query execution |
| `cdp_profile_api` | Data Cloud profile data access |
| `cdp_admin_api` | **Required for Scope 3** — Data Cloud admin/config CRUD operations |

> **Note:** `cdp_admin_api` is required for all Scope 3 tools. If this scope is missing from your Connected App, add it and re-authenticate.

---

## Authentication

The server implements OAuth 2.0 with PKCE (unchanged from the original):
- Automatically opens a browser window for the first authentication
- Handles token exchange and maintains the session
- Token expires after 110 minutes and is automatically refreshed on next tool call

---

## How `get_api_schema` Works

Before calling any `create_*` or `update_*` tool, the AI agent should call `get_api_schema` with the tool name to retrieve the live API contract:

```
get_api_schema("create_calculated_insight")
→ {
    "summary": "Create calculated insights",
    "example": {
      "apiName": "...",
      "displayName": "...",
      "definitionType": "CALCULATED_METRIC",
      "dataSpaceName": "default",
      "expression": "SELECT ... FROM ... GROUP BY ...",
      "publishScheduleInterval": "Six",
      "publishScheduleStartDateTime": "2025-12-11T12:00"
    },
    "schema": { ... }
  }
```

The spec is fetched from the Salesforce static CDN (`SWAGGER_URL`). If that URL is unreachable, it falls back to a local YAML file at `SWAGGER_PATH`. The parsed spec is cached in-process for the lifetime of the server — no repeated network calls.

**Valid `tool_name` values for `get_api_schema`:**

| Tool name | Operation |
|---|---|
| `create_calculated_insight` | Create a new Calculated Insight |
| `update_calculated_insight` | Update an existing Calculated Insight |
| `create_dlo` | Create a Data Lake Object |
| `update_dlo` | Update a Data Lake Object |
| `create_dmo` | Create a Data Model Object |
| `update_dmo` | Update a Data Model Object |
| `create_segment` | Create a Segment |
| `update_segment` | Update a Segment |
| `create_identity_resolution_ruleset` | Create an Identity Resolution Ruleset |
| `update_identity_resolution_ruleset` | Update an Identity Resolution Ruleset |
| `create_data_stream` | Create a Data Stream |
| `update_data_stream` | Update a Data Stream |

---

## Example Natural Language Prompts

Once connected you can ask Cursor things like:

**Querying data:**
- *"List all tables in Data Cloud"*
- *"How many opportunities are there per account?"*
- *"Show me total opportunity amount by stage"*

**Calculated Insights:**
- *"Create a calculated insight to track total opportunities by account"*
- *"Create a monthly opportunity trend insight grouped by account and creation month"*
- *"Refresh the TotalOpportunitiesByAccount calculated insight"*

**Segments:**
- *"Create a segment targeting US accounts with non-closed opportunity amount greater than $10,000"*
- *"List all active segments"*
- *"Publish the HighValueAccounts segment"*

**Data Streams & DLOs:**
- *"List all data streams and show which ones haven't run recently"*
- *"Deploy the Case_Home data stream"*
- *"Show me all Data Lake Objects and their categories"*

**Identity Resolution:**
- *"Run identity resolution for the default ruleset"*
- *"List all identity resolution rulesets and their last run status"*

---

## File Structure

```
datacloud-mcp-query/
├── server.py                  # MCP server — all tool definitions (Scopes 1, 2, 2b, 3)
├── connect_api_dc_sql.py      # SQL query execution client (Scope 2)
├── connect_api_dc_admin.py    # Admin REST API client (Scope 3) ← new
├── swagger_client.py          # Runtime Swagger spec loader — powers get_api_schema ← new
├── oauth.py                   # OAuth 2.0 + PKCE authentication
├── requirements.txt           # Python dependencies (includes pyyaml) ← updated
├── README.md
├── CONNECTED_APP_SETUP.md
└── .gitignore                 # Excludes .env, .cursor/, local Swagger file
```

---

## Security Notes

- **No credentials are stored in this repository.** All secrets are passed via environment variables.
- `.env` files, the Cursor project folder (`.cursor/`), and any local copy of the Swagger spec (`cdp-connect-api-Swagger.yaml`) are excluded via `.gitignore`.
- The `SWAGGER_PATH` env var pointing to your local Swagger copy is set in your Cursor MCP config (not committed).

---

## License

Apache-2.0 — see [LICENSE.txt](LICENSE.txt)
