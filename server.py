import json
import logging
from typing import Optional
from mcp.server.fastmcp import FastMCP
from pydantic import Field
import requests
import os
from oauth import OAuthConfig, OAuthSession
from connect_api_dc_sql import run_query
from connect_api_dc_admin import DataCloudAdminClient
from swagger_client import get_api_schema as _swagger_get_api_schema
# Get logger for this module
logger = logging.getLogger(__name__)


# Create an MCP server
mcp = FastMCP("Demo")

# Global config and session
sf_org: OAuthConfig = OAuthConfig.from_env()
oauth_session: OAuthSession = OAuthSession(sf_org)
admin_client: DataCloudAdminClient = DataCloudAdminClient(oauth_session)

# Non-auth configuration
DEFAULT_LIST_TABLE_FILTER = os.getenv('DEFAULT_LIST_TABLE_FILTER', '%')


# =============================================================================
# SCOPE 1 — Catalog / Metadata tools
# =============================================================================

@mcp.tool(description="Lists the available tables in the database")
def list_tables() -> list[str]:
    sql = "SELECT c.relname AS TABLE_NAME FROM pg_catalog.pg_namespace n, pg_catalog.pg_class c LEFT JOIN pg_catalog.pg_description d ON (c.oid = d.objoid AND d.objsubid = 0  and d.classoid = 'pg_class'::regclass) WHERE c.relnamespace = n.oid AND c.relname LIKE '%s'" % DEFAULT_LIST_TABLE_FILTER
    result = run_query(oauth_session, sql)
    data = result.get("data", [])
    return [x[0] for x in data]


@mcp.tool(description="Describes the columns of a table")
def describe_table(
    table: str = Field(description="The table name"),
) -> list[str]:
    sql = f"SELECT a.attname FROM pg_catalog.pg_namespace n JOIN pg_catalog.pg_class c ON (c.relnamespace = n.oid) JOIN pg_catalog.pg_attribute a ON (a.attrelid = c.oid) JOIN pg_catalog.pg_type t ON (a.atttypid = t.oid) LEFT JOIN pg_catalog.pg_attrdef def ON (a.attrelid = def.adrelid AND a.attnum = def.adnum) LEFT JOIN pg_catalog.pg_description dsc ON (c.oid = dsc.objoid AND a.attnum = dsc.objsubid) LEFT JOIN pg_catalog.pg_class dc ON (dc.oid = dsc.classoid AND dc.relname = 'pg_class') LEFT JOIN pg_catalog.pg_namespace dn ON (dc.relnamespace = dn.oid AND dn.nspname = 'pg_catalog') WHERE a.attnum > 0 AND NOT a.attisdropped AND c.relname='{table}'"
    result = run_query(oauth_session, sql)
    data = result.get("data", [])
    return [x[0] for x in data]


# =============================================================================
# SCOPE 2 — Query tool
# =============================================================================

@mcp.tool(description="Executes a SQL query and returns the results")
def query(
    sql: str = Field(
        description="A SQL query in the PostgreSQL dialect make sure to always quote all identifies and use the exact casing. To formulate the query first verify which tables and fields to use through the suggest fields tool (or if it is broken through the list tables / describe tables call). Before executing the tool provide the user a succinct summary (targeted to low code users) on the semantics of the query"),
):
    return run_query(oauth_session, sql)


# =============================================================================
# SCOPE 2b — API Schema lookup (live Swagger spec)
# =============================================================================

@mcp.tool(description=(
    "Fetches the official Salesforce Data 360 Connect API Swagger spec at runtime and returns "
    "the request-body JSON Schema plus a concrete input example for the given tool. "
    "Call this BEFORE any create_* or update_* tool to get the exact field names, types, and "
    "required fields expected by the live API. "
    "Valid tool_name values: create_calculated_insight, update_calculated_insight, "
    "create_dlo, update_dlo, create_dmo, update_dmo, "
    "create_segment, update_segment, "
    "create_identity_resolution_ruleset, update_identity_resolution_ruleset, "
    "create_data_stream, update_data_stream."
))
def get_api_schema(
    tool_name: str = Field(
        description=(
            "Name of the MCP tool you are about to call. "
            "E.g. 'create_calculated_insight', 'update_segment', 'create_dlo'."
        )
    ),
) -> dict:
    """Return the live API schema and example for *tool_name* from the Swagger spec."""
    return _swagger_get_api_schema(tool_name)


# =============================================================================
# SCOPE 3 — Data / Process CRUD and Actions
# =============================================================================
#
# Resources covered:
#   • Data Streams           – list, get, refresh, deploy
#   • Data Lake Objects      – list, get, create, update, delete, deploy
#   • Data Model Objects     – list, get, create, update, delete
#   • Segments               – list, get, create, update, delete, publish, refresh
#   • Identity Resolution    – list, get, create, update, delete, run
#   • Calculated Insights    – list, get, create, update, delete, refresh
#   • Activation Targets     – list, get, create, update, delete
#   • Activations            – list, get, create, update, delete, publish
# =============================================================================

# ---------------------------------------------------------------------------
# Data Streams
# ---------------------------------------------------------------------------

@mcp.tool(description=(
    "List all Data Streams configured in Data 360. "
    "Data Streams define how external data is ingested into Data Cloud (e.g. S3, Marketing Cloud, CRM). "
    "Returns a list of data stream objects including their IDs, names, and status."
))
def list_data_streams(
    dataspace: str = Field(default="default", description="The Data Cloud dataspace name (default: 'default')"),
):
    return admin_client.list_data_streams(dataspace)


@mcp.tool(description=(
    "Get full details of a specific Data Stream by its ID or developer name. "
    "Returns configuration including source connector type, target DLO mapping, schedule, and current status."
))
def get_data_stream(
    stream_id: str = Field(description="The Data Stream ID or developer name"),
    dataspace: str = Field(default="default", description="The Data Cloud dataspace name (default: 'default')"),
):
    return admin_client.get_data_stream(stream_id, dataspace)


@mcp.tool(description=(
    "Create a new Data Stream in Data 360. "
    "A Data Stream ingests records from a source (Salesforce CRM home org, S3, Marketing Cloud, etc.) "
    "into a Data Lake Object. "
    "To create a CRM home org stream, first call list_data_streams to get the connectorId of an existing "
    "home org stream, then model the new stream after it. "
    "Before calling this tool, use get_api_schema('create_data_stream') to retrieve the exact expected payload."
))
def create_data_stream(
    body_json: str = Field(description=(
        "JSON string with the data stream configuration. "
        "Call get_api_schema('create_data_stream') first to get the correct field names and structure. "
        "Get the connector ID by calling list_data_streams and inspecting an existing stream of the same source type."
    )),
):
    return admin_client.create_data_stream(json.loads(body_json))


@mcp.tool(description=(
    "Update an existing Data Stream — change its label, field list, or schedule. "
    "Only include the properties you want to change. "
    "The source connector and source object cannot be changed after creation. "
    "Before calling this tool, use get_api_schema('update_data_stream') to retrieve the exact expected payload."
))
def update_data_stream(
    stream_id: str = Field(description="The Data Stream ID or developer name to update"),
    body_json: str = Field(description=(
        "JSON string with the updates. "
        "Call get_api_schema('update_data_stream') first to get the correct field names and structure."
    )),
):
    return admin_client.update_data_stream(stream_id, json.loads(body_json))


@mcp.tool(description=(
    "Delete a Data Stream. "
    "Warning: this removes the data stream configuration. "
    "Data already ingested into the target DLO is not automatically deleted."
))
def delete_data_stream(
    stream_id: str = Field(description="The Data Stream ID or developer name to delete"),
):
    return admin_client.delete_data_stream(stream_id)


@mcp.tool(description=(
    "Trigger an immediate ingestion refresh run for a Data Stream. "
    "Use this to pull the latest data from the source connector without waiting for the scheduled run. "
    "Provide the Data Stream ID (visible from list_data_streams)."
))
def refresh_data_stream(
    stream_id: str = Field(description="The Data Stream ID or developer name to refresh"),
):
    return admin_client.refresh_data_stream(stream_id)


@mcp.tool(description=(
    "Deploy (activate) a Data Stream so it starts ingesting data. "
    "A data stream must be deployed before it can run. "
    "Provide the Data Stream ID (visible from list_data_streams)."
))
def deploy_data_stream(
    stream_id: str = Field(description="The Data Stream ID or developer name to deploy"),
):
    return admin_client.deploy_data_stream(stream_id)


# ---------------------------------------------------------------------------
# Data Lake Objects (DLO)
# ---------------------------------------------------------------------------

@mcp.tool(description=(
    "List all Data Lake Objects (DLOs) in Data 360. "
    "DLOs are the raw storage layer that holds ingested data before it is mapped to semantic Data Model Objects. "
    "Returns DLO names, IDs, categories, and status."
))
def list_dlos(
    dataspace: str = Field(default="default", description="The Data Cloud dataspace name (default: 'default')"),
):
    return admin_client.list_dlos(dataspace)


@mcp.tool(description=(
    "Get full details of a specific Data Lake Object (DLO) including its fields, data types, "
    "category (Profile, Engagement, Other), and relationships."
))
def get_dlo(
    dlo_id: str = Field(description="The DLO ID or developer name"),
    dataspace: str = Field(default="default", description="The Data Cloud dataspace name (default: 'default')"),
):
    return admin_client.get_dlo(dlo_id, dataspace)


@mcp.tool(description=(
    "Create a new Data Lake Object (DLO) in Data 360. "
    "Provide the configuration as a JSON string. "
    "Before calling this tool, use get_api_schema('create_dlo') to retrieve the exact expected payload."
))
def create_dlo(
    body_json: str = Field(description=(
        "JSON string with the DLO configuration. "
        "Call get_api_schema('create_dlo') first to get the correct field names and structure."
    )),
):
    return admin_client.create_dlo(json.loads(body_json))


@mcp.tool(description=(
    "Update an existing Data Lake Object (DLO). "
    "Only include the fields you want to change in the body. "
    "You can add new fields or update the label. You cannot change the primary key or category after creation. "
    "Before calling this tool, use get_api_schema('update_dlo') to retrieve the exact expected payload."
))
def update_dlo(
    dlo_id: str = Field(description="The DLO ID or developer name to update"),
    body_json: str = Field(description=(
        "JSON string with the fields to update. "
        "Call get_api_schema('update_dlo') first to get the correct field names and structure."
    )),
):
    return admin_client.update_dlo(dlo_id, json.loads(body_json))


@mcp.tool(description=(
    "Delete a Data Lake Object (DLO). "
    "Warning: this permanently removes the DLO definition. Any data stored in the DLO will also be removed. "
    "The DLO must not be referenced by active Data Streams or DMO mappings."
))
def delete_dlo(
    dlo_id: str = Field(description="The DLO ID or developer name to delete"),
):
    return admin_client.delete_dlo(dlo_id)


@mcp.tool(description=(
    "Deploy a Data Lake Object (DLO) to make it active and queryable. "
    "After creation or updates, a DLO must be deployed before data can be written to or queried from it."
))
def deploy_dlo(
    dlo_id: str = Field(description="The DLO ID or developer name to deploy"),
):
    return admin_client.deploy_dlo(dlo_id)


# ---------------------------------------------------------------------------
# Data Model Objects (DMO)
# ---------------------------------------------------------------------------

@mcp.tool(description=(
    "List all Data Model Objects (DMOs) in Data 360. "
    "DMOs are the semantic layer — they define the canonical data model (e.g. Individual, Contact Point Email) "
    "and map raw DLO data into a unified schema. Returns DMO names, IDs, categories, and status."
))
def list_dmos(
    dataspace: str = Field(default="default", description="The Data Cloud dataspace name (default: 'default')"),
):
    return admin_client.list_dmos(dataspace)


@mcp.tool(description=(
    "Get full details of a specific Data Model Object (DMO) including its fields, "
    "relationships to other DMOs, and data source mappings."
))
def get_dmo(
    dmo_id: str = Field(description="The DMO ID or developer name (e.g. ssot__Individual__dlm)"),
    dataspace: str = Field(default="default", description="The Data Cloud dataspace name (default: 'default')"),
):
    return admin_client.get_dmo(dmo_id, dataspace)


@mcp.tool(description=(
    "Create a new custom Data Model Object (DMO) in Data 360. "
    "Use this to define custom semantic objects outside of the standard data model. "
    "Standard DMOs like ssot__Individual__dlm cannot be created — only custom ones. "
    "Before calling this tool, use get_api_schema('create_dmo') to retrieve the exact expected payload."
))
def create_dmo(
    body_json: str = Field(description=(
        "JSON string with the DMO configuration. "
        "Call get_api_schema('create_dmo') first to get the correct field names and structure."
    )),
):
    return admin_client.create_dmo(json.loads(body_json))


@mcp.tool(description=(
    "Update an existing Data Model Object (DMO). "
    "You can add new custom fields or update metadata. "
    "Standard system fields on built-in DMOs cannot be modified. "
    "Before calling this tool, use get_api_schema('update_dmo') to retrieve the exact expected payload."
))
def update_dmo(
    dmo_id: str = Field(description="The DMO ID or developer name to update"),
    body_json: str = Field(description=(
        "JSON string with the updates. "
        "Call get_api_schema('update_dmo') first to get the correct field names and structure."
    )),
):
    return admin_client.update_dmo(dmo_id, json.loads(body_json))


@mcp.tool(description=(
    "Delete a custom Data Model Object (DMO). "
    "Warning: this removes the DMO definition. Only custom DMOs can be deleted — standard system DMOs cannot."
))
def delete_dmo(
    dmo_id: str = Field(description="The DMO ID or developer name to delete"),
):
    return admin_client.delete_dmo(dmo_id)


# ---------------------------------------------------------------------------
# Segments
# ---------------------------------------------------------------------------

@mcp.tool(description=(
    "List all Segments defined in Data 360. "
    "Segments are audiences defined by filter criteria on DMO data (e.g. 'all customers who purchased in last 30 days'). "
    "Returns segment names, IDs, target entity, population count, and publish status."
))
def list_segments(
    dataspace: str = Field(default="default", description="The Data Cloud dataspace name (default: 'default')"),
):
    return admin_client.list_segments(dataspace)


@mcp.tool(description=(
    "Get full details of a specific Segment including its filter criteria (segment definition), "
    "target entity (usually Individual), membership count, schedule, and activation status."
))
def get_segment(
    segment_id: str = Field(description="The Segment ID or developer name"),
    dataspace: str = Field(default="default", description="The Data Cloud dataspace name (default: 'default')"),
):
    return admin_client.get_segment(segment_id, dataspace)


@mcp.tool(description=(
    "Create a new Segment in Data 360. "
    "A segment defines an audience using filter criteria on DMO data. "
    "Before calling this tool, use get_api_schema('create_segment') to retrieve the exact expected payload."
))
def create_segment(
    body_json: str = Field(description=(
        "JSON string defining the segment. "
        "Call get_api_schema('create_segment') first to get the correct field names and structure."
    )),
):
    return admin_client.create_segment(json.loads(body_json))


@mcp.tool(description=(
    "Update an existing Segment — change its label, filter criteria, description, or refresh schedule. "
    "Only include the properties you want to change. "
    "Before calling this tool, use get_api_schema('update_segment') to retrieve the exact expected payload."
))
def update_segment(
    segment_id: str = Field(description="The Segment ID or developer name to update"),
    body_json: str = Field(description=(
        "JSON string with the updates. "
        "Call get_api_schema('update_segment') first to get the correct field names and structure."
    )),
):
    return admin_client.update_segment(segment_id, json.loads(body_json))


@mcp.tool(description=(
    "Delete a Segment from Data 360. "
    "Warning: deleting a segment also removes all associated activations. "
    "The segment must be unpublished before deletion."
))
def delete_segment(
    segment_id: str = Field(description="The Segment ID or developer name to delete"),
):
    return admin_client.delete_segment(segment_id)


@mcp.tool(description=(
    "Publish a Segment to make it available for Activations. "
    "A segment must be published before it can be used in activation flows to push data to marketing channels. "
    "Publishing also triggers a membership calculation."
))
def publish_segment(
    segment_id: str = Field(description="The Segment ID or developer name to publish"),
):
    return admin_client.publish_segment(segment_id)


@mcp.tool(description=(
    "Trigger a Segment membership recalculation (refresh). "
    "Use this to recompute segment membership based on the latest data without waiting for the scheduled run."
))
def refresh_segment(
    segment_id: str = Field(description="The Segment ID or developer name to refresh"),
):
    return admin_client.refresh_segment(segment_id)


# ---------------------------------------------------------------------------
# Identity Resolution Rulesets
# ---------------------------------------------------------------------------

@mcp.tool(description=(
    "List all Identity Resolution (IR) Rulesets in Data 360. "
    "Identity Resolution unifies profile data across sources by matching individuals using rules "
    "(e.g. exact email match, fuzzy name+address match). "
    "Returns ruleset names, IDs, target DMO, and last run status."
))
def list_identity_resolution_rulesets(
    dataspace: str = Field(default="default", description="The Data Cloud dataspace name (default: 'default')"),
):
    return admin_client.list_identity_resolution_rulesets(dataspace)


@mcp.tool(description=(
    "Get full details of an Identity Resolution Ruleset including all matching rules, "
    "reconciliation rules, target DMO, and run history."
))
def get_identity_resolution_ruleset(
    ruleset_id: str = Field(description="The Identity Resolution Ruleset ID or developer name"),
    dataspace: str = Field(default="default", description="The Data Cloud dataspace name (default: 'default')"),
):
    return admin_client.get_identity_resolution_ruleset(ruleset_id, dataspace)


@mcp.tool(description=(
    "Create a new Identity Resolution Ruleset in Data 360. "
    "Define matching rules (how to detect the same individual across sources) and reconciliation rules "
    "(how to pick the best value when sources disagree). "
    "Before calling this tool, use get_api_schema('create_identity_resolution_ruleset') to retrieve the exact expected payload."
))
def create_identity_resolution_ruleset(
    body_json: str = Field(description=(
        "JSON string defining the IR ruleset. "
        "Call get_api_schema('create_identity_resolution_ruleset') first to get the correct field names and structure."
    )),
):
    return admin_client.create_identity_resolution_ruleset(json.loads(body_json))


@mcp.tool(description=(
    "Update an existing Identity Resolution Ruleset — modify matching rules, "
    "reconciliation rules, or metadata. Only include properties you want to change. "
    "Before calling this tool, use get_api_schema('update_identity_resolution_ruleset') to retrieve the exact expected payload."
))
def update_identity_resolution_ruleset(
    ruleset_id: str = Field(description="The IR Ruleset ID or developer name to update"),
    body_json: str = Field(description=(
        "JSON string with the updates. "
        "Call get_api_schema('update_identity_resolution_ruleset') first to get the correct field names and structure."
    )),
):
    return admin_client.update_identity_resolution_ruleset(ruleset_id, json.loads(body_json))


@mcp.tool(description=(
    "Delete an Identity Resolution Ruleset. "
    "Warning: deleting a ruleset will remove the unified individual data it produced. "
    "Ensure no downstream segments or activations depend on it before deleting."
))
def delete_identity_resolution_ruleset(
    ruleset_id: str = Field(description="The IR Ruleset ID or developer name to delete"),
):
    return admin_client.delete_identity_resolution_ruleset(ruleset_id)


@mcp.tool(description=(
    "Trigger an Identity Resolution run for a given ruleset. "
    "This re-processes all profile data through the matching and reconciliation rules "
    "to produce updated unified individual records."
))
def run_identity_resolution(
    ruleset_id: str = Field(description="The IR Ruleset ID or developer name to run"),
):
    return admin_client.run_identity_resolution(ruleset_id)


# ---------------------------------------------------------------------------
# Calculated Insights
# ---------------------------------------------------------------------------

@mcp.tool(description=(
    "List all Calculated Insights (CIs) in Data 360. "
    "Calculated Insights are pre-computed aggregations defined by SQL (e.g. total spend, LTV, days since last purchase). "
    "Results are stored as DMO attributes for use in segments and activations. "
    "Returns CI names, IDs, target DMO, and last refresh status."
))
def list_calculated_insights(
    dataspace: str = Field(default="default", description="The Data Cloud dataspace name (default: 'default')"),
):
    return admin_client.list_calculated_insights(dataspace)


@mcp.tool(description=(
    "Get full details of a specific Calculated Insight including the SQL expression, "
    "output fields mapped to the target DMO, and refresh schedule."
))
def get_calculated_insight(
    ci_id: str = Field(description="The Calculated Insight ID or developer name"),
    dataspace: str = Field(default="default", description="The Data Cloud dataspace name (default: 'default')"),
):
    return admin_client.get_calculated_insight(ci_id, dataspace)


@mcp.tool(description=(
    "Create a new Calculated Insight in Data 360. "
    "Define a SQL expression over DMO data to compute aggregate metrics per individual or entity. "
    "IMPORTANT: Before calling this tool, always call get_api_schema('create_calculated_insight') "
    "to retrieve the exact expected payload from the live API spec."
))
def create_calculated_insight(
    body_json: str = Field(description=(
        "JSON string with the Calculated Insight definition. "
        "Call get_api_schema('create_calculated_insight') first to get the correct field names and structure."
    )),
):
    return admin_client.create_calculated_insight(json.loads(body_json))


@mcp.tool(description=(
    "Update an existing Calculated Insight — modify the SQL expression, label, output fields, or schedule. "
    "Only include the properties you want to change. "
    "Before calling this tool, use get_api_schema('update_calculated_insight') to retrieve the exact expected payload."
))
def update_calculated_insight(
    ci_id: str = Field(description="The Calculated Insight ID or developer name to update"),
    body_json: str = Field(description=(
        "JSON string with the updates. "
        "Call get_api_schema('update_calculated_insight') first to get the correct field names and structure."
    )),
):
    return admin_client.update_calculated_insight(ci_id, json.loads(body_json))


@mcp.tool(description=(
    "Delete a Calculated Insight. "
    "Warning: this removes the CI and its computed output attributes from the target DMO. "
    "Segments or activations that use the CI output fields must be updated first."
))
def delete_calculated_insight(
    ci_id: str = Field(description="The Calculated Insight ID or developer name to delete"),
):
    return admin_client.delete_calculated_insight(ci_id)


@mcp.tool(description=(
    "Trigger a recalculation of a Calculated Insight. "
    "Use this to recompute aggregate values against the latest data "
    "without waiting for the scheduled refresh window."
))
def refresh_calculated_insight(
    ci_id: str = Field(description="The Calculated Insight ID or developer name to refresh"),
):
    return admin_client.refresh_calculated_insight(ci_id)


# ---------------------------------------------------------------------------
# Activation Targets
# ---------------------------------------------------------------------------

@mcp.tool(description=(
    "List all Activation Targets configured in Data 360. "
    "Activation Targets are the destinations where segment data is pushed (e.g. Marketing Cloud, Google Ads, Meta). "
    "Returns target names, IDs, connector type, and connection status."
))
def list_activation_targets(
    dataspace: str = Field(default="default", description="The Data Cloud dataspace name (default: 'default')"),
):
    return admin_client.list_activation_targets(dataspace)


@mcp.tool(description=(
    "Get full details of a specific Activation Target including connector configuration, "
    "authentication credentials reference, and associated activations."
))
def get_activation_target(
    target_id: str = Field(description="The Activation Target ID or developer name"),
    dataspace: str = Field(default="default", description="The Data Cloud dataspace name (default: 'default')"),
):
    return admin_client.get_activation_target(target_id, dataspace)


@mcp.tool(description=(
    "Create a new Activation Target in Data 360. "
    "An activation target defines the connected marketing platform where segment members will be pushed. "
    "Example body for a Marketing Cloud target:\n"
    '{"name": "MCActivationTarget", "label": "Marketing Cloud Target", '
    '"connectorType": "SalesforceMarketingCloud", '
    '"connectionProperties": {"businessUnitId": "12345678"}}'
))
def create_activation_target(
    body_json: str = Field(description=(
        "JSON string defining the activation target. "
        "Required: name, label, connectorType. "
        "connectorType values include: SalesforceMarketingCloud, GoogleAds, MetaAds, AmazonAds, "
        "S3, Snowflake, or other supported connector types. "
        "connectionProperties varies by connector type."
    )),
):
    return admin_client.create_activation_target(json.loads(body_json))


@mcp.tool(description=(
    "Update an existing Activation Target — modify the label, connection properties, or credentials. "
    "Only include the properties you want to change."
))
def update_activation_target(
    target_id: str = Field(description="The Activation Target ID or developer name to update"),
    body_json: str = Field(description=(
        "JSON string with the updates. Example: "
        '{"label": "Updated Target Name", "connectionProperties": {"businessUnitId": "99999999"}}'
    )),
):
    return admin_client.update_activation_target(target_id, json.loads(body_json))


@mcp.tool(description=(
    "Delete an Activation Target. "
    "Warning: all Activations that use this target must be deleted first. "
    "Deleting a target does not delete the data already pushed to the external platform."
))
def delete_activation_target(
    target_id: str = Field(description="The Activation Target ID or developer name to delete"),
):
    return admin_client.delete_activation_target(target_id)


# ---------------------------------------------------------------------------
# Activations
# ---------------------------------------------------------------------------

@mcp.tool(description=(
    "List all Activations in Data 360. "
    "An Activation links a published Segment to an Activation Target and defines which attributes "
    "to include in the data push. "
    "Returns activation names, IDs, linked segment, target, schedule, and last publish status."
))
def list_activations(
    dataspace: str = Field(default="default", description="The Data Cloud dataspace name (default: 'default')"),
):
    return admin_client.list_activations(dataspace)


@mcp.tool(description=(
    "Get full details of a specific Activation including the segment it uses, the target it pushes to, "
    "the attribute mapping, schedule, and publish history."
))
def get_activation(
    activation_id: str = Field(description="The Activation ID or developer name"),
    dataspace: str = Field(default="default", description="The Data Cloud dataspace name (default: 'default')"),
):
    return admin_client.get_activation(activation_id, dataspace)


@mcp.tool(description=(
    "Create a new Activation in Data 360. "
    "An activation wires a published segment to an activation target and specifies which profile attributes "
    "to include in the data push. "
    "Example body:\n"
    '{"name": "HighValueToMC", "label": "High Value Customers to MC", '
    '"segmentId": "<segment_id>", "activationTargetId": "<target_id>", '
    '"attributeSets": [{"name": "ContactAttributes", '
    '"fields": [{"sourceField": "ssot__Email__c", "targetField": "EmailAddress"}]}]}'
))
def create_activation(
    body_json: str = Field(description=(
        "JSON string defining the activation. "
        "Required: name, label, segmentId (from list_segments), activationTargetId (from list_activation_targets), "
        "attributeSets (list of attribute groups mapping source DMO fields to target platform fields). "
        "The segment must be published before creating an activation."
    )),
):
    return admin_client.create_activation(json.loads(body_json))


@mcp.tool(description=(
    "Update an existing Activation — modify the attribute mapping, schedule, or label. "
    "Only include the properties you want to change. The segment and target cannot be changed; "
    "delete and recreate the activation if those need to change."
))
def update_activation(
    activation_id: str = Field(description="The Activation ID or developer name to update"),
    body_json: str = Field(description=(
        "JSON string with the updates. Example: "
        '{"label": "Updated Activation", "attributeSets": [...]}'
    )),
):
    return admin_client.update_activation(activation_id, json.loads(body_json))


@mcp.tool(description=(
    "Delete an Activation. "
    "This stops the data push from the linked segment to the activation target. "
    "Data already pushed to the target platform is not removed."
))
def delete_activation(
    activation_id: str = Field(description="The Activation ID or developer name to delete"),
):
    return admin_client.delete_activation(activation_id)


@mcp.tool(description=(
    "Publish an Activation to trigger a data push of the segment members to the activation target. "
    "Use this to send the latest segment data to a marketing platform immediately, "
    "without waiting for the scheduled publish window."
))
def publish_activation(
    activation_id: str = Field(description="The Activation ID or developer name to publish"),
):
    return admin_client.publish_activation(activation_id)


# =============================================================================
# Entry point
# =============================================================================

if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )

    logger.info("Starting MCP server")
    mcp.run()
