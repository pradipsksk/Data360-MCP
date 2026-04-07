"""
One-shot script: create a Data Stream for the Salesforce CRM home org "Case" object.

Steps performed automatically:
  1. Authenticate via OAuth (browser window will open once)
  2. List existing data streams to discover the home org connector ID
  3. Fetch an existing home org stream (Account_Home) to inspect its exact payload shape
  4. POST a new Case_Home data stream modelled on the same connector
  5. Optionally deploy the stream so it starts ingesting

Run:
    python create_case_data_stream.py

Environment variables required (same as the MCP server):
    SF_CLIENT_ID
    SF_CLIENT_SECRET
    SF_LOGIN_URL        (optional, default: login.salesforce.com)
"""

import json
import logging
import sys

from oauth import OAuthConfig, OAuthSession
from connect_api_dc_admin import DataCloudAdminClient

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Standard Case fields to include in the data stream
# ---------------------------------------------------------------------------
CASE_FIELDS = [
    {"name": "Id"},
    {"name": "CaseNumber"},
    {"name": "Subject"},
    {"name": "Description"},
    {"name": "Status"},
    {"name": "Priority"},
    {"name": "Origin"},
    {"name": "Type"},
    {"name": "Reason"},
    {"name": "AccountId"},
    {"name": "ContactId"},
    {"name": "OwnerId"},
    {"name": "ParentId"},
    {"name": "IsEscalated"},
    {"name": "IsClosed"},
    {"name": "ClosedDate"},
    {"name": "CreatedDate"},
    {"name": "LastModifiedDate"},
]

TARGET_STREAM_NAME = "Case_Home"
TARGET_STREAM_LABEL = "Case"
SOURCE_OBJECT = "Case"
CATEGORY = "Other"

# Name of an existing home org stream to borrow the connector ID from
REFERENCE_STREAM_HINT = "Account_Home"


def find_home_org_connector(client: DataCloudAdminClient) -> dict:
    """
    Inspect existing data streams to find the home org CRM connector.
    Returns the connector block (at minimum {"id": "..."}) to reuse.
    """
    logger.info("Fetching list of existing data streams...")
    result = client.list_data_streams()

    streams = result if isinstance(result, list) else result.get("dataStreams", result.get("items", []))
    if not streams:
        logger.warning("No data streams returned — raw response:\n%s", json.dumps(result, indent=2))
        return {}

    logger.info("Found %d data stream(s)", len(streams))

    # Try to find the reference stream first
    reference = None
    for s in streams:
        name = s.get("name", "") or s.get("developerName", "")
        if REFERENCE_STREAM_HINT.lower() in name.lower():
            reference = s
            break

    if not reference:
        # Fall back to the first stream
        reference = streams[0]

    ref_name = reference.get("name") or reference.get("developerName", "<unknown>")
    ref_id = reference.get("id") or reference.get("dataStreamId", "")
    logger.info("Using '%s' (id=%s) as reference stream", ref_name, ref_id)

    # Fetch full details of the reference stream to get connector info
    if ref_id:
        logger.info("Fetching full details of reference stream...")
        detail = client.get_data_stream(ref_id)
        connector = detail.get("connector") or detail.get("sourceConnector", {})
        if connector:
            logger.info("Connector info: %s", json.dumps(connector))
            return connector
        # Some APIs embed connector ID at the top level
        connector_id = detail.get("connectorId") or detail.get("sourceConnectorId")
        if connector_id:
            return {"id": connector_id}

    # Last resort: try top-level connector fields on the list item
    connector = reference.get("connector") or reference.get("sourceConnector", {})
    if connector:
        return connector
    connector_id = reference.get("connectorId") or reference.get("sourceConnectorId", "")
    return {"id": connector_id} if connector_id else {}


def build_payload(connector: dict) -> dict:
    payload = {
        "name": TARGET_STREAM_NAME,
        "label": TARGET_STREAM_LABEL,
        "category": CATEGORY,
        "sourceObjectName": SOURCE_OBJECT,
        "fields": CASE_FIELDS,
    }
    if connector:
        payload["connector"] = connector
    return payload


def main():
    # Authenticate
    sf_org = OAuthConfig.from_env()
    session = OAuthSession(sf_org)
    client = DataCloudAdminClient(session)

    # Discover connector
    connector = find_home_org_connector(client)
    if not connector or not connector.get("id"):
        logger.warning(
            "Could not automatically determine the home org connector ID.\n"
            "Please run `list_data_streams` in the MCP, inspect an existing home org stream,\n"
            "and set the connector ID manually in this script."
        )

    # Build and display the payload before sending
    payload = build_payload(connector)
    print("\n" + "=" * 60)
    print("DATA STREAM PAYLOAD TO BE CREATED:")
    print("=" * 60)
    print(json.dumps(payload, indent=2))
    print("=" * 60)

    answer = input("\nProceed with creating this Data Stream? [y/N] ").strip().lower()
    if answer != "y":
        print("Aborted.")
        sys.exit(0)

    # Create the data stream
    logger.info("Creating Data Stream '%s'...", TARGET_STREAM_NAME)
    try:
        result = client.create_data_stream(payload)
        print("\nData Stream created successfully:")
        print(json.dumps(result, indent=2))
    except Exception as e:
        print(f"\nCreation failed: {e}")
        sys.exit(1)

    # Optionally deploy
    stream_id = (
        result.get("id")
        or result.get("dataStreamId")
        or result.get("name")
        or TARGET_STREAM_NAME
    )
    answer2 = input(f"\nDeploy the stream now (id={stream_id})? [y/N] ").strip().lower()
    if answer2 == "y":
        logger.info("Deploying Data Stream '%s'...", stream_id)
        try:
            deploy_result = client.deploy_data_stream(stream_id)
            print("Deploy result:")
            print(json.dumps(deploy_result, indent=2))
        except Exception as e:
            print(f"Deploy failed: {e}")
            sys.exit(1)
    else:
        print(f"\nSkipped deploy. Run `deploy_data_stream('{stream_id}')` from the MCP when ready.")


if __name__ == "__main__":
    main()
