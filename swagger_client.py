"""
Runtime Swagger client for the Salesforce Data 360 Connect API.

Resolution order for the OpenAPI spec:
  1. HTTP GET from SWAGGER_URL (env override or the canonical Salesforce URL).
  2. Local file at SWAGGER_PATH env var (useful when the remote URL is behind
     a CDN that requires a browser session).

The spec is parsed once and kept in an in-process cache for the lifetime of
the MCP server process.

Environment variables
---------------------
SWAGGER_URL   Override the remote URL (default: Salesforce static asset URL).
SWAGGER_PATH  Absolute path to a local copy of the YAML spec to use as
              fallback (or primary source when the URL is unreachable).
"""

import json
import logging
import os
import threading
from typing import Any

import requests
import yaml

logger = logging.getLogger(__name__)

SWAGGER_URL: str = os.getenv(
    "SWAGGER_URL",
    "https://developer.salesforce.com/static/datacloud/connectapi/spec/cdp-connect-api-Swagger.yaml",
)
SWAGGER_PATH: str | None = os.getenv("SWAGGER_PATH")

# Maps the MCP tool name → Swagger path + HTTP method
# Paths must match exactly the keys in the spec's "paths" object.
_OPERATION_MAP: dict[str, dict[str, str]] = {
    "create_calculated_insight":            {"path": "/ssot/calculated-insights",                                   "method": "post"},
    "update_calculated_insight":            {"path": "/ssot/calculated-insights/{apiName}",                         "method": "patch"},
    "create_dlo":                           {"path": "/ssot/data-lake-objects",                                     "method": "post"},
    "update_dlo":                           {"path": "/ssot/data-lake-objects/{recordIdOrDeveloperName}",           "method": "patch"},
    "create_dmo":                           {"path": "/ssot/data-model-objects",                                    "method": "post"},
    "update_dmo":                           {"path": "/ssot/data-model-objects/{dataModelObjectName}",              "method": "patch"},
    "create_segment":                       {"path": "/ssot/segments",                                              "method": "post"},
    "update_segment":                       {"path": "/ssot/segments/{segmentApiName}",                             "method": "patch"},
    "create_identity_resolution_ruleset":   {"path": "/ssot/identity-resolutions",                                  "method": "post"},
    "update_identity_resolution_ruleset":   {"path": "/ssot/identity-resolutions/{identityResolution}",             "method": "patch"},
    "create_data_stream":                   {"path": "/ssot/data-streams",                                          "method": "post"},
    "update_data_stream":                   {"path": "/ssot/data-streams/{recordIdOrDeveloperName}",                "method": "patch"},
}

_lock = threading.Lock()
_spec_cache: dict[str, Any] | None = None


# ---------------------------------------------------------------------------
# Spec loading
# ---------------------------------------------------------------------------

def _load_from_url() -> dict[str, Any] | None:
    """Try to fetch and parse the YAML spec from SWAGGER_URL. Returns None on failure."""
    try:
        logger.info("Fetching Swagger spec from %s", SWAGGER_URL)
        resp = requests.get(
            SWAGGER_URL,
            timeout=30,
            headers={
                "User-Agent": "Mozilla/5.0 (compatible; datacloud-mcp/1.0)",
                "Accept": "application/yaml, text/yaml, */*",
                "Referer": "https://developer.salesforce.com/docs/data/connectapi/references/spec",
            },
        )
        resp.raise_for_status()
        spec = yaml.safe_load(resp.text)
        logger.info("Swagger spec loaded from URL (%d paths)", len(spec.get("paths", {})))
        return spec
    except Exception as exc:
        logger.warning("Could not load Swagger spec from URL: %s", exc)
        return None


def _load_from_file(path: str) -> dict[str, Any] | None:
    """Try to parse the YAML spec from a local file path. Returns None on failure."""
    try:
        logger.info("Loading Swagger spec from local file: %s", path)
        with open(path, "r", encoding="utf-8") as fh:
            spec = yaml.safe_load(fh)
        logger.info("Swagger spec loaded from file (%d paths)", len(spec.get("paths", {})))
        return spec
    except Exception as exc:
        logger.warning("Could not load Swagger spec from file %s: %s", path, exc)
        return None


def _fetch_spec() -> dict[str, Any]:
    """
    Return the parsed Swagger spec, loading it on first call.
    Resolution order: URL → SWAGGER_PATH env var local file.
    Raises RuntimeError if neither source is available.
    """
    global _spec_cache
    with _lock:
        if _spec_cache is not None:
            return _spec_cache

        spec = _load_from_url()

        if spec is None and SWAGGER_PATH:
            spec = _load_from_file(SWAGGER_PATH)

        if spec is None:
            raise RuntimeError(
                "Swagger spec unavailable. "
                "Set SWAGGER_PATH to the absolute path of the local cdp-connect-api-Swagger.yaml file, "
                f"or ensure {SWAGGER_URL} is reachable."
            )

        _spec_cache = spec
        return _spec_cache


# ---------------------------------------------------------------------------
# Schema / example extraction helpers
# ---------------------------------------------------------------------------

def _resolve_ref(spec: dict[str, Any], ref: str) -> Any:
    """Resolve a $ref pointer like '#/components/schemas/Foo'."""
    if not ref.startswith("#/"):
        return {}
    parts = ref.lstrip("#/").split("/")
    node: Any = spec
    for part in parts:
        if not isinstance(node, dict):
            return {}
        node = node.get(part, {})
    return node


def _inline_schema(spec: dict[str, Any], schema: Any, depth: int = 0) -> Any:
    """Recursively resolve $ref inside a schema (limited depth to avoid cycles)."""
    if depth > 6 or not isinstance(schema, dict):
        return schema
    if "$ref" in schema:
        return _inline_schema(spec, _resolve_ref(spec, schema["$ref"]), depth + 1)
    result: dict[str, Any] = {}
    for k, v in schema.items():
        if isinstance(v, dict):
            result[k] = _inline_schema(spec, v, depth + 1)
        elif isinstance(v, list):
            result[k] = [
                _inline_schema(spec, item, depth + 1) if isinstance(item, dict) else item
                for item in v
            ]
        else:
            result[k] = v
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def get_api_schema(tool_name: str) -> dict[str, Any]:
    """
    Return a dict describing the expected request body for *tool_name*:

      "summary"  – operation summary string from the spec
      "example"  – canonical request-body example (dict) from the spec
      "schema"   – inlined JSON Schema for the request body (dict)

    Raises ValueError for unknown tool names.
    Raises RuntimeError if the spec cannot be loaded from any source.
    """
    if tool_name not in _OPERATION_MAP:
        raise ValueError(
            f"Unknown tool '{tool_name}'. "
            f"Valid values: {sorted(_OPERATION_MAP)}"
        )

    spec = _fetch_spec()
    op_info = _OPERATION_MAP[tool_name]
    path_item = spec.get("paths", {}).get(op_info["path"], {})
    operation: dict[str, Any] = path_item.get(op_info["method"], {})

    summary: str = operation.get("summary", "")

    # Request-body schema
    req_body = operation.get("requestBody", {})
    content = req_body.get("content", {})
    json_content = content.get("application/json", {})
    raw_schema = json_content.get("schema", {})
    schema = _inline_schema(spec, raw_schema)

    # Example: resolve the first inline or $ref example from the spec
    example: Any = None
    examples_map: dict[str, Any] = json_content.get("examples", {})
    for _key, ex_val in examples_map.items():
        if isinstance(ex_val, dict):
            if "$ref" in ex_val:
                resolved = _resolve_ref(spec, ex_val["$ref"])
                candidate = resolved.get("value")
            else:
                candidate = ex_val.get("value")
            if candidate is not None:
                example = candidate
                break

    return {
        "summary": summary,
        "example": example,
        "schema": schema,
    }
