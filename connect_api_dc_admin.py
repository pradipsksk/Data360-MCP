import json
import logging
from typing import Any, Dict, Optional

import requests

from oauth import OAuthSession

logger = logging.getLogger(__name__)

API_VERSION = "v63.0"


class DataCloudAdminClient:
    """
    REST API client for Salesforce Data Cloud / Data 360 admin and configuration operations.

    Covers:
      - Data Streams       (list, get, refresh, deploy)
      - Data Lake Objects  (list, get, create, update, delete, deploy)
      - Data Model Objects (list, get, create, update, delete)
      - Segments           (list, get, create, update, delete, publish, refresh)
      - Identity Resolution Rulesets (list, get, create, update, delete, run)
      - Calculated Insights (list, get, create, update, delete, refresh)
      - Activation Targets  (list, get, create, update, delete)
      - Activations         (list, get, create, update, delete, publish)
    """

    def __init__(self, oauth_session: OAuthSession):
        self.oauth_session = oauth_session

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _base(self) -> str:
        return f"{self.oauth_session.get_instance_url()}/services/data/{API_VERSION}"

    def _headers(self) -> Dict[str, str]:
        return {
            "Authorization": f"Bearer {self.oauth_session.get_token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }

    def _handle(self, response: requests.Response) -> Any:
        logger.info(
            "%s %s -> %s (%.2fs)",
            response.request.method,
            response.url,
            response.status_code,
            response.elapsed.total_seconds(),
        )
        if response.status_code >= 400:
            try:
                err = response.json()
                detail = json.dumps(err)
            except Exception:
                detail = response.text
            raise Exception(f"HTTP {response.status_code} {response.reason}: {detail}")
        if response.status_code == 204 or not response.content:
            return {"status": "success"}
        return response.json()

    def _get(self, path: str, params: Optional[Dict] = None) -> Any:
        return self._handle(
            requests.get(f"{self._base()}{path}", headers=self._headers(), params=params or {}, timeout=60)
        )

    def _post(self, path: str, body: Optional[Dict] = None) -> Any:
        return self._handle(
            requests.post(f"{self._base()}{path}", headers=self._headers(), json=body or {}, timeout=60)
        )

    def _patch(self, path: str, body: Dict) -> Any:
        return self._handle(
            requests.patch(f"{self._base()}{path}", headers=self._headers(), json=body, timeout=60)
        )

    def _delete(self, path: str) -> Any:
        return self._handle(
            requests.delete(f"{self._base()}{path}", headers=self._headers(), timeout=60)
        )

    # ------------------------------------------------------------------
    # Data Streams
    # ------------------------------------------------------------------

    def list_data_streams(self, dataspace: str = "default") -> Any:
        return self._get("/ssot/data-streams", {"dataspace": dataspace})

    def get_data_stream(self, stream_id: str, dataspace: str = "default") -> Any:
        return self._get(f"/ssot/data-streams/{stream_id}", {"dataspace": dataspace})

    def refresh_data_stream(self, stream_id: str) -> Any:
        """Trigger a data stream ingestion/refresh run."""
        return self._post(f"/ssot/data-streams/{stream_id}/actions/refresh")

    def create_data_stream(self, body: Dict[str, Any]) -> Any:
        return self._post("/ssot/data-streams", body)

    def update_data_stream(self, stream_id: str, body: Dict[str, Any]) -> Any:
        return self._patch(f"/ssot/data-streams/{stream_id}", body)

    def delete_data_stream(self, stream_id: str) -> Any:
        return self._delete(f"/ssot/data-streams/{stream_id}")

    def deploy_data_stream(self, stream_id: str) -> Any:
        """Deploy (activate) a data stream."""
        return self._post(f"/ssot/data-streams/{stream_id}/actions/deploy")

    # ------------------------------------------------------------------
    # Data Lake Objects (DLO)
    # ------------------------------------------------------------------

    def list_dlos(self, dataspace: str = "default") -> Any:
        return self._get("/ssot/data-lake-objects", {"dataspace": dataspace})

    def get_dlo(self, dlo_id: str, dataspace: str = "default") -> Any:
        return self._get(f"/ssot/data-lake-objects/{dlo_id}", {"dataspace": dataspace})

    def create_dlo(self, body: Dict[str, Any]) -> Any:
        return self._post("/ssot/data-lake-objects", body)

    def update_dlo(self, dlo_id: str, body: Dict[str, Any]) -> Any:
        return self._patch(f"/ssot/data-lake-objects/{dlo_id}", body)

    def delete_dlo(self, dlo_id: str) -> Any:
        return self._delete(f"/ssot/data-lake-objects/{dlo_id}")

    def deploy_dlo(self, dlo_id: str) -> Any:
        """Deploy a Data Lake Object to make it queryable."""
        return self._post(f"/ssot/data-lake-objects/{dlo_id}/actions/deploy")

    # ------------------------------------------------------------------
    # Data Model Objects (DMO)
    # ------------------------------------------------------------------

    def list_dmos(self, dataspace: str = "default") -> Any:
        return self._get("/ssot/data-model-objects", {"dataspace": dataspace})

    def get_dmo(self, dmo_id: str, dataspace: str = "default") -> Any:
        return self._get(f"/ssot/data-model-objects/{dmo_id}", {"dataspace": dataspace})

    def create_dmo(self, body: Dict[str, Any]) -> Any:
        return self._post("/ssot/data-model-objects", body)

    def update_dmo(self, dmo_id: str, body: Dict[str, Any]) -> Any:
        return self._patch(f"/ssot/data-model-objects/{dmo_id}", body)

    def delete_dmo(self, dmo_id: str) -> Any:
        return self._delete(f"/ssot/data-model-objects/{dmo_id}")

    # ------------------------------------------------------------------
    # Segments
    # ------------------------------------------------------------------

    def list_segments(self, dataspace: str = "default") -> Any:
        return self._get("/ssot/segments", {"dataspace": dataspace})

    def get_segment(self, segment_id: str, dataspace: str = "default") -> Any:
        return self._get(f"/ssot/segments/{segment_id}", {"dataspace": dataspace})

    def create_segment(self, body: Dict[str, Any]) -> Any:
        return self._post("/ssot/segments", body)

    def update_segment(self, segment_id: str, body: Dict[str, Any]) -> Any:
        return self._patch(f"/ssot/segments/{segment_id}", body)

    def delete_segment(self, segment_id: str) -> Any:
        return self._delete(f"/ssot/segments/{segment_id}")

    def publish_segment(self, segment_id: str) -> Any:
        """Publish a segment so it is available for activations."""
        return self._post(f"/ssot/segments/{segment_id}/actions/publish")

    def refresh_segment(self, segment_id: str) -> Any:
        """Trigger a segment membership recalculation."""
        return self._post(f"/ssot/segments/{segment_id}/actions/refresh")

    # ------------------------------------------------------------------
    # Identity Resolution Rulesets
    # ------------------------------------------------------------------

    def list_identity_resolution_rulesets(self, dataspace: str = "default") -> Any:
        return self._get("/ssot/identity-resolution-rulesets", {"dataspace": dataspace})

    def get_identity_resolution_ruleset(self, ruleset_id: str, dataspace: str = "default") -> Any:
        return self._get(f"/ssot/identity-resolution-rulesets/{ruleset_id}", {"dataspace": dataspace})

    def create_identity_resolution_ruleset(self, body: Dict[str, Any]) -> Any:
        return self._post("/ssot/identity-resolution-rulesets", body)

    def update_identity_resolution_ruleset(self, ruleset_id: str, body: Dict[str, Any]) -> Any:
        return self._patch(f"/ssot/identity-resolution-rulesets/{ruleset_id}", body)

    def delete_identity_resolution_ruleset(self, ruleset_id: str) -> Any:
        return self._delete(f"/ssot/identity-resolution-rulesets/{ruleset_id}")

    def run_identity_resolution(self, ruleset_id: str) -> Any:
        """Trigger an identity resolution run for the given ruleset."""
        return self._post(f"/ssot/identity-resolution-rulesets/{ruleset_id}/actions/run")

    # ------------------------------------------------------------------
    # Calculated Insights
    # ------------------------------------------------------------------

    def list_calculated_insights(self, dataspace: str = "default") -> Any:
        return self._get("/ssot/calculated-insights", {"dataspace": dataspace})

    def get_calculated_insight(self, ci_id: str, dataspace: str = "default") -> Any:
        return self._get(f"/ssot/calculated-insights/{ci_id}", {"dataspace": dataspace})

    def create_calculated_insight(self, body: Dict[str, Any]) -> Any:
        return self._post("/ssot/calculated-insights", body)

    def update_calculated_insight(self, ci_id: str, body: Dict[str, Any]) -> Any:
        return self._patch(f"/ssot/calculated-insights/{ci_id}", body)

    def delete_calculated_insight(self, ci_id: str) -> Any:
        return self._delete(f"/ssot/calculated-insights/{ci_id}")

    def refresh_calculated_insight(self, ci_id: str) -> Any:
        """Trigger a recalculation of a Calculated Insight."""
        return self._post(f"/ssot/calculated-insights/{ci_id}/actions/refresh")

    # ------------------------------------------------------------------
    # Activation Targets
    # ------------------------------------------------------------------

    def list_activation_targets(self, dataspace: str = "default") -> Any:
        return self._get("/ssot/activation-targets", {"dataspace": dataspace})

    def get_activation_target(self, target_id: str, dataspace: str = "default") -> Any:
        return self._get(f"/ssot/activation-targets/{target_id}", {"dataspace": dataspace})

    def create_activation_target(self, body: Dict[str, Any]) -> Any:
        return self._post("/ssot/activation-targets", body)

    def update_activation_target(self, target_id: str, body: Dict[str, Any]) -> Any:
        return self._patch(f"/ssot/activation-targets/{target_id}", body)

    def delete_activation_target(self, target_id: str) -> Any:
        return self._delete(f"/ssot/activation-targets/{target_id}")

    # ------------------------------------------------------------------
    # Activations
    # ------------------------------------------------------------------

    def list_activations(self, dataspace: str = "default") -> Any:
        return self._get("/ssot/activations", {"dataspace": dataspace})

    def get_activation(self, activation_id: str, dataspace: str = "default") -> Any:
        return self._get(f"/ssot/activations/{activation_id}", {"dataspace": dataspace})

    def create_activation(self, body: Dict[str, Any]) -> Any:
        return self._post("/ssot/activations", body)

    def update_activation(self, activation_id: str, body: Dict[str, Any]) -> Any:
        return self._patch(f"/ssot/activations/{activation_id}", body)

    def delete_activation(self, activation_id: str) -> Any:
        return self._delete(f"/ssot/activations/{activation_id}")

    def publish_activation(self, activation_id: str) -> Any:
        """Publish an activation to push segment data to the activation target."""
        return self._post(f"/ssot/activations/{activation_id}/actions/publish")
