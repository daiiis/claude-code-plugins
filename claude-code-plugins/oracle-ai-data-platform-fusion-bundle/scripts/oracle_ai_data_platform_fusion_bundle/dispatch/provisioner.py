"""Idempotent one-time AIDP tenant setup over REST.

Creates the three things a fresh tenant needs before
``aidp-fusion-bundle run --mode seed`` can land bronze data:

  1. **Credential** — the BICC password, stored in AIDP's credential
     store so the in-cluster notebook can read it via
     ``aidputils.secrets.get(...)`` without ever inlining the password
     into source.
  2. **Catalog** — an ``INTERNAL`` catalog (Delta-backed) for the
     bundle's bronze tables.
  3. **Bronze schema** — namespace inside that catalog. Silver / gold
     schemas are deferred to follow-up PRs (the bronze-end-to-end PR
     intentionally ships nothing else).

Each step is **idempotent**: a pre-check via the matching ``GET``
endpoint short-circuits the ``POST`` if the resource already exists by
display name. Re-running ``provision`` on a tenant that's already
set up is a no-op + a status table — never a 409 or duplicate.

All REST shapes are empirically-confirmed against the 20260430 API on
2026-05-23 — see ``project_aidp_rest_setup_patterns`` in memory for
the receipts.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from enum import Enum

import requests

from .rest_client import AidpRestClient, AidpRestError

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


class ProvisionOutcome(str, Enum):
    """Per-step outcome surfaced to the CLI."""

    CREATED = "created"
    """Resource didn't exist; POSTed and accepted by the API (200/201/202)."""

    EXISTS = "exists"
    """Pre-check found a matching displayName; POST skipped."""

    FAILED = "failed"
    """API rejected the create call. ``message`` carries the response body."""


@dataclass(frozen=True)
class ProvisionStep:
    """One row in the provisioning report."""

    name: str  # human-readable step label
    outcome: ProvisionOutcome
    message: str = ""  # populated for FAILED + EXISTS (the existing resource's key)


@dataclass(frozen=True)
class ProvisionReport:
    """Aggregate result of a single :func:`provision` invocation."""

    steps: list[ProvisionStep]

    @property
    def all_ok(self) -> bool:
        return all(s.outcome is not ProvisionOutcome.FAILED for s in self.steps)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def provision(
    *,
    client: AidpRestClient,
    secret_name: str,
    secret_key: str,
    secret_value: str,
    catalog_name: str,
    bronze_schema: str,
) -> ProvisionReport:
    """Provision credential + catalog + bronze schema, idempotent.

    Args:
        client: A configured :class:`AidpRestClient` (region + aidp_id +
            oci_profile must be valid; ``workspace_key`` is unused by
            these endpoints, which all live at the AIDP-platform level).
        secret_name: Credential displayName (default
            ``fusion_bicc_password``).
        secret_key: Key inside the credential's secretTokenPair
            (default ``password``).
        secret_value: The BICC password to store.
        catalog_name: Catalog displayName (default ``fusion_catalog``).
        bronze_schema: Schema displayName inside the catalog
            (default ``bronze``).

    Returns:
        :class:`ProvisionReport` with one step per resource provisioned.
        Inspect ``report.all_ok`` to gate downstream commands; iterate
        ``report.steps`` to render a user-facing status table.
    """
    steps: list[ProvisionStep] = []

    # 1. Credential
    steps.append(_provision_credential(client, secret_name, secret_key, secret_value))

    # 2. Catalog
    cat_step, catalog_key = _provision_catalog(client, catalog_name)
    steps.append(cat_step)

    # 3. Schema — needs the catalog's KEY (UUID) for the list-by-catalog
    #    query, so we always look it up fresh post-provision rather than
    #    relying on whatever key shape the POST returned.
    if cat_step.outcome is ProvisionOutcome.FAILED or catalog_key is None:
        steps.append(ProvisionStep(
            name=f"schema {bronze_schema!r} in {catalog_name!r}",
            outcome=ProvisionOutcome.FAILED,
            message="upstream catalog provisioning failed or key unresolved",
        ))
    else:
        steps.append(_provision_schema(client, catalog_name, catalog_key, bronze_schema))

    return ProvisionReport(steps=steps)


# ---------------------------------------------------------------------------
# Individual provisioning steps
# ---------------------------------------------------------------------------


def _provision_credential(
    client: AidpRestClient, name: str, key: str, value: str,
) -> ProvisionStep:
    """POST /credentials — idempotent by displayName."""
    label = f"credential {name!r}"
    base = f"{client.datalake_base}/credentials"

    # Pre-check
    try:
        r = client._request("GET", base)
        body = client._ok(r, context="list_credentials")
    except AidpRestError as exc:
        return ProvisionStep(label, ProvisionOutcome.FAILED, f"list: {exc}")
    existing = _find_by_display_name(body, name)
    if existing is not None:
        return ProvisionStep(label, ProvisionOutcome.EXISTS, f"key={existing}")

    # Create
    payload = {
        "displayName": name,
        "type": "SECRET_TOKEN",
        "credentialDetails": {
            "credentialType": "SECRET_TOKEN",
            # The API requires this nested-array shape with
            # ``secretKey`` / ``secretValue`` (NOT ``key`` / ``value``)
            # — verified empirically by probing field-name candidates.
            "secretTokenPair": [
                {"secretKey": key, "secretValue": value},
            ],
        },
    }
    try:
        r = client._request("POST", base, json_body=payload)
    except Exception as exc:  # noqa: BLE001
        return ProvisionStep(label, ProvisionOutcome.FAILED, f"network: {exc}")
    if r.status_code in (200, 201, 202):
        return ProvisionStep(label, ProvisionOutcome.CREATED)
    return ProvisionStep(label, ProvisionOutcome.FAILED, f"HTTP {r.status_code}: {r.text[:200]}")


def _provision_catalog(
    client: AidpRestClient, name: str,
) -> tuple[ProvisionStep, str | None]:
    """POST /catalogs — idempotent by displayName, type=INTERNAL.

    Returns (step, catalog_key). The key is needed for the downstream
    schema-list query (POST /schemas requires ``catalogKey`` as a query
    param). When the catalog was already there, the key comes from the
    pre-check; when freshly created, we re-list to pick up the new key.
    """
    label = f"catalog {name!r}"
    base = f"{client.datalake_base}/catalogs"

    try:
        r = client._request("GET", base)
        body = client._ok(r, context="list_catalogs")
    except AidpRestError as exc:
        return ProvisionStep(label, ProvisionOutcome.FAILED, f"list: {exc}"), None
    existing_key = _find_by_display_name(body, name)
    if existing_key is not None:
        return ProvisionStep(label, ProvisionOutcome.EXISTS, f"key={existing_key}"), existing_key

    payload = {"displayName": name, "catalogType": "INTERNAL"}
    try:
        r = client._request("POST", base, json_body=payload)
    except Exception as exc:  # noqa: BLE001
        return ProvisionStep(label, ProvisionOutcome.FAILED, f"network: {exc}"), None
    if r.status_code not in (200, 201, 202):
        return ProvisionStep(label, ProvisionOutcome.FAILED, f"HTTP {r.status_code}: {r.text[:200]}"), None

    # 202 = async accept; poll the list endpoint briefly for the new key.
    new_key: str | None = None
    for _ in range(6):  # ~30s budget
        try:
            r = client._request("GET", base)
            new_key = _find_by_display_name(client._ok(r), name)
        except AidpRestError:
            new_key = None
        if new_key is not None:
            break
        import time as _t
        _t.sleep(5)
    return ProvisionStep(label, ProvisionOutcome.CREATED, f"key={new_key or '(pending)'}"), new_key


def _provision_schema(
    client: AidpRestClient, catalog_name: str, catalog_key: str, schema_name: str,
) -> ProvisionStep:
    """POST /schemas — idempotent by (catalogKey, displayName).

    The GET requires ``?catalogKey=<key>`` as a query param (empirically
    confirmed 2026-05-23: a bare GET returns HTTP 400
    ``InvalidParameter: query param catalogKey must not be null``).
    The POST body still uses ``catalogName``, not the key.
    """
    label = f"schema {schema_name!r} in {catalog_name!r}"
    base = f"{client.datalake_base}/schemas"

    try:
        r = client._request("GET", f"{base}?catalogKey={catalog_key}")
        body = client._ok(r, context="list_schemas")
    except AidpRestError as exc:
        return ProvisionStep(label, ProvisionOutcome.FAILED, f"list: {exc}")

    existing_key = _find_by_display_name(body, schema_name)
    if existing_key is not None:
        return ProvisionStep(label, ProvisionOutcome.EXISTS, f"key={existing_key}")

    payload = {"displayName": schema_name, "catalogName": catalog_name}
    try:
        r = client._request("POST", base, json_body=payload)
    except Exception as exc:  # noqa: BLE001
        return ProvisionStep(label, ProvisionOutcome.FAILED, f"network: {exc}")
    if r.status_code in (200, 201, 202):
        return ProvisionStep(label, ProvisionOutcome.CREATED)
    return ProvisionStep(label, ProvisionOutcome.FAILED, f"HTTP {r.status_code}: {r.text[:200]}")


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _find_by_display_name(body: dict | list, name: str) -> str | None:
    """Return the ``key`` of the resource whose ``displayName == name``,
    or None if no match. Handles both ``{"items": [...]}`` and bare-list
    response shapes seen across AIDP endpoints."""
    items = body.get("items", body) if isinstance(body, dict) else body
    if not isinstance(items, list):
        return None
    for it in items:
        if isinstance(it, dict) and it.get("displayName") == name:
            return it.get("key")
    return None


__all__ = ["ProvisionOutcome", "ProvisionReport", "ProvisionStep", "provision"]
