"""Implementation of ``aidp-fusion-bundle provision``.

One-time, idempotent tenant setup so the customer can go from a fresh
AIDP instance to ``aidp-fusion-bundle run --mode seed`` with zero
clicks in the AIDP UI. Creates:

  1. the BICC password as an entry in the AIDP credential store
     (named by ``env.secret.name`` / ``env.secret.key``);
  2. the bundle's ``aidp.catalog`` as an INTERNAL Delta catalog;
  3. the bundle's ``aidp.bronzeSchema`` as a namespace inside that
     catalog.

Re-running the command on a tenant that's already set up is a no-op
plus a status table — every step pre-checks via the matching GET
endpoint and short-circuits on displayName match.

Silver/gold schemas are intentionally NOT provisioned in this PR —
the bronze-end-to-end scope stops here.
"""

from __future__ import annotations

import os
from pathlib import Path

import yaml
from pydantic import ValidationError
from rich.console import Console
from rich.table import Table

from ..dispatch import AidpRestClient, AidpRestError, ProvisionOutcome, provision as provision_rest
from ..schema.bundle import AidpConfig, Bundle
from ..schema.refs import render_tree


def provision(
    bundle_path: Path,
    config_path: Path,
    env_name: str,
    *,
    console: Console | None = None,
) -> int:
    """Provision the AIDP-side resources the bundle's bronze layer needs.

    Returns process exit code: 0 if every step is created-or-exists,
    1 if any step hard-failed.
    """
    console = console or Console()

    bundle, config = _load_inputs(bundle_path, config_path, console)
    if bundle is None or config is None:
        return 2
    env = config.environments.get(env_name)
    if env is None:
        console.print(
            f"[red]env '{env_name}' not in aidp.config.yaml[/red] — "
            f"available: {sorted(config.environments.keys())}"
        )
        return 2

    # Validate the dispatcher-required fields up-front. ``aidp_id`` is
    # the only hard prerequisite for provisioning — workspace/cluster
    # come into play at ``run`` time.
    if not env.aidp_id:
        console.print(
            "[red]aidp.config.yaml is missing[/red] [cyan]environments.<env>.aidpId[/cyan].\n"
            "Find it in OCI Console -> Analytics & AI -> AI Data Platform -> your instance -> OCID."
        )
        return 2

    # Pull the BICC password from env (set by the operator via .env).
    # We accept a literal here too — useful in CI where the value comes
    # from a vault upstream and is exported into the shell.
    password = _resolve_bundle_password(bundle.fusion.password)
    if password is None:
        console.print(
            "[red]bundle.fusion.password could not be resolved[/red] — "
            "set FUSION_BICC_PASSWORD in your .env (or in the calling shell)."
        )
        return 2

    secret_name = env.secret.name if env.secret else "fusion_bicc_password"
    secret_key = env.secret.key if env.secret else "password"

    region = env.region or config.defaults.region
    client = AidpRestClient(
        region=region,
        aidp_id=env.aidp_id,
        workspace_key="",  # control-plane endpoints don't scope to a workspace
        oci_profile=env.oci_profile or "DEFAULT",
    )

    console.print(
        f"[bold]Provisioning AIDP setup[/bold] for project "
        f"[cyan]{bundle.project}[/cyan] (env=[cyan]{env_name}[/cyan]):"
    )
    try:
        report = provision_rest(
            client=client,
            secret_name=secret_name,
            secret_key=secret_key,
            secret_value=password,
            catalog_name=bundle.aidp.catalog,
            bronze_schema=bundle.aidp.bronze_schema,
        )
    except AidpRestError as exc:
        console.print(f"[red]provisioning aborted:[/red] {exc}")
        return 2

    _render(report, console)
    return 0 if report.all_ok else 1


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_inputs(
    bundle_path: Path, config_path: Path, console: Console,
) -> tuple[Bundle | None, AidpConfig | None]:
    if not bundle_path.exists():
        console.print(f"[red]bundle.yaml not found:[/red] {bundle_path}")
        return None, None
    if not config_path.exists():
        console.print(f"[red]aidp.config.yaml not found:[/red] {config_path}")
        return None, None
    try:
        bundle = Bundle.model_validate(render_tree(yaml.safe_load(bundle_path.read_text(encoding="utf-8"))))
    except (ValidationError, yaml.YAMLError) as exc:
        console.print(f"[red]bundle.yaml schema error:[/red] {str(exc).splitlines()[0]}")
        return None, None
    try:
        config = AidpConfig.model_validate(render_tree(yaml.safe_load(config_path.read_text(encoding="utf-8"))))
    except (ValidationError, yaml.YAMLError) as exc:
        console.print(f"[red]aidp.config.yaml schema error:[/red] {str(exc).splitlines()[0]}")
        return None, None
    return bundle, config


def _resolve_bundle_password(value: str) -> str | None:
    """Resolve ``${VAR}`` from os.environ; literal otherwise. We only
    need the cleartext password locally so we can register it as an
    AIDP credential — after that, the cluster reads it from the
    credential store via ``aidputils.secrets``."""
    if value.startswith("${") and value.endswith("}"):
        var = value[2:-1]
        if var.startswith("vault:"):
            # We don't resolve vault refs in this command. Operator must
            # have FUSION_BICC_PASSWORD set in the environment directly
            # (or hand-write the literal in bundle.yaml for first-time
            # provisioning).
            return os.environ.get("FUSION_BICC_PASSWORD")
        return os.environ.get(var)
    return value


def _render(report, console: Console) -> None:
    table = Table(title="Provisioning report")
    table.add_column("step")
    table.add_column("outcome")
    table.add_column("note", overflow="fold")
    for step in report.steps:
        style = {
            ProvisionOutcome.CREATED: "green",
            ProvisionOutcome.EXISTS: "cyan",
            ProvisionOutcome.FAILED: "red",
        }[step.outcome]
        table.add_row(step.name, f"[{style}]{step.outcome.value}[/{style}]", step.message)
    console.print(table)
    if report.all_ok:
        console.print("\n[green]Provisioning complete.[/green] You can now run [cyan]aidp-fusion-bundle run --mode seed[/cyan].")
    else:
        console.print("\n[red]Provisioning had failures — fix and re-run.[/red]")


__all__ = ["provision"]
