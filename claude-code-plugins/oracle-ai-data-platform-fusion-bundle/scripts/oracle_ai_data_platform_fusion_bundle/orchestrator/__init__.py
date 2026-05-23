"""Bronze-only orchestrator — invokes inside an AIDP notebook session.

Scope (intentionally narrow for the bronze-end-to-end PR):

  * Iterates ``bundle.datasets[]`` in order.
  * Calls :func:`extractors.bicc.extract_pvo` for each enabled dataset.
  * Enriches the Spark DataFrame with the four audit columns required by
    the medallion contract (``_extract_ts``, ``_source_pvo``, ``_run_id``,
    ``_watermark_used``).
  * Writes Delta to ``<catalog>.<bronze_schema>.<table>`` with
    ``CREATE OR REPLACE TABLE`` semantics (mode=seed only — incremental
    mode is a follow-up PR with ``MERGE INTO``).
  * Records one ``fusion_bundle_state`` row per dataset so SOX auditors
    can join ``_run_id`` from any bronze row back to the orchestrator
    run that produced it.
  * Stops on first failure (subsequent datasets get ``status='skipped',
    skip_reason='aborted'``) — Spark errors are usually environmental
    (cluster OOM, schema mismatch) and pressing through them wastes the
    next 20-30 minutes per dataset on a tenant that's already broken.

What this module *deliberately* does NOT do (deferred):

  * Silver / Gold layers — bundle.yaml should not list any
    ``dimensions:`` or ``gold:`` block; if it does, this orchestrator
    ignores it.
  * DAG resolution + dependency analysis — bronze datasets are
    independent, so the loop is linear.
  * Retry on transient errors — a single ``saveAsTable`` flake fails the
    step. Operator re-runs the whole bundle (cheap for bronze).
  * ``${vault:OCID}`` password resolution — we accept literal strings
    and ``${VAR}`` placeholders only. The dispatch path sets
    ``FUSION_BICC_PASSWORD`` from the AIDP credential store via
    ``aidputils.secrets``, so vault-OCID indirection isn't needed here.

The dispatch path (``dispatch/runner.py``) generates a notebook that
imports this module and calls :func:`run` with the AIDP-injected
``spark`` global. Operators running by hand from an AIDP notebook do the
same.
"""

from __future__ import annotations

import logging
import os
import re
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from oracle_ai_data_platform_fusion_bundle.config.paths import TablePaths
from oracle_ai_data_platform_fusion_bundle.extractors import bicc
from oracle_ai_data_platform_fusion_bundle.schema.bundle import Bundle
from oracle_ai_data_platform_fusion_bundle.schema.fusion_catalog import CATALOG
from oracle_ai_data_platform_fusion_bundle.schema.refs import render_tree

if TYPE_CHECKING:  # pragma: no cover
    from pyspark.sql import DataFrame, SparkSession

logger = logging.getLogger(__name__)

_ENV_PLACEHOLDER = re.compile(r"^\$\{([A-Za-z_][A-Za-z0-9_]*)\}$")

STATE_TABLE_NAME = "fusion_bundle_state"


# ---------------------------------------------------------------------------
# Result dataclasses
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class RunStep:
    """One dataset's outcome inside a single :func:`run` call."""

    dataset_id: str
    layer: str  # always "bronze" in this PR
    status: str  # "success" | "failed" | "skipped"
    row_count: int | None = None
    duration_seconds: float = 0.0
    skip_reason: str | None = None  # "aborted" only (no silver-cascade yet)
    error_message: str | None = None


@dataclass(frozen=True)
class RunSummary:
    """Aggregate result of one :func:`run` invocation. Matches the shape
    that ``dispatch/notebook_builder.py`` serialises into the
    base64-wrapped marker payload."""

    run_id: str
    bundle_project: str
    mode: str
    succeeded: int
    failed: int
    skipped: int
    deferred: int = 0  # always 0 until silver/gold land
    total_duration_seconds: float = 0.0
    steps: list[RunStep] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------


def run(
    *,
    bundle_path: Path,
    spark: "SparkSession",
    mode: str = "seed",
    datasets: list[str] | None = None,
    layers: list[str] | None = None,  # accepted for forward-compat, ignored
    dry_run: bool = False,
) -> RunSummary:
    """Run all enabled bronze extracts from ``bundle.yaml``.

    Args:
        bundle_path: Path to ``bundle.yaml`` (typically written to the
            cluster filesystem by the dispatch notebook's creds cell).
        spark: AIDP-injected SparkSession.
        mode: ``seed`` is the only supported value today.
        datasets: Optional whitelist of ``DatasetSpec.id`` to filter.
            ``None`` = every enabled dataset in bundle.yaml.
        layers: Forward-compatibility shim (ignored - bronze-only).
        dry_run: If True, print the resolved plan and exit without
            calling Spark. Useful for sanity-checking the dispatch
            wiring without burning a BICC extract.

    Returns:
        :class:`RunSummary` with one :class:`RunStep` per dataset
        considered. Operators serialise this back to the laptop via the
        ``AIDP_LIVE_TEST_RESULT_BEGIN`` marker.
    """
    if mode != "seed":
        raise NotImplementedError(
            f"mode={mode!r} not supported in the bronze-only orchestrator. "
            "Today only 'seed' (CREATE OR REPLACE) is implemented; "
            "'incremental' (MERGE INTO) is a follow-up PR."
        )

    bundle = _load_bundle(bundle_path)
    paths = _paths_from_bundle(bundle)

    run_id = str(uuid.uuid4())
    extract_ts = datetime.now(timezone.utc).isoformat()

    enabled = [d for d in bundle.datasets if d.enabled]
    if datasets is not None:
        wanted = set(datasets)
        enabled = [d for d in enabled if d.id in wanted]

    if dry_run:
        plan = [d.id for d in enabled]
        logger.info(f"[dry-run] would extract {len(plan)} bronze datasets: {plan}")
        return RunSummary(
            run_id=run_id, bundle_project=bundle.project, mode=mode,
            succeeded=0, failed=0, skipped=0,
            steps=[RunStep(d.id, "bronze", "skipped", skip_reason="dry_run") for d in enabled],
        )

    _ensure_state_table(spark, paths)

    password = _resolve_password(bundle.fusion.password)

    steps: list[RunStep] = []
    halted = False
    t_overall = time.perf_counter()

    for dataset in enabled:
        if halted:
            step = RunStep(dataset.id, "bronze", "skipped", skip_reason="aborted")
            steps.append(step)
            _record_state(spark, paths, run_id, step)
            continue

        step = _run_one_bronze(
            spark=spark, paths=paths, bundle=bundle, dataset_id=dataset.id,
            password=password, run_id=run_id, extract_ts=extract_ts,
        )
        steps.append(step)
        _record_state(spark, paths, run_id, step)

        if step.status == "failed":
            halted = True  # stop-on-first-fail

    succeeded = sum(1 for s in steps if s.status == "success")
    failed = sum(1 for s in steps if s.status == "failed")
    skipped = sum(1 for s in steps if s.status == "skipped")
    total_duration = sum(s.duration_seconds for s in steps)

    summary = RunSummary(
        run_id=run_id,
        bundle_project=bundle.project,
        mode=mode,
        succeeded=succeeded,
        failed=failed,
        skipped=skipped,
        total_duration_seconds=total_duration,
        steps=steps,
    )
    logger.info(
        f"orchestrator.run finished -- run_id={run_id} "
        f"{succeeded} ok / {failed} failed / {skipped} skipped "
        f"({total_duration:.1f}s, wall={time.perf_counter() - t_overall:.1f}s)"
    )
    return summary


# ---------------------------------------------------------------------------
# Per-dataset bronze extract
# ---------------------------------------------------------------------------


def _run_one_bronze(
    *,
    spark: "SparkSession",
    paths: TablePaths,
    bundle: Bundle,
    dataset_id: str,
    password: str,
    run_id: str,
    extract_ts: str,
) -> RunStep:
    """Extract ONE PVO -> bronze Delta table. Catches any exception and
    surfaces it as ``RunStep.failed`` -- the run loop interprets that as
    'halt the rest of the bundle'."""
    t0 = time.perf_counter()
    try:
        pvo = CATALOG.get(dataset_id)
        if pvo is None:
            raise KeyError(
                f"dataset_id={dataset_id!r} not in fusion_catalog -- "
                f"add it to bundle.yaml only if it's a curated PVO"
            )

        df = bicc.extract_pvo(
            spark, pvo,
            fusion_service_url=bundle.fusion.service_url,
            username=bundle.fusion.username,
            password=password,
            fusion_external_storage=bundle.fusion.external_storage,
        )
        df = _enrich_audit_cols(df, source_pvo=pvo.datastore, run_id=run_id, extract_ts=extract_ts)

        # The canonical catalog hardcodes ``pvo.bronze_table`` as a 3-part
        # name (``catalog.schema.table``) per the historical
        # saasfademo1-centric design. We extract just the table segment
        # and re-compose via TablePaths so bundle.yaml's ``aidp.catalog``
        # / ``aidp.bronzeSchema`` overrides are actually honored (the
        # plugin must run on any tenant per the CLAUDE.md portability
        # mission). Future PR can drop the 3-part field once every
        # consumer flows through ``paths.bronze(...)``.
        table_name_only = pvo.bronze_table.rsplit(".", 1)[-1]
        target = paths.bronze(table_name_only)
        (
            df.write.format("delta")
            .mode("overwrite")
            .option("overwriteSchema", "true")  # tolerate audit-col schema additions across re-runs
            .saveAsTable(target)
        )
        row_count = spark.table(target).count()
        return RunStep(
            dataset_id=dataset_id,
            layer="bronze",
            status="success",
            row_count=row_count,
            duration_seconds=time.perf_counter() - t0,
        )
    except Exception as exc:  # noqa: BLE001 -- broad catch by design (Spark + Py4J + KeyError)
        logger.warning(f"bronze.{dataset_id} failed: {exc!r}")
        return RunStep(
            dataset_id=dataset_id,
            layer="bronze",
            status="failed",
            duration_seconds=time.perf_counter() - t0,
            error_message=repr(exc)[:1000],  # truncate -- full traceback lives in the executed notebook
        )


def _enrich_audit_cols(
    df: "DataFrame", *, source_pvo: str, run_id: str, extract_ts: str,
) -> "DataFrame":
    """Add the four audit columns required by the medallion contract."""
    from pyspark.sql.functions import lit  # local -- pyspark isn't on the laptop

    return (
        df.withColumn("_extract_ts", lit(extract_ts))
        .withColumn("_source_pvo", lit(source_pvo))
        .withColumn("_run_id", lit(run_id))
        .withColumn("_watermark_used", lit(None).cast("string"))
    )


# ---------------------------------------------------------------------------
# State table
# ---------------------------------------------------------------------------


def _state_table_path(paths: TablePaths) -> str:
    return paths.bronze(STATE_TABLE_NAME)


def _ensure_state_table(spark: "SparkSession", paths: TablePaths) -> None:
    """Create ``<bronze_schema>.fusion_bundle_state`` if missing.

    Idempotent. Run on every :func:`run` call so the schema is whatever
    the latest plugin version expects -- schema migrations would land
    here when they're needed.
    """
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {_state_table_path(paths)} (
            run_id           STRING,
            dataset_id       STRING,
            layer            STRING,
            status           STRING,
            row_count        BIGINT,
            duration_seconds DOUBLE,
            skip_reason      STRING,
            error_message    STRING,
            last_run_at      TIMESTAMP
        ) USING delta
        """
    )


def _record_state(
    spark: "SparkSession", paths: TablePaths, run_id: str, step: RunStep,
) -> None:
    """INSERT one row into ``fusion_bundle_state``. Swallows write
    failures with a WARN -- losing an audit row shouldn't kill the run
    (state-table writes can transiently fail under Delta contention,
    and the executed notebook still carries the marker payload as a
    backup audit channel)."""
    try:
        spark.sql(
            f"""
            INSERT INTO {_state_table_path(paths)}
            VALUES (
                '{run_id}',
                '{step.dataset_id}',
                '{step.layer}',
                '{step.status}',
                {step.row_count if step.row_count is not None else 'NULL'},
                {step.duration_seconds},
                {('NULL' if step.skip_reason is None else _sql_literal(step.skip_reason))},
                {('NULL' if step.error_message is None else _sql_literal(step.error_message))},
                current_timestamp()
            )
            """
        )
    except Exception as exc:  # noqa: BLE001
        logger.warning(f"state-row write failed for {step.dataset_id}: {exc!r}")


def _sql_literal(value: str) -> str:
    """Escape single-quotes for inline SQL VALUES literals.

    We INSERT via SQL text (not the DataFrame API) because the orchestrator
    runs inside an AIDP notebook session and the SparkSession's
    ``createDataFrame`` path has occasionally surfaced classpath-mismatch
    issues -- a one-row INSERT via raw SQL avoids that surface entirely.
    """
    return "'" + value.replace("'", "''") + "'"


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _load_bundle(bundle_path: Path) -> Bundle:
    # Lazy yaml import — orchestrator module-load must survive an isolated
    # ``pip install --no-deps --target`` (the AIDP cluster's default
    # sys.path does NOT shadow our install dir, so a top-level ``import
    # yaml`` would crash before the cluster's site-packages get a chance).
    import yaml

    raw = render_tree(yaml.safe_load(bundle_path.read_text(encoding="utf-8")))
    return Bundle.model_validate(raw)


def _paths_from_bundle(bundle: Bundle) -> TablePaths:
    return TablePaths(
        catalog=bundle.aidp.catalog,
        bronze_schema=bundle.aidp.bronze_schema,
        silver_schema=bundle.aidp.silver_schema,
        gold_schema=bundle.aidp.gold_schema,
    )


def _resolve_password(value: str) -> str:
    """Resolve the BICC password from ``bundle.fusion.password``.

    Accepted shapes:
      * ``${VAR}`` -- looks up ``os.environ[VAR]``. Used by the dispatch
        path: the creds cell sets ``FUSION_BICC_PASSWORD`` from the AIDP
        credential store, then this resolver pulls it back out.
      * Literal string -- returned as-is. Used by tests + by operators
        who have explicitly inlined the password.

    ``${vault:OCID}`` is intentionally NOT supported in this PR -- the
    canonical path is the credential store via ``aidputils.secrets``.
    """
    m = _ENV_PLACEHOLDER.match(value)
    if m is None:
        return value
    var = m.group(1)
    if var not in os.environ:
        raise KeyError(
            f"bundle.fusion.password references ${{{var}}} but env var "
            f"is unset. The dispatch path sets FUSION_BICC_PASSWORD from "
            f"the AIDP credential store before importing the orchestrator; "
            f"check that the secret named in env.secret.name exists."
        )
    return os.environ[var]


__all__ = ["RunStep", "RunSummary", "run"]
