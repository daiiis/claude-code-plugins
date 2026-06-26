"""`init` scaffold must ship inside the package, not only in the repo.

Regression for the customer-found bug: ``pip install`` (non-editable) built a
wheel WITHOUT the repo-root ``examples/`` tree, so ``aidp-fusion-bundle init``
— the first quickstart command — crashed with ``FileNotFoundError: examples
directory not found`` for every real customer. The editable-install test
suite never caught it because ``examples/`` is present on disk in a dev
checkout.

Two guards:

* :class:`TestScaffoldShipsAndMatchesRepo` (fast) — every template ``init``
  references resolves from package data (``_scaffold/``) AND is byte-identical
  to its repo ``examples/`` source, so the bundled copy can't silently drift.
* :class:`TestWheelInstallInitWorks` (slow, opt-in) — builds a real wheel,
  installs it into a throwaway venv, and runs ``init`` there. This is the
  customer path end-to-end; it would have caught the original bug directly.
"""

from __future__ import annotations

import subprocess
import sys
import venv
from importlib import resources
from pathlib import Path

import pytest

from oracle_ai_data_platform_fusion_bundle.commands.init import TEMPLATES

# Repo root = three parents up from the installed module
# (.../scripts/oracle_ai_data_platform_fusion_bundle/commands/init.py -> repo).
REPO_ROOT = Path(__file__).resolve().parents[2]
EXAMPLES = REPO_ROOT / "examples"


def _scaffold_relpaths() -> list[str]:
    """Every scaffold file ``init`` can copy: the two files per template plus
    the ``.env.example`` it always scaffolds."""
    rels: set[str] = {".env.example"}
    for bundle_src, config_src in TEMPLATES.values():
        rels.add(bundle_src)
        rels.add(config_src)
    return sorted(rels)


def _repo_source(relpath: str) -> Path:
    """The repo-side source of a scaffold file (the dev-fallback location)."""
    if relpath == ".env.example":
        return REPO_ROOT / ".env.example"
    return EXAMPLES / relpath


class TestScaffoldShipsAndMatchesRepo:
    @pytest.mark.parametrize("relpath", _scaffold_relpaths())
    def test_template_is_packaged(self, relpath: str) -> None:
        """The file is present in package data under ``_scaffold/`` — i.e. it
        ships in the wheel, so ``init`` works for a ``pip install`` customer."""
        res = resources.files(
            "oracle_ai_data_platform_fusion_bundle"
        ).joinpath("_scaffold", relpath)
        assert res.is_file(), (
            f"{relpath!r} is referenced by init's TEMPLATES but is NOT shipped "
            f"in package data (_scaffold/). A pip-installed customer's `init` "
            f"would crash. Add it under _scaffold/ and to pyproject.toml's "
            f"[tool.setuptools.package-data]."
        )

    @pytest.mark.parametrize("relpath", _scaffold_relpaths())
    def test_packaged_matches_repo_source(self, relpath: str) -> None:
        """The packaged copy is byte-identical to the repo ``examples/`` /
        ``.env.example`` source, so the two can't drift unnoticed."""
        repo_src = _repo_source(relpath)
        assert repo_src.is_file(), (
            f"repo source {repo_src} for scaffold {relpath!r} is missing"
        )
        packaged = resources.files(
            "oracle_ai_data_platform_fusion_bundle"
        ).joinpath("_scaffold", relpath)
        assert packaged.read_bytes() == repo_src.read_bytes(), (
            f"packaged _scaffold/{relpath} has drifted from examples/{relpath}. "
            f"Re-sync the _scaffold copy when you edit the repo example."
        )


class TestWheelInstallInitWorks:
    """End-to-end customer path: build a wheel, install it clean, run init.

    Slow (~30-60s: wheel build + venv + install). Gated on the `build`
    backend; skipped when unavailable so it never errors on a minimal box.
    """

    @pytest.mark.timeout(600)
    def test_pip_installed_init_succeeds(self, tmp_path: Path) -> None:
        pytest.importorskip("build")

        # Build a wheel from the repo into a temp dir.
        wheel_out = tmp_path / "wheels"
        proc = subprocess.run(
            [sys.executable, "-m", "build", "--wheel", "--outdir", str(wheel_out)],
            cwd=str(REPO_ROOT),
            capture_output=True,
            text=True,
        )
        assert proc.returncode == 0, f"wheel build failed:\n{proc.stderr}"
        wheels = list(wheel_out.glob("*.whl"))
        assert len(wheels) == 1, f"expected one wheel, got {wheels}"

        # Fresh venv, install the wheel (no editable, no repo on path).
        venv_dir = tmp_path / "venv"
        venv.create(venv_dir, with_pip=True)
        pip = venv_dir / ("Scripts" if sys.platform == "win32" else "bin") / "pip"
        cli = venv_dir / ("Scripts" if sys.platform == "win32" else "bin") / "aidp-fusion-bundle"
        install = subprocess.run(
            [str(pip), "install", str(wheels[0])],
            capture_output=True, text=True,
        )
        assert install.returncode == 0, f"wheel install failed:\n{install.stderr}"

        # Run `init` in an isolated dir — this is the command that crashed.
        proj = tmp_path / "proj"
        proj.mkdir()
        result = subprocess.run(
            [str(cli), "init"],
            cwd=str(proj), capture_output=True, text=True,
        )
        assert result.returncode == 0, (
            f"`init` failed for a pip-installed customer (the original bug):\n"
            f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
        )
        assert (proj / "bundle.yaml").is_file()
        assert (proj / "aidp.config.yaml").is_file()
