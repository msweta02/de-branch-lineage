"""Which code ran on the date under investigation.

A trace is only honest if it parses the SQL that *actually ran* on the date in
question. A pipeline fixed last week must still be diagnosable as it behaved
last month, and today's working tree cannot answer that.

**The hybrid approach** (chosen after the Task 0 spike; see docs/BUILD_LOG.md,
Phase 1 "Task 0"). Airflow 3.3 can do this natively: a git-backed DAG bundle
records a commit SHA on every DagRun, and instantiating the bundle at that SHA
materialises the repository as it was. The spike proved retrieval works and
that ``include/sql/`` is inside the bundle. But serving DAGs from a git bundle
means a code change is not live until it is committed and pushed, which is a
material cost mid-project.

So the pipeline keeps running from the local ``dags-folder`` bundle, and the
code version is resolved in three steps, most authoritative first:

1. ``DagRun.bundle_version`` -- populated by Airflow itself when a versioning
   bundle is configured. Nothing to record, nothing to trust but Airflow.
2. ``DagRun.conf['code_version']`` -- supplied by whoever triggered the run.
   Weaker than (1) because it is an assertion rather than an observation, but it
   needs no change to how DAGs are served.
3. an XCom under the key ``code_version``, pushed by every pipeline task. This
   is the self-recorded pin that actually works on Airflow 3.3: a task *cannot*
   write to its own run's ``conf`` -- execution goes through the API server and
   conf is trigger-time only -- so the recording half of the hybrid lands in
   XCom instead. See ``include/pipeline_dag.py::_record_code_version``.
4. the working tree, with the resolution recorded as ``working-tree`` so a
   trace never silently claims a provenance it does not have.

Whatever the source, the SQL is materialised with ``git archive`` rather than
``git worktree add``: archive writes nothing into ``.git``, so a diagnosis can
never mutate the repository it is reading.
"""

from __future__ import annotations

import subprocess
import tempfile
from dataclasses import dataclass
from pathlib import Path

from branchlin.sources.rawfiles import DEFAULT_SQL_ROOT, RawFilesSource

#: Key under which a run records its own commit, for resolution step 2.
CONF_KEY = "code_version"

#: Repository root, inferred from this file's location.
REPO_ROOT = Path(__file__).resolve().parents[2]

#: Path within the repository holding the pipeline SQL.
SQL_SUBDIR = "include/sql"

# Provenance values, in descending order of authority.
BUNDLE = "airflow-bundle-version"
RUN_CONF = "run-conf"
RUN_XCOM = "run-xcom"
RUN_XCOM_LATEST = "run-xcom (latest run, not date-matched)"
WORKING_TREE = "working-tree"
EXPLICIT = "explicit"


class PinningError(RuntimeError):
    """The requested code version could not be materialised."""


@dataclass(frozen=True)
class PinnedSource:
    """A SQL source, plus an honest account of where the SQL came from."""

    source: RawFilesSource
    #: Commit SHA, or None when reading the working tree.
    version: str | None
    #: One of BUNDLE / RUN_CONF / WORKING_TREE / EXPLICIT.
    provenance: str
    #: Where the SQL was materialised, for the audit trail.
    root: Path

    @property
    def is_pinned(self) -> bool:
        return self.version is not None

    def describe(self) -> str:
        if not self.is_pinned:
            return f"working tree at {self.root} (UNPINNED -- not the code that ran)"
        return f"{self.version[:12]} via {self.provenance}"


def _git_argv(repo: Path, args: tuple[str, ...]) -> tuple[str, ...]:
    """Build a git command that works on a repository we do not own.

    The Airflow containers run as uid 50000 while the bind-mounted repository is
    owned by the host user (uid 1000), and git refuses to operate on a
    repository owned by someone else: ``fatal: detected dubious ownership``.
    Same root cause as Surprise 10, different symptom.

    Fixed with a per-invocation ``-c safe.directory=<repo>`` rather than global
    git config in the image: it is scoped to this one subprocess, names one
    exact path instead of ``*``, and keeps the fix next to the code that needs
    it instead of in the Dockerfile where nothing explains it.
    """
    return ("git", "-c", f"safe.directory={repo}") + args


def _git(*args: str, cwd: Path | None = None) -> str:
    """Run git, returning stdout. Raises PinningError on failure."""
    repo = cwd or REPO_ROOT
    try:
        out = subprocess.run(
            _git_argv(repo, args),
            cwd=str(cwd or REPO_ROOT),
            capture_output=True,
            text=True,
            check=True,
        )
    except FileNotFoundError as exc:  # no git binary -- the Astro image has none
        raise PinningError(
            "no git executable available; version-pinned tracing needs git "
            "(add it to packages.txt for the Airflow image)"
        ) from exc
    except subprocess.CalledProcessError as exc:
        raise PinningError(f"git {' '.join(args)} failed: {exc.stderr.strip()}") from exc
    return out.stdout.strip()


def git_available(repo: Path | None = None) -> bool:
    """Whether this process can read git history at all."""
    try:
        _git("rev-parse", "--git-dir", cwd=repo)
        return True
    except PinningError:
        return False


def head(repo: Path | None = None) -> str:
    """The working tree's current commit."""
    return _git("rev-parse", "HEAD", cwd=repo)


def materialise(
    version: str,
    *,
    repo: Path | None = None,
    into: Path | None = None,
    subdir: str = SQL_SUBDIR,
) -> Path:
    """Extract ``subdir`` as of ``version`` into a directory, and return it.

    Uses ``git archive``, which reads the object database and writes nothing to
    it -- a diagnosis cannot leave a worktree, a lock or a detached HEAD behind
    in the repository it is investigating.
    """
    repo = repo or REPO_ROOT
    target = Path(into) if into is not None else Path(
        tempfile.mkdtemp(prefix=f"branchlin-{version[:12]}-")
    )
    target.mkdir(parents=True, exist_ok=True)

    try:
        archive = subprocess.run(
            _git_argv(repo, ("archive", version, subdir)),
            cwd=str(repo),
            capture_output=True,
            check=True,
        )
    except FileNotFoundError as exc:
        raise PinningError("no git executable available") from exc
    except subprocess.CalledProcessError as exc:
        raise PinningError(
            f"cannot archive {subdir} at {version}: {exc.stderr.decode().strip()}"
        ) from exc

    try:
        subprocess.run(
            ("tar", "-x", "-C", str(target)),
            input=archive.stdout,
            capture_output=True,
            check=True,
        )
    except subprocess.CalledProcessError as exc:
        raise PinningError(f"cannot unpack archive: {exc.stderr.decode().strip()}") from exc

    root = target / subdir
    if not root.exists():
        raise PinningError(
            f"{subdir} does not exist at {version} -- the pipeline SQL was not "
            f"in the repository at that commit"
        )
    return root


def source_at(version: str, *, repo: Path | None = None) -> PinnedSource:
    """A :class:`RawFilesSource` over the SQL as of ``version``."""
    root = materialise(version, repo=repo)
    return PinnedSource(
        source=RawFilesSource(root, version=version),
        version=version,
        provenance=EXPLICIT,
        root=root,
    )


def working_tree_source() -> PinnedSource:
    """The SQL in the working tree, labelled honestly as unpinned."""
    return PinnedSource(
        source=RawFilesSource(DEFAULT_SQL_ROOT),
        version=None,
        provenance=WORKING_TREE,
        root=DEFAULT_SQL_ROOT,
    )


# ---------------------------------------------------------------------------
# resolving a version from an Airflow run
# ---------------------------------------------------------------------------


def version_for_run(
    dag_id: str,
    logical_date: str,
    *,
    trace: list[str] | None = None,
) -> tuple[str | None, str]:
    """The commit a given run executed against, and how we know.

    Returns ``(version, provenance)``; ``version`` is None when nothing can be
    established, in which case the caller falls back to the working tree.
    ``trace`` collects a human-readable account of what was tried -- resolution
    failing silently is how this went unnoticed once already.

    **Finding the run is harder than it looks on Airflow 3.** ``logical_date``
    is nullable, and it is NULL for exactly the runs this pipeline uses:
    asset-triggered and manual ones. Only a scheduled run carries one. A
    date-based lookup therefore matches nothing at all, silently. So the search
    is: a run whose ``logical_date`` matches, else one whose ``run_after`` or
    ``start_date`` falls on that date, else the most recent successful run --
    and in that last case the provenance says so, because "the code the latest
    run used" is a different claim from "the code that ran on 2026-09-03".

    Import of Airflow is deliberately local and guarded: the tracer must remain
    runnable from a plain shell with no Airflow installed.
    """
    def note(msg: str) -> None:
        if trace is not None:
            trace.append(msg)

    try:
        from airflow.models.dagrun import DagRun  # type: ignore
        from airflow.utils.session import create_session  # type: ignore
    except Exception as exc:
        note(f"no Airflow available ({type(exc).__name__})")
        return None, WORKING_TREE

    wanted = str(logical_date)[:10]

    try:
        with create_session() as session:
            run = _find_run(session, DagRun, dag_id, wanted, note)
            if run is None:
                note(f"no run of {dag_id} found for {wanted} or otherwise")
                return None, WORKING_TREE

            if getattr(run, "bundle_version", None):
                note(f"bundle_version on {run.run_id}")
                return run.bundle_version, BUNDLE

            conf = getattr(run, "conf", None) or {}
            if conf.get(CONF_KEY):
                note(f"conf[{CONF_KEY!r}] on {run.run_id}")
                return conf[CONF_KEY], RUN_CONF

            from_xcom = _version_from_xcom(session, dag_id, run.run_id)
            if from_xcom:
                note(f"XCom {CONF_KEY!r} on {run.run_id}")
                return from_xcom, getattr(run, "_branchlin_provenance", RUN_XCOM)

            note(f"run {run.run_id} recorded no version")
    except Exception as exc:
        note(f"lookup failed: {type(exc).__name__}: {exc}")
        return None, WORKING_TREE
    return None, WORKING_TREE


def _find_run(session, DagRun, dag_id: str, wanted: str, note):
    """The run that produced the data for ``wanted``, by decreasing confidence."""
    import sqlalchemy as sa

    base = session.query(DagRun).filter(DagRun.dag_id == dag_id)

    # 1. A scheduled run carries a logical_date.
    try:
        run = (
            base.filter(DagRun.logical_date.isnot(None))
            .filter(sa.cast(DagRun.logical_date, sa.String).like(f"{wanted}%"))
            .order_by(DagRun.logical_date.desc())
            .first()
        )
        if run is not None:
            note(f"matched on logical_date {wanted}")
            run._branchlin_provenance = RUN_XCOM
            return run
    except Exception as exc:
        note(f"logical_date match unavailable: {type(exc).__name__}")

    # 2. Asset-triggered and manual runs have no logical_date, so fall back to
    #    when the run actually happened.
    for column, label in (("run_after", "run_after"), ("start_date", "start_date")):
        col = getattr(DagRun, column, None)
        if col is None:
            continue
        try:
            run = (
                base.filter(col.isnot(None))
                .filter(sa.cast(col, sa.String).like(f"{wanted}%"))
                .order_by(col.desc())
                .first()
            )
            if run is not None:
                note(f"matched on {label} {wanted}")
                run._branchlin_provenance = RUN_XCOM
                return run
        except Exception as exc:
            note(f"{label} match unavailable: {type(exc).__name__}")

    # 3. Nothing matches the date. The most recent successful run is still
    #    informative -- it is the code that produced the data now in the
    #    warehouse -- but it is a weaker claim and is labelled as such.
    try:
        run = (
            base.filter(DagRun.state == "success")
            .order_by(DagRun.start_date.desc())
            .first()
        )
        if run is not None:
            note(f"no run on {wanted}; using latest successful run {run.run_id}")
            run._branchlin_provenance = RUN_XCOM_LATEST
            return run
    except Exception as exc:
        note(f"latest-run fallback unavailable: {type(exc).__name__}")
    return None


def _version_from_xcom(session, dag_id: str, run_id: str) -> str | None:
    """The ``code_version`` a pipeline task recorded for this run.

    Every SQL task in a run pushes the same value, so any one of them answers.
    Deserialised through ``XComModel`` rather than read raw, because the column
    holds JSON-encoded bytes.
    """
    try:
        from airflow.models.xcom import XComModel  # type: ignore
    except Exception:
        return None
    try:
        rows = (
            session.query(XComModel)
            .filter(XComModel.dag_id == dag_id)
            .filter(XComModel.run_id == run_id)
            .filter(XComModel.key == CONF_KEY)
            .all()
        )
    except Exception:
        return None
    for row in rows:
        try:
            value = XComModel.deserialize_value(row)
        except Exception:
            value = getattr(row, "value", None)
        if isinstance(value, bytes):
            value = value.decode(errors="ignore")
        if isinstance(value, str) and value.strip():
            return value.strip().strip('"')
    return None


def source_for(
    as_of: str | None,
    *,
    dag_id: str = "04_reporting_pipeline",
    repo: Path | None = None,
) -> PinnedSource:
    """The SQL that ran for ``as_of``, falling back to the working tree.

    This is the function a DIAGNOSE task calls. It never raises for want of a
    version: an unpinned trace is still useful, and is labelled as such so the
    report cannot overclaim.
    """
    if not as_of:
        return working_tree_source()

    steps: list[str] = []
    version, provenance = version_for_run(dag_id, str(as_of), trace=steps)
    if version is None:
        return working_tree_source()

    try:
        pinned = source_at(version, repo=repo)
    except PinningError:
        return working_tree_source()
    return PinnedSource(
        source=pinned.source,
        version=version,
        provenance=provenance,
        root=pinned.root,
    )


def describe_resolution(as_of: str | None, *, dag_id: str = "04_reporting_pipeline") -> str:
    """One line naming the code version a trace would use, and how it was found."""
    resolved = source_for(as_of, dag_id=dag_id)
    return resolved.describe()
