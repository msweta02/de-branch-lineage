"""SQLSource over plain ``.sql`` files.

Reads the numbered modules under ``include/sql/`` -- ``01_reference``,
``02_trades``, ``03_positions``, ``04_reporting`` -- and yields their
statements with the relation each produces and the relations it consumes.

**Ordering comes from the ``-- depends:`` header in each file.** That is the
same declaration ``include/runner.py`` executes on, so there is one dependency
mechanism in the project rather than two. The filename prefixes are a human
convenience: they are used only to break ties between statements that are
genuinely independent, so that the emitted order matches the order the runner
happens to execute them in.

(An earlier draft of this docstring claimed ordering came from the Airflow task
and asset graph. It does not. Two mechanisms describing the same graph is a
mechanism that will eventually disagree with itself; see docs/BUILD_LOG.md,
Phase 1 "Settled before starting".)

The root is a parameter rather than a constant so the tracer can point this at
a *version-pinned checkout* of the repository instead of the working tree --
the SQL that actually ran on the date under investigation. See
:mod:`branchlin.pinning`.
"""

from __future__ import annotations

from pathlib import Path
from typing import Iterable, Iterator

from branchlin.parser import header
from branchlin.sources import SQLSource, Statement

#: Default location of the pipeline's SQL, relative to the ``include`` dir.
DEFAULT_SQL_ROOT = Path(__file__).resolve().parents[2] / "sql"

MODULES = ("01_reference", "02_trades", "03_positions", "04_reporting")


class RawFilesSource(SQLSource):
    """Every ``.sql`` file under ``root``, one statement per file.

    ``root`` defaults to ``include/sql`` in the working tree. Point it at a
    bundle checkout to trace a historical run.
    """

    def __init__(self, root: Path | str | None = None, *, version: str | None = None):
        self.root = Path(root) if root is not None else DEFAULT_SQL_ROOT
        #: The commit the root was materialised at, when it came from pinning.
        self.version = version
        if not self.root.exists():
            raise FileNotFoundError(f"no SQL root at {self.root}")

    def __repr__(self) -> str:
        v = f", version={self.version!r}" if self.version else ""
        return f"RawFilesSource({str(self.root)!r}{v})"

    def files(self) -> tuple[Path, Path]:
        """Every ``.sql`` file, in module then filename order."""
        return tuple(sorted(self.root.glob("*/*.sql")))

    def statements(self) -> Iterator[Statement]:
        for path in self.files():
            sql = path.read_text()
            target, depends = header(sql)
            if target is None:
                # A file with no `-- target:` header declares no output, so
                # nothing downstream can depend on it and it cannot be a hop.
                continue
            yield Statement(
                target=target,
                depends=depends,
                sql=sql,
                origin=f"{path.parent.name}/{path.name}",
            )

    def path_for(self, relation: str) -> Path | None:
        """The file that produces ``relation``."""
        st = self.get(relation)
        if st is None:
            return None
        return self.root / st.origin
