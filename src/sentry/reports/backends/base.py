from typing import (
    Mapping,
    Sequence,
    Tuple,
)

from sentry.models import (  # type: ignore
    Organization,
    Project,
)
from sentry.reports.types import (
    Interval,
    Report,
)


Key = Tuple[Interval, Organization]


class InvalidTaskError(Exception):
    pass


class Backend(object):
    """
    Provides intermediate storage for reports as they are processed.
    """
    def store(self, key, reports, tasks):
        # type: (Key, Mapping[Project, Report], Sequence[str]) -> None
        """
        Store a collection of reports for projects, as well as a collection of
        tasks IDs that are expected to read from the report data.

        Storing both the reports and the task set is performed as an atomic
        operation. If either the report or task set already exists, this
        operation will be error.
        """
        raise NotImplementedError

    def fetch(self, key, projects, task):
        # type: (Key, Sequence[Project], str) -> Sequence[Report]
        """
        Fetch reports for a sequence of projects, returning a sequence of
        reports in the same order as the project squence.

        The provided task ID must still be in the task set, otherwise an
        ``InvalidTaskError`` will be raised.
        """
        raise NotImplementedError

    def commit(self, key, task):
        # type: (Key, str) -> None
        """
        Mark a task as completed. When all tasks are completed for a key, the
        associated report data will be removed from the database.

        In the case that the specified task is not part of the task set, this
        is a noop (making this method idempotent.)
        """
        raise NotImplementedError
