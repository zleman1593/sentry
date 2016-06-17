from datetime import datetime, timedelta
import operator

from typing import (
    Callable,
    List,
    Mapping,
    NamedTuple,
    Optional,
    Tuple,
)
from django.db.models.query import Q  # type: ignore


Timestamp = int

class Interval(object):
    def __init__(self, start, end):  # type: (datetime, datetime) -> None
        self.start = start
        self.end = end

    def __repr__(self):   # type: () -> str
        return 'Interval({}, {})'.format(self.start, self.end)

    def duration(self):  # type: () -> timedelta
        return self.end - self.start

    def range(self, step):  #  type: (timedelta) -> List[Interval]
        steps = self.duration().total_seconds() / step.total_seconds()
        assert (steps % 1) == 0, 'step must evenly divide interval duration'
        results = []
        for i in xrange(0, int(steps)):
            results.append(
                Interval(
                    self.start + (step * i),
                    self.start + (step * (i + 1)),
                )
            )
        return results


IssueListScore = float

IssueReference = NamedTuple('IssueReference', [
    ('id', long),
])

IssueStatistics = NamedTuple('IssueStatistics', [
    ('events', long),
    ('users', long),
])

IssueListItem = NamedTuple('IssueListItem', [
    ('reference', IssueReference),
    ('statistics', IssueStatistics),
])

ScoredIssueListItem = Tuple[
    IssueListItem,
    IssueListScore,
]

IssueListSpecification = NamedTuple('IssueListSpecification', [
    ('label', unicode),
    ('filter_factory', Callable[[Interval], Q]),
    ('score', Callable[[IssueListItem], IssueListScore]),
    ('limit', int),
])

ScoredIssueList = NamedTuple('ScoredIssueList', [
    ('count', long),
    ('issues', List[ScoredIssueListItem]),
])

ReportStatistics = NamedTuple('ReportStatistics', [
    ('series', Mapping[str, List[Tuple[Timestamp, long]]]),
    ('aggregates', Mapping[str, long]),
])

Report = NamedTuple('Report', [
    ('interval', Interval),
    ('statistics', ReportStatistics),
    ('issues', Mapping[IssueListSpecification, ScoredIssueList]),  # TODO: This should be ordered.
])

UserStatistics = NamedTuple('UserStatistics', [
    ('resolved', long),
    ('users', long),
])
