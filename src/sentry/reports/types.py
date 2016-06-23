from datetime import datetime
from typing import (
    Mapping,
    NamedTuple,
    Sequence,
    Tuple,
)

Timestamp = int

Interval = NamedTuple('Interval', [
    ('start', datetime),
    ('stop', datetime),
])

UserStatistics = NamedTuple('UserStatistics', [
    ('resolved_issue_count', int),
    ('users_affected_count', int),
])

IssueScore = float

IssueListItem = NamedTuple('IssueListItem', [
    ('issue_id', int),
    ('statistics', Mapping[str, float]),
])

IssueList = NamedTuple('IssueList', [
    ('count', int),
    ('issues', Sequence[Tuple[IssueListItem, IssueScore]]),
])

Report = NamedTuple('Report', [
    ('series', Sequence[Tuple[Timestamp, Mapping[str, float]]]),
    ('aggregates', Mapping[str, float]),
    ('issues', Mapping[str, IssueList]),
])
