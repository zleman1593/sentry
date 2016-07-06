import json
import zlib

from typing import (
    Any,
    List,
    Mapping,
    Tuple,
)

from sentry.reports.types import (
    IssueList,
    IssueListItem,
    Report,
    Timestamp,
)


def decode_series(value):
    # type: (Any) -> List[Tuple[Timestamp, Mapping[str, float]]]
    return [(timestamp, {k: float(v) for k, v in statistics.items()}) for timestamp, statistics in value]

def decode_aggregates(value):
    # type: (Any) -> Mapping[str, float]
    return {k: float(v) for k, v in value.items()}

def decode_issue_list_item(value):
    # type: (Any) -> IssueListItem
    return IssueListItem(value[0], {k: float(v) for k, v in value[1].items()})

def decode_issues(value):
    # type: (Any) -> Mapping[str, IssueList]
    return {k: IssueList(v[0], [(decode_issue_list_item(i), float(s)) for i, s in v[1]]) for k, v in value.items()}


class Codec(object):
    def encode(self, value):
        # type: (Report) -> bytes
        return zlib.compress(json.dumps(value))

    def decode(self, value):
        # type: (bytes) -> Report
        data = json.loads(zlib.decompress(value))
        return Report(
            decode_series(data[0]),
            decode_aggregates(data[1]),
            decode_issues(data[2]),
        )
