import json
import zlib

from sentry.reports.types import (
    IssueList,
    IssueListItem,
    Report,
)


class Codec(object):
    def encode(self, value):
        # type: (Report) -> bytes
        return zlib.compress(json.dumps(value))

    def decode(self, value):
        # type: (bytes) -> Report
        data = json.loads(zlib.decompress(value))

        def decode_series(value):
            return [(timestamp, {k: float(v) for k, v in statistics.items()}) for timestamp, statistics in value]

        def decode_aggregates(value):
            return {k: float(v) for k, v in value.items()}

        def decode_issue_list_item(value):
            return IssueListItem(value[0], {k: float(v) for k, v in value[1].items()})

        def decode_issues(value):
            return {k: IssueList(v[0], [(decode_issue_list_item(i), float(s)) for i, s in v[1]]) for k, v in value.items()}

        return Report(
            decode_series(data[0]),
            decode_aggregates(data[1]),
            decode_issues(data[2]),
        )
