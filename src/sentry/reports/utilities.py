import itertools
import operator

from typing import (
    Callable,
    Mapping,
    Sequence,
    Tuple,
    TypeVar,
)

from sentry.app import tsdb  # type: ignore
from sentry.reports.types import (
    Interval,
    Timestamp,
)


K = TypeVar('K')
T = TypeVar('T')
V = TypeVar('V')


def merge_mapping(target, other, function=operator.add):
    # type: (Mapping[K, V], Mapping[K, V], Callable[[V, V], V]) -> Mapping[K, V]
    assert set(target) == set(other), 'mapping keys must match'
    results = {}
    for key, value in target.items():
        results[key] = function(value, other[key])
    return results


def merge_series(target, other, function=operator.add):
    # type: (Sequence[Tuple[Timestamp, T]], Sequence[Tuple[Timestamp, T]], Callable[[T, T], V]) -> Sequence[Tuple[Timestamp, V]]
    missing = object()
    result = []
    for left, right in itertools.izip_longest(target, other, fillvalue=missing):
        assert left is not missing and right is not missing, 'series must be same length'
        assert left[0] == right[0], 'timestamps do not match'
        result.append((left[0], function(left[1], right[1])))
    return result


def get_empty_series(interval, rollup, default):
    # type: (Interval, int, Callable[[Timestamp], V]) -> Sequence[Tuple[Timestamp, V]]
    resolution, timestamps = tsdb.get_optimal_rollup_series(interval.start, interval.stop, rollup)
    assert resolution == rollup, 'resolution must match provided rollup'
    return [(timestamp, default(timestamp)) for timestamp in timestamps]
