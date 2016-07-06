from __future__ import absolute_import

from typing import (
    Mapping,
    Optional,
    Sequence,
    Tuple,
)

from rb.clients import LocalClient  # type: ignore
from redis.exceptions import ResponseError

from sentry.models import Project  # type: ignore
from sentry.reports.backends.base import (
    Backend,
    InvalidStateError,
    InvalidTaskError,
    Key,
)
from sentry.reports.codec import Codec
from sentry.reports.types import Report
from sentry.utils.dates import to_timestamp  # type: ignore
from sentry.utils.redis import (  # type: ignore
    clusters,
    load_script,
)


assert_keys_do_not_exist = load_script('reports/assert_keys_do_not_exist.lua')
commit_task = load_script('reports/commit_task.lua')


class RedisBackend(Backend):
    """
    Utilizes Redis to provide intermediate storage for reports as they are
    processed.

    The storage model consists of two data structures: the encoded report data
    (stored as a hash, where keys are project IDs), and a set of outstanding
    tasks IDs that have yet to process the reports. These keys are colocated
    (so that they can be accessed atomically) by the report's ``Key``.
    """
    def __init__(self, cluster='default', ttl=None):
        # type: (str, Optional[int]) -> None
        self.cluster = clusters.get(cluster)
        self.ttl = ttl

        self.codec = Codec()

    def __get_client(self, key):
        # type: (Key) -> LocalClient
        return self.cluster.get_local_client_for_key(self.__make_key(key))

    def __make_key(self, (interval, organization), suffix=None):
        # type: (Key, str) -> str
        key = 'r:{}:{}:{}'.format(
            to_timestamp(interval.start),
            to_timestamp(interval.stop),
            organization.id,
        )

        if suffix is not None:
            return '{}:{}'.format(key, suffix)
        else:
            return key

    def store(self, key, reports, tasks, force=False):
        # type: (Key, Mapping[Project, Report], Sequence[str], bool) -> None
        if not force and not tasks:
            return

        keys = (
            self.__make_key(key, 'r'),
            self.__make_key(key, 't'),
        )
        with self.__get_client(key).pipeline(transaction=True) as pipeline:
            if not force:
                assert_keys_do_not_exist(pipeline, keys, ())
            else:
                pipeline.delete(*keys)

            pipeline.hmset(
                self.__make_key(key, 'r'),
                {project.id: self.codec.encode(report) for project, report in reports.items()}
            )
            pipeline.sadd(self.__make_key(key, 't'), *tasks)

            if self.ttl is not None:
                for k in keys:
                    pipeline.expire(k, self.ttl)

            try:
                pipeline.execute()
            except ResponseError as error:
                if error.message.startswith('keys exist:'):
                    raise InvalidStateError()  # TODO: Improve messaging here.
                else:
                    raise

    def fetch(self, key, projects, task):
        # type: (Key, Sequence[Project], str) -> Sequence[Report]
        client = self.__get_client(key)

        if not client.sismember(self.__make_key(key, 't'), task):
            raise InvalidTaskError('{} is not in task set'.format(task))

        return map(
            self.codec.decode,
            client.hmget(
                self.__make_key(key, 'r'),
                [project.id for project in projects],
            ),
        )

    def commit(self, key, task):
        # type: (Key, str) -> None
        commit_task(
            self.__get_client(key),
            (
                self.__make_key(key, 'r'),
                self.__make_key(key, 't'),
            ),
            (task,),
        )
