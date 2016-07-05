import logging

import pytz
from django.utils import timezone

from sentry.app import (  # type: ignore
    locks,
    reports,
)
from sentry.models import (  # type: ignore
    Organization,
    OrganizationStatus,
    User,
)
from sentry.reports.base import InvalidTaskError
from sentry.reports.types import Interval
from sentry.tasks.base import (  # type: ignore
    instrumented_task,
    retry,
)
from sentry.utils.dates import (  # type: ignore
    to_datetime,
    to_timestamp,
)


logger = logging.getLogger(__name__)


def floor_to_utc_day(value):
    # type: (datetime) -> datetime
    """
    Floors a given datetime to UTC midnight.
    """
    return value.astimezone(pytz.utc).replace(
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    )


# TODO: This task needs retries!
@instrumented_task(
    name='sentry.tasks.reports.schedule_organization_reports',
    queue='reports.scheduling')
def schedule_organization_reports(period, now=None):
    # type: (int, datetime) -> None
    """
    Enqueue a task to prepare reports for all active organizations.
    """
    if now is None:
        now = timezone.now()

    # XXX: This task assumes we're always generating reports that start and end
    # on a UTC date boundary. We do this for a few reasons. First, the largest
    # metric aggregation interval defined in the default TSDB configuration is
    # 86400 seconds (one full 24-hour day) aligned on UTC day boundaries --
    # using this alignment means we are able to make the most accurate and
    # efficient queries possible with the default configuration. Second, the
    # Celery scheduler doesn't send the schedule time along with the task when
    # it is dispatched into the queue, so this large minimum interval gives us
    # a reasonably high chance that we'll still be in the same reporting period
    # at the time the task is dispatched as when it is executed (especially if
    # the task expiration is set correctly) so that we don't inadvertently skip
    # a reporting period or shift it's bounds by less than 24 hours. (This also
    # means that daylight savings time changes shifts the reporting interval in
    # the recipient's local time zone by one hour.)

    # TODO: To be even more safe/correct, we should probably track the latest
    # interval that was handled by the scheduler somewhere to prevent
    # double-sending.
    assert period <= 86400, 'reporting period must be at least 24 hours'
    assert period % 86400 == 0, 'reporting period must be a multiple of 86400'

    stop = to_timestamp(floor_to_utc_day(now))
    start = stop - period

    organization_id_list = Organization.objects.filter(
        status=OrganizationStatus.VISIBLE
    ).values_list('id', flat=True)

    for organization_id in organization_id_list:
        prepare_organization_reports.delay(start, stop, organization_id)


# TODO: This task needs retries!
@instrumented_task(
    name='sentry.tasks.reports.prepare_organization_reports',
    queue='reports.preparation')
def prepare_organization_reports(start, stop, organization_id):
    # type: (float, float, int) -> None
    """
    Prepare project reports for projects within an organization and enqueue
    tasks to deliver to individualized reports for users.
    """
    interval = Interval(to_datetime(start), to_datetime(stop))
    organization = Organization.objects.get(id=organization_id)

    # TODO: Catch exception on duplicate report generation, reschedule from
    # remaining task set instead. (This can cause lots of tasks to be enqueued
    # if the parent task gets stuck in a retry loop!) It might also make sense
    # to set a tombstone record here so that if all records have been
    # processed, we don't try and recreate the reports.
    tasks = reports.prepare_organization_reports(interval, organization)

    for user_id in tasks:
        deliver_user_report.delay(start, stop, organization_id, user_id)


@instrumented_task(
    name='sentry.tasks.reports.deliver_user_report',
    queue='reports.delivery')
@retry(exclude=(InvalidTaskError,))
def deliver_user_report(start, stop, organization_id, user_id):
    # type: (float, float, int, int) -> None
    """
    Deliver an individualized report to a user.
    """
    # TODO: It would make sense to colocate these locks with the data if
    # possible, like we do with digests.
    lock = locks.get(
        'report:{}:{}:{}:{}'.format(start, stop, organization_id, user_id),
        120,
    )

    with lock.acquire():
        reports.deliver_user_report(
            Interval(to_datetime(start), to_datetime(stop)),
            Organization.objects.get(id=organization_id),
            User.objects.get(id=user_id),
        )
