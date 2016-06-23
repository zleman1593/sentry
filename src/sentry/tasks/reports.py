from datetime import timedelta

import pytz
from django.utils import timezone

from sentry.app import reports  # type: ignore
from sentry.models import (  # type: ignore
    Organization,
    OrganizationStatus,
    User,
)
from sentry.reports.types import Interval
from sentry.tasks.base import instrumented_task  # type: ignore
from sentry.utils.dates import (  # type: ignore
    to_datetime,
    to_timestamp,
)


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


@instrumented_task(
    name='sentry.tasks.reports.schedule_organization_reports',
    queue='reports.scheduling')
def schedule_organization_reports(period):
    stop_dt = floor_to_utc_day(timezone.now())
    start_dt = stop_dt - timedelta(seconds=period)

    start = to_timestamp(start_dt)
    stop = to_timestamp(stop_dt)

    organization_id_list = Organization.objects.filter(
        status=OrganizationStatus.VISIBLE
    ).values_list('id', flat=True)
    for organization_id in organization_id_list:
        prepare_organization_reports.delay(start, stop, organization_id)


@instrumented_task(
    name='sentry.tasks.reports.prepare_organization_reports',
    queue='reports.preparation')
def prepare_organization_reports(start, stop, organization_id):
    # type: (float, float, int) -> None
    interval = Interval(to_datetime(start), to_datetime(stop))
    organization = Organization.objects.get(id=organization_id)
    for user_id in reports.prepare_organization_reports(interval, organization):
        deliver_user_report.delay(start, stop, organization_id, user_id)


@instrumented_task(
    name='sentry.tasks.reports.deliver_user_report',
    queue='reports.delivery')
def deliver_user_report(start, stop, organization_id, user_id):
    # type: (float, float, int, int) -> None
    reports.deliver_user_report(
        Interval(to_datetime(start), to_datetime(stop)),
        Organization.objects.get(id=organization_id),
        User.objects.get(id=user_id),
    )
