import functools
import itertools
import logging
import operator
from collections import OrderedDict, namedtuple
from django.db.models import Q
from sentry.models import Activity, Group, GroupStatus, Organization
from sentry.app import tsdb


logger = logging.getLogger(__name__)


ReportSpecification = namedtuple(
    'ReportSpecification',
    ('label', 'filter_factory', 'limit', 'score_function'),
)

IssueStatistics = namedtuple('IssueStatistics', ('occurences', 'users'))
Issue = namedtuple('Issue', ('id', 'statistics', 'score'))
IssueList = namedtuple('IssueList', ('count', 'issues'))
Report = namedtuple('Report', ('series', 'issues'))
ReportSeriesItem = namedtuple('ReportSeriesItem', ('timestamp', 'statistics'))
ReportSeriesStatistics = namedtuple('ReportSeriesStatistics', ('resolved', 'total'))
UserReport = namedtuple('UserReport', ('resolved_issues', 'users_affected'))


def simple_issue_score(statistics):
    # TODO: Make this scoring function less naive.
    return statistics.occurences


report_specifications = OrderedDict((
    ('new', ReportSpecification(
        'New groups',
        lambda start, end: Q(
            first_seen__gte=start,
            first_seen__lt=end,
        ),
        limit=5,
        score_function=simple_issue_score,
    )),
    ('reopened', ReportSpecification(
        'Reopened groups',
        lambda start, end: Q(
            status=GroupStatus.UNRESOLVED,
            resolved_at__gte=start,
            resolved_at__lt=end,
        ),  # TODO: Is this safe?
        limit=5,
        score_function=simple_issue_score,
    )),
    ('most-seen', ReportSpecification(
        'Most seen groups',
        lambda start, end: Q(
            last_seen__gte=start,
            last_seen__lt=end,
        ),  # XXX: This might be very large, it might make sense to start sketching this?
        limit=5,
        score_function=simple_issue_score,
    )),
))


# TODO: Probably refactor this into a instance method on ``specification``?
def prepare_issue_list(queryset, start, end, specification, rollup):
    # Fetch all of the groups IDs that meet this constraint.
    # TODO: This join and sort should be chunked out and performed
    # incrementally to avoid potentially consuming a lot of memory. We should
    # also ensure query caching is disabled here.
    issue_id_list = queryset.filter(specification.filter_factory(start, end)).values_list('id', flat=True)

    # Join them against the group statistics.
    issue_occurrences = tsdb.get_sums(tsdb.models.group, issue_id_list, start, end, rollup)
    issue_users = tsdb.get_distinct_counts_totals(tsdb.models.users_affected_by_group, issue_id_list, start, end, rollup)

    # Score the groups, and sort them by score.
    results = []
    for issue_id in issue_id_list:
        statistics = IssueStatistics(
            issue_occurrences.get(issue_id, 0),
            issue_users.get(issue_id, 0),
        )
        results.append(
            Issue(
                issue_id,
                statistics,
                specification.score_function(statistics)
            )
        )

    # Truncate the groups to the limit.
    return IssueList(
        len(issue_id_list),
        sorted(
            results,
            key=operator.attrgetter('score'),
            reverse=True,
        )[:specification.limit],
    )


def merge_series(target, other, function=operator.add):
    missing = object()
    result = []
    for left, right in itertools.izip_longest(target, other, fillvalue=missing):
        assert left[0] == right[0], 'timestamps must match'
        assert left[1] is not missing and right[1] is not missing, 'iterables of unequal length'
        result.append((left[0], function(left[1], right[1])))
    return result


def prepare_project_series(project, queryset, start, end, rollup):
    # Fetch the resolved issues.
    resolved_issue_ids = queryset.filter(
        status=GroupStatus.RESOLVED,
        resolved_at__gte=start,
        resolved_at__lt=end,
    ).values_list('id', flat=True)

    resolution, timestamps = tsdb.get_optimal_rollup_series(start, end, rollup)
    assert resolution == rollup, 'series resolution does not match requested rollup duration'

    # Fetch the series data for the number of times each resolved issue was seen.
    resolved_event_series = reduce(
        merge_series,
        tsdb.get_range(tsdb.models.group, resolved_issue_ids, start, end, rollup).values(),
        [(timestamp, 0) for timestamp in timestamps],
    )

    # Fetch the series data for the number of times any issue on the project was seen.
    total_event_series = tsdb.get_range(tsdb.models.project, (project.id,), start, end, rollup).get(project.id, [])

    series = []
    for i, timestamp in enumerate(timestamps):
        total_event_series_item = total_event_series[i]
        assert total_event_series_item[0] == timestamp

        resolved_event_series_item = resolved_event_series[i]
        assert resolved_event_series_item[0] == timestamp

        series.append(
            ReportSeriesItem(
                timestamp,
                ReportSeriesStatistics(
                    resolved_event_series_item[1],
                    total_event_series_item[1],
                ),
            )
        )

    return series


def prepare_project_report(project, end, period):
    """
    Calculate report data for a project.
    """
    queryset = Group.objects.filter(project=project).exclude(status=GroupStatus.MUTED)

    start = end - period
    rollup = int(period.total_seconds() / 7)

    # Fetch all of the groups for each query.
    issue_lists = {}
    for key, specification in report_specifications.iteritems():
        issue_lists[key] = prepare_issue_list(queryset, start, end, specification, rollup)

    # Return the series data, and issue lists.
    return Report(
        prepare_project_series(project, queryset, start, end, rollup),
        issue_lists,
    )


def prepare_reports_for_organization(organization_id, end, period):
    """
    Calculate report data for an organization, and enqueue delivery tasks for
    all recipients.
    """
    try:
        organization = Organization.objects.get(id=organization_id)
    except Organization.DoesNotExist:
        logger.info('Skipping report preparation for %r, organization does not exist.', organization)
        return

    # Check to make sure this organization has reports enabled.
    if not features.has('organizations:reports', organization):
        logger.info('Skipping report preparation for %r, organization does have reports enabled.', organization)
        return

    # Identify all users who are recipients of the report.
    # Identify all projects that have recipients associated with them.

    # Prepare the summaries for each project.
    # Store the project summaries somewhere.

    # Enqueue the delivery task for each (organization, user) pair.
    raise NotImplementedError


def merge_mappings(target, other, function=None, keys=None):
    # TODO: Support updating in place, this creates a lot of garbage.
    unset = object()

    if keys is None:
        keys = set(target.keys()) | set(other.keys())

    if function is None:
        function = lambda key, a, b: a + b

    results = {}
    for key in keys:
        a = target.get(key, unset)
        b = other.get(key, unset)
        if a is unset:
            assert b is not unset
            results[key] = b
        elif b is unset:
            assert a is not unset
            results[key] = a
        else:
            results[key] = function(key, a, b)

    return results


def merge_issue_lists(key, target, other):
    # NOTE: This makes the assumption that the members of the ``target`` and
    # ``other`` issue lists are mutually exclusive (the same ``issue.id``
    # doesn't show up in both lists.)
    specification = report_specifications[key]
    return IssueList(
        target.count + other.count,
        sorted(
            target.issues + other.issues,
            key=operator.attrgetter('score'),
            reverse=True,
        )[:specification.limit],
    )


def merge_series_statistics(target, other):
    return ReportSeriesStatistics(
        target.resolved + other.resolved,
        target.total + other.total,
    )


def merge_reports(aggregate, report):
    return Report(
        merge_series(
            aggregate.series,
            report.series,
            merge_series_statistics,
        ),
        merge_mappings(
            aggregate.issues,
            report.issues,
            merge_issue_lists,
            report_specifications.keys(),
        ),
    )


def prepare_user_report(organization, user, start, end, rollup):
    resolved_issue_ids = Activity.objects.filter(
        project__organization_id=organization.id,
        user_id=user.id,
        type__in=(
            Activity.SET_RESOLVED,
            Activity.SET_RESOLVED_IN_RELEASE,
        ),
        datetime__gte=start,
        datetime__lt=end,
        group__status=GroupStatus.RESOLVED,  # only count if the issue is still resolved
    ).values_list('group_id', flat=True)

    return UserReport(
        len(resolved_issue_ids),
        tsdb.get_distinct_counts_union(
            tsdb.models.users_affected_by_group,
            resolved_issue_ids,
            start,
            end,
            rollup,
        )
    )


def prepare_and_deliver_report_to_user(organization_id, user_id, end, period):
    """
    Compose report data from projects this user is a member of, fetch user
    specific data, render and send the user's report.
    """
    # Fetch all of the statistics for the projects that this user is associated with.
    # Combine all of the statistics (series data and issue lists.)
    # TODO: This needs to handle the case where there are no issues to display in the list.

    # Build the email message.
    raise NotImplementedError
