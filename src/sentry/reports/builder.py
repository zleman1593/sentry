import itertools
import operator
import random
from datetime import datetime, timedelta

from typing import (
    cast,
    Callable,
    Dict,
    Mapping,
    Optional,
    Tuple,
)

from django.db.models.query import (  # type: ignore
    Q,
    QuerySet,
)

from sentry.app import tsdb  # type: ignore
from sentry.models import (  # type: ignore
    Activity,
    Organization,
    GroupStatus,
    Project,
    User,
)
from sentry.reports.types import (
    Interval,
    IssueStatistics,
    IssueReference,
    IssueListSpecification,
    IssueListItem,
    IssueListScore,
    Report,
    ReportStatistics,
    ScoredIssueList,
    ScoredIssueListItem,
    Timestamp,
    UserStatistics,
)
from sentry.reports.utilities import (
    merge_mappings,
    merge_series,
)
from sentry.utils.dates import to_timestamp  # type: ignore


def simple_score((issue, statistics)):  # type: (IssueListItem) -> IssueListScore
    return float(statistics.events)


issue_list_specifications = {
    'new': IssueListSpecification(
        "New Issues",
        lambda interval: Q(
            first_seen__gte=interval.start,
            first_seen__lt=interval.end,
        ),
        score=simple_score,
        limit=5,
    ),
    'reopened': IssueListSpecification(
        "Reintroduced Issues",
        lambda interval: Q(
            status=GroupStatus.UNRESOLVED,
            resolved_at__gte=interval.start,
            resolved_at__lt=interval.end,
        ),  # TODO: Is this safe?
        score=simple_score,
        limit=5,
    ),
    'most-seen': IssueListSpecification(
        "Most Seen Issues",
        lambda interval: Q(
            last_seen__gte=interval.start,
            last_seen__lt=interval.end,
        ),  # XXX: This might be very large, it might make sense to start sketching this?
        score=simple_score,
        limit=5,
    ),
}


def sort_and_truncate_issues(specification, issues):
    # type: (IssueListSpecification, List[ScoredIssueListItem]) -> List[ScoredIssueListItem]
    return sorted(
        issues,
        key=lambda (item, score): score,
        reverse=True,
    )[:specification.limit]


def merge_reports(target, other):
    # type: (Report, Report) -> Report
    assert target.interval == other.interval, 'report intervals must match'

    def merge_scored_issue_lists(specification, left, right):
        # type: (IssueListSpecification, ScoredIssueList, ScoredIssueList) -> ScoredIssueList
        return ScoredIssueList(
            left.count + right.count,
            sort_and_truncate_issues(specification, left.issues + right.issues),
        )

    def merge_report_statistics(left, right):
        # type: (ReportStatistics, ReportStatistics) -> ReportStatistics
        return ReportStatistics(
            merge_mappings(
                lambda key, left, right: merge_series(
                    operator.add,
                    left,
                    right,
                ),
                left.series,
                right.series,
            ),
            merge_mappings(
                lambda key, left, right: left + right,
                left.aggregates,
                right.aggregates,
            ),
        )

    return Report(
        target.interval,
        merge_report_statistics(
            target.statistics,
            other.statistics,
        ),
        merge_mappings(
            merge_scored_issue_lists,
            target.issues,
            other.issues,
        ),
    )


def prepare_project_issue_list(specification, queryset, interval, rollup):
    # type: (IssueListSpecification, Project, Interval, int) -> ScoredIssueList
    # Fetch all of the groups IDs that meet this constraint.
    issue_id_list = queryset.filter(
        specification.filter_factory(interval)
    ).values_list('id', flat=True)

    issue_events = tsdb.get_sums(
        tsdb.models.group,
        issue_id_list,
        interval.start,
        interval.end,
        rollup,
    )  # type: Mapping[int, int]

    issue_users = tsdb.get_distinct_counts_totals(
        tsdb.models.users_affected_by_group,
        issue_id_list,
        interval.start,
        interval.end,
        rollup,
    )  # type: Mapping[int, int]

    issue_list = []
    for issue_id in issue_id_list:
        item = IssueListItem(
            IssueReference(issue_id),
            IssueStatistics(
                issue_events.get(issue_id, 0),
                issue_users.get(issue_id, 0),
            ),
        )
        issue_list.append((
            item,
            specification.score(item),
        ))

    return ScoredIssueList(
        len(issue_id_list),
        sort_and_truncate_issues(
            specification,
            issue_list,
        ),
    )


def get_aggregation_periods(interval):  # type: (Interval) -> Mapping[str, Interval]
    return {
        'this_week': interval,
        'last_week': Interval(
            interval.start - timedelta(days=7),
            interval.end - timedelta(days=7),
        ),
        'this_month': Interval(
            interval.end - timedelta(days=7 * 4),
            interval.end,
        ),
    }


def closed_range(series):
    # XXX: This is a temporary hack and should be implemented more intelligently!
    return series[:-1]


def get_resolved_issue_queryset(queryset, interval):
    # type: (QuerySet, Interval) -> QuerySet
    return queryset.filter(
        status=GroupStatus.RESOLVED,
        resolved_at__gte=interval.start,
        resolved_at__lt=interval.end,
    )


def get_series_generators(interval):
    # type: (Interval) -> Mapping[str, Callable[[Project], List[Tuple[Timestamp, int]]]]
    rollup = 60 * 60 * 24
    return {
        "resolved": lambda project: reduce(
            lambda left, right: merge_series(operator.add, left, right),
            map(
                closed_range,
                tsdb.get_range(
                    tsdb.models.group,
                    get_resolved_issue_queryset(
                        project.group_set.all(),
                        interval,
                    ).values_list('id', flat=True),
                    interval.start,
                    interval.end,
                    rollup
                ).values(),
            ),
            [(to_timestamp(i.start), 0) for i in interval.range(timedelta(seconds=rollup))],
        ),
        "total": lambda project: closed_range(
            tsdb.get_range(
                tsdb.models.project,
                (project.id,),
                interval.start,
                interval.end,
                rollup,
            ).get(project.id, []),
        )
    }


def prepare_project_statistics(project, interval, queryset, rollup):
    # type: (Project, Interval, QuerySet, int) -> ReportStatistics
    """
    Prepares a project's report statistics.
    """
    # Fetch the series data for the project for the report duration.
    # TODO: Normalize these series keys! (Some of them are returned as floats.)
    series = {k: fetch(project) for k, fetch in get_series_generators(interval).items()}

    # Fetch the history data for the project for the given durations.
    aggregate_results = cast(Dict[str, long], {})
    for key, i in get_aggregation_periods(interval).items():
        aggregate_results[key] = get_resolved_issue_queryset(queryset, i).count()

    return ReportStatistics(
        series,
        aggregate_results,
    )


def prepare_project_report(project, interval, rollup):
    # type: (Project, Interval, int) -> Report
    """
    Prepares a project's report.
    """
    assert (interval.duration().total_seconds() / rollup % 1) == 0, \
            'rollup must divide interval duration without remainder'

    # We don't need to include muted groups in the report, so filter them here.
    queryset = project.group_set.exclude(status=GroupStatus.MUTED)

    issue_lists = {}
    for key, specification in issue_list_specifications.items():
        issue_lists[specification] = prepare_project_issue_list(
            specification,
            queryset,
            interval,
            rollup,
        )

    return Report(
        interval,
        prepare_project_statistics(
            project,
            interval,
            queryset,
            rollup,
        ),
        issue_lists,
    )


def prepare_user_statistics(organization, user, interval, rollup):
    # type: (Organization, User, Interval, int) -> UserStatistics
    resolved_issue_ids = Activity.objects.filter(
        project__organization_id=organization.id,
        user_id=user.id,
        type__in=(
            Activity.SET_RESOLVED,
            Activity.SET_RESOLVED_IN_RELEASE,
        ),
        datetime__gte=interval.start,
        datetime__lt=interval.end,
        group__status=GroupStatus.RESOLVED,  # only count if the issue is still resolved
    ).values_list('group_id', flat=True)

    return UserStatistics(
        len(resolved_issue_ids),
        tsdb.get_distinct_counts_union(
            tsdb.models.users_affected_by_group,
            resolved_issue_ids,
            interval.start,
            interval.end,
            rollup,
        ),
    )
