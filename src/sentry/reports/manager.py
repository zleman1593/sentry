from datetime import timedelta

from django.db.models import Q  # type: ignore
from typing import (
    Callable,
    Dict,
    Mapping,
    NamedTuple,
    Sequence,
    Tuple,
)

from sentry.app import tsdb  # type: ignore
from sentry.models import (  # type: ignore
    Activity,
    GroupStatus,
    Organization,
    Project,
    ProjectStatus,
    Team,
    User,
)
from sentry.reports.types import (
    Interval,
    IssueList,
    IssueListItem,
    IssueScore,
    Report,
    Timestamp,
    UserStatistics,
)
from sentry.reports.utilities import (
    get_empty_series,
    merge_mapping,
    merge_series,
)


IssueListSpecification = NamedTuple('IssueListSpecification', [
    ('label', unicode),
    ('filter', Callable[[Interval], Q]),
])


ISSUE_LIMIT = 5


def merge_issue_lists(target, other, limit=ISSUE_LIMIT):
    # type: (IssueList, IssueList, int) -> IssueList
    return IssueList(
        target.count + other.count,
        sorted(
            list(target.issues) + list(other.issues),
            key=lambda item: item[1],
            reverse=True,
        )[:limit],
    )


def merge_reports(target, other):
    # type: (Report, Report) -> Report
    return Report(
        merge_series(target.series, other.series, merge_mapping),
        merge_mapping(target.aggregates, other.aggregates),
        merge_mapping(target.issues, other.issues, merge_issue_lists),
    )


def get_weekly_average(interval, project, rollup):
    # type: (Interval, Project, int) -> float
    duration = timedelta(days=7)
    count = 4

    # TODO: Assert that this organization (or project?) is older than start.
    start = interval.stop - (duration * count)

    resolutions = project.group_set.filter(
        status=GroupStatus.RESOLVED,
        resolved_at__gte=start,
        resolved_at__lt=interval.stop,
    ).values_list('id', 'resolved_at')

    partitions = [0.0] * count
    for resolution in resolutions:
        partitions[int((resolution[1] - start).total_seconds() / duration.total_seconds())] += 1

    return sum(partitions) / count


DEFAULT_ISSUE_SPECIFICATIONS = {
    'new': IssueListSpecification(
        "New Issues",
        lambda interval: Q(
            first_seen__gte=interval.start,
            first_seen__lt=interval.stop,
        ),
    ),
    'reopened': IssueListSpecification(
        "Reopened Issues",
        lambda interval: Q(
            status=GroupStatus.UNRESOLVED,
            resolved_at__gte=interval.start,
            resolved_at__lt=interval.stop,
        ),
    ),
    'most-seen': IssueListSpecification(
        "Most Frequently Seen Issues",
        lambda interval: Q(
            last_seen__gte=interval.start,
            last_seen__lt=interval.stop,
        ),
    ),
}


DEFAULT_AGGREGATES = {
    'this-week': lambda interval, project, rollup: project.group_set.filter(
        status=GroupStatus.RESOLVED,
        resolved_at__gte=interval.start,
        resolved_at__lt=interval.stop,
    ).count(),
    'last-week': lambda interval, project, rollup: project.group_set.filter(
        status=GroupStatus.RESOLVED,
        resolved_at__gte=interval.start - timedelta(days=7),
        resolved_at__lt=interval.stop - timedelta(days=7),
    ).count(),
    'weekly-average': get_weekly_average,
}


DEFAULT_SERIES = {
    'total': lambda interval, project, rollup: tsdb.get_range(
        tsdb.models.project,
        (project.id,),
        interval.start,
        interval.stop,
        rollup=rollup,
    )[project.id],
    'resolved': lambda interval, project, rollup: reduce(
        merge_series,
        tsdb.get_range(
            tsdb.models.group,
            project.group_set.filter(
                status=GroupStatus.RESOLVED,
                resolved_at__gte=interval.start,
                resolved_at__lt=interval.stop,
            ).values_list('id', flat=True),
            interval.start,
            interval.stop,
            rollup=rollup,
        ).values(),
        get_empty_series(interval, rollup, lambda timestamp: 0),
    )
}


DEFAULT_ISSUE_STATISTICS = {
    'events': lambda interval, keys, rollup: tsdb.get_sums(
        tsdb.models.group,
        keys,
        interval.start,
        interval.stop,
        rollup=60 * 60 * 24,
    ),
    'users': lambda interval, keys, rollup: tsdb.get_distinct_counts_totals(
        tsdb.models.users_affected_by_group,
        keys,
        interval.start,
        interval.stop,
        rollup=60 * 60 * 24,
    ),
}


class ReportManager(object):
    def __init__(
        self,
        backend,
        specifications=DEFAULT_ISSUE_SPECIFICATIONS,
        aggregates=DEFAULT_AGGREGATES,
        series=DEFAULT_SERIES,
        issue_statistics=DEFAULT_ISSUE_STATISTICS
    ):
        self.backend = backend
        self.specifications = specifications
        self.aggregates = aggregates
        self.series = series
        self.issue_statistics = issue_statistics

    def prepare_project_report(self, interval, organization, project):
        # type: (Interval, Organization, Project) -> Report
        """
        Prepare project statistics for a report.
        """
        queryset = project.group_set.exclude(status=GroupStatus.MUTED)
        rollup = 60 * 60 * 24

        def attach_statistics(issues_id_list):
            # type: (Sequence[int]) -> Sequence[IssueListItem]
            results = {key: fetch(interval, issues_id_list, rollup) for key, fetch in self.issue_statistics.items()}
            return [IssueListItem(id, {key: result.get(id, 0) for key, result in results.items()}) for id in issues_id_list]

        def score_item(item):
            # type: (IssueListItem) -> Tuple[IssueListItem, IssueScore]
            return (item, item.statistics['events'])

        def sort_issues(items):
            # type: (Sequence[Tuple[IssueListItem, IssueScore]]) -> Sequence[Tuple[IssueListItem, IssueScore]]
            return sorted(items, key=lambda item: item[1], reverse=True)

        def prepare_issue_list(specification):
            # type: (IssueListSpecification) -> IssueList
            issue_id_list = list(queryset.filter(specification.filter(interval)).values_list('id', flat=True))
            return IssueList(
                len(issue_id_list),
                sort_issues(map(score_item, attach_statistics(issue_id_list)))[:ISSUE_LIMIT],
            )

        def prepare_series():
            # type: () -> Sequence[Tuple[Timestamp, Mapping[str, float]]]
            def update_item(key, item, value):
                # type: (str, Dict[str, float], float) -> Dict[str, float]
                item[key] = value
                return item

            return reduce(
                lambda result, (key, series): merge_series(
                    result,
                    series,
                    lambda item, value: update_item(key, item, value)
                ),
                {key: fetch(interval, project, rollup) for key, fetch in self.series.items()}.items(),
                get_empty_series(interval, rollup, lambda timestamp: {}),
            )

        return Report(
            prepare_series(),
            {key: fetch(interval, project, rollup) for key, fetch in self.aggregates.items()},
            {key: prepare_issue_list(specification) for key, specification in self.specifications.items()}
        )

    def prepare_organization_user_report_statistics(self, interval, organization, user):
        # type: (Interval, Organization, User) -> Tuple[int, int]
        """
        Prepare user statistics for a report.
        """
        resolved_issue_ids = Activity.objects.filter(
            project__organization_id=organization.id,
            user_id=user.id,
            type__in=(
                Activity.SET_RESOLVED,
                Activity.SET_RESOLVED_IN_RELEASE,
            ),
            datetime__gte=interval.start,
            datetime__lt=interval.stop,
            group__status=GroupStatus.RESOLVED,  # only count if the issue is still resolved
        ).values_list('group_id', flat=True)
        return UserStatistics(
            len(resolved_issue_ids),
            tsdb.get_distinct_counts_union(
                tsdb.models.users_affected_by_group,
                resolved_issue_ids,
                interval.start,
                interval.stop,
                rollup=60 * 60 * 24,
            ),
        )

    def prepare_organization_reports(self, interval, organization):
        # type: (Interval, Organization) -> Sequence[str]
        """
        """
        reports = {}
        for project in organization.project_set.filter(status=ProjectStatus.VISIBLE):
            reports[project] = self.prepare_project_report(interval, organization, project)

        users = map(str, organization.member_set.values_list('id', flat=True))
        self.backend.store((interval, organization), reports, users)
        return users

    def build_report_message(self, interval, organization, user, projects):
        # XXX: late import to avoid circular import breaks type annotation for this method
        from sentry.utils.email import MessageBuilder  # type: ignore
        # type: (Interval, Organization, User, Sequence[Project]) -> MessageBuilder
        """
        Build a report mesage for a user.
        """
        # Fetch all of the reports for the projects this user has access to and
        # merge them into a single report.
        report = reduce(
            merge_reports,
            self.backend.fetch((interval, organization), projects, str(user.id))
        )

        # Fetch the user statistics.
        statistics = self.prepare_organization_user_report_statistics(
            interval,
            organization,
            user,
        )

        # Build the report message.
        raise NotImplementedError

    def deliver_user_report(self, interval, organization, user):
        # type: (Interval, Organization, User) -> None
        """
        Build and deliver a report to a user.
        """
        # Fetch all the projects that this user has access to.
        projects = list()  # type: List[Project]
        for team in Team.objects.get_for_user(organization, user):
            projects.extend(Project.objects.get_for_user(team, user, _skip_team_check=True))

        if projects:
            self.build_report_message(
                interval,
                organization,
                user,
                projects,
            ).send()

        self.backend.commit((interval, organization), str(user.id))
