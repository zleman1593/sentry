import itertools
from datetime import datetime, timedelta
from random import Random

from django.contrib.webdesign.lorem_ipsum import WORDS  # type: ignore
from django.utils import timezone  # type: ignore
from typing import Tuple, List, Mapping, MutableMapping, Generator

from sentry.app import tsdb  # type: ignore
from sentry.constants import LOG_LEVELS  # type: ignore
from sentry.models import (  # type: ignore
    Group,
    GroupStatus,
    Organization,
    Team,
    Project
)
from sentry.reports.builder import (
    issue_list_specifications,
    sort_and_truncate_issues,
    get_series_generators,
    get_aggregation_periods,
)
from sentry.reports.types import (
    Interval,
    IssueListItem,
    IssueStatistics,
    IssueReference,
    IssueListSpecification,
    Report,
    ReportStatistics,
    ScoredIssueList,
    ScoredIssueListItem,
    Timestamp,
    UserStatistics,
)
from sentry.utils.dates import to_timestamp  # type: ignore


def make_message(random):  # type: (Random) -> str
    return ' '.join(random.sample(WORDS, (int(random.weibullvariate(8, 4)))))

def make_culprit(random):  # type: (Random) -> str
    return '{module} in {function}'.format(
        module='.'.join(''.join(random.sample(WORDS, random.randint(1, int(random.paretovariate(2.2))))) for word in xrange(1, 4)),
        function=random.choice(WORDS)
    )


def truncate_to_day(datetime):
    # type: (datetime) -> datetime
    return datetime.replace(hour=0, minute=0, second=0, microsecond=0)


def generate_report(organization, random):
    # type: (Organization, Random) -> Tuple[Report, UserStatistics, Mapping[IssueReference, Group]]

    end = truncate_to_day(timezone.now())
    interval = Interval(end - timedelta(days=7), end)

    team = Team(
        id=1,
        slug='example',
        name='Example Team',
        organization=organization,
    )

    project = Project(
        id=1,
        slug='example',
        name='Example Project',
        team=team,
        organization=organization,
    )

    groups = {}  # type: MutableMapping[IssueReference, Group]

    def make_issue_reference_generator():
        # type: () -> Generator[IssueReference, None, None]
        for i in itertools.count(1):
            reference = IssueReference(i)
            groups[reference] = Group(
                id=i,
                project=project,
                message=make_message(random),
                culprit=make_culprit(random),
                level=random.choice(LOG_LEVELS.keys()),
                status=random.choice((GroupStatus.RESOLVED, GroupStatus.UNRESOLVED)),
            )
            yield reference

    issue_reference_generator = make_issue_reference_generator()

    def make_series(interval):  # type: (Interval) -> List[Tuple[Timestamp, int]]
        result = []
        for i in interval.range(timedelta(days=1)):
            result.append((to_timestamp(i.start), random.randint(0, int(1e6))))
        return result

    def make_statistics():  # type: () -> ReportStatistics
        return ReportStatistics(
            {k: make_series(interval) for k in get_series_generators(interval)},
            {k: random.randint(0, 2500) for k in get_aggregation_periods(interval)},
        )

    def make_scored_issue_list_item(specification):
        # type: (IssueListSpecification) -> ScoredIssueListItem
        users = random.randint(0, 2500) if random.random() < 0.8 else 0
        count = int(users * max(1, random.paretovariate(2.2)))
        if count < 1:
            count = random.randint(0, 2500)
        item = IssueListItem(
            next(issue_reference_generator),
            IssueStatistics(count, users),
        )
        return item, specification.score(item)

    def make_scored_issue_list(specification):
        # type: (IssueListSpecification) -> ScoredIssueList
        count = int(random.paretovariate(0.25))
        range = xrange(0, min(count, specification.limit))
        issues = [make_scored_issue_list_item(specification) for i in range]
        return ScoredIssueList(
            count,
            sort_and_truncate_issues(specification, issues),
        )

    def make_issue_lists():  # type: () -> Mapping[IssueListSpecification, ScoredIssueList]
        issues = {}
        for key, specification in issue_list_specifications.items():
            issues[specification] = make_scored_issue_list(specification)
        return issues

    report = Report(
        interval,
        make_statistics(),
        make_issue_lists(),
    )

    user_statistics = UserStatistics(
        resolved=int(random.paretovariate(0.6)),
        users=int(random.paretovariate(0.2)) if random.random() < 0.9 else 0,
    )

    return report, user_statistics, groups
