from datetime import datetime
from collections import Counter
from typing import Callable, Iterable, Literal
import argparse
import os, time, json, sys, hashlib, re, csv
from neo4j import GraphDatabase, Driver, ManagedTransaction, Result, ResultSummary
from dataclasses import asdict, dataclass


def parse_datetime(value: datetime | str | None) -> datetime | None:
    if value is None or isinstance(value, datetime):
        return value

    if isinstance(value, str):
        return datetime.fromisoformat(value.replace("Z", "+00:00"))

    return None


def parse_timespent_to_minutes(value: str | None) -> int:
    N_DAYS_IN_WEEK = 5
    N_HOURS_IN_DAY = 8
    N_MINUTES_IN_HOUR = 60
    if not value:
        return 0
    split = re.split("[, ]+", value)
    if not split or len(split) % 2 != 0:
        return 0

    minutes = 0
    for i in range(0, len(split), 2):
        number = int(split[i])
        unit = split[i + 1].lower()
        if "week" in unit:
            minutes = minutes + (
                number * N_DAYS_IN_WEEK * N_HOURS_IN_DAY * N_MINUTES_IN_HOUR
            )
        if "day" in unit:
            minutes = minutes + (number * N_HOURS_IN_DAY * N_MINUTES_IN_HOUR)
        if "hour" in unit:
            minutes = minutes + (number * N_MINUTES_IN_HOUR)
        if "minute" in unit:
            minutes = minutes + number
    return minutes


@dataclass(frozen=True)
class JiraIssue:
    issue_id: str
    name: str
    summary: str = ""
    description: str = ""
    creation_date: datetime | None = None
    itype: str = ""
    priority: str = ""
    status: str = ""
    status_category: str = ""
    closed_date: datetime | None = None
    update_date: datetime | None = None
    scope: str = ""
    project_category: str = ""
    parent: str | None = None
    epic: str | None = None
    time_estimate: str | None = None
    time_spent: str | None = None
    story_points: str | None = None
    original_story_points: str | None = None
    account_id: str | None = None
    account_key: str | None = None
    account_name: str | None = None

    def __post_init__(self) -> None:
        object.__setattr__(
            self,
            "creation_date",
            parse_datetime(self.creation_date),
        )
        object.__setattr__(
            self,
            "closed_date",
            parse_datetime(self.closed_date),
        )
        object.__setattr__(
            self,
            "update_date",
            parse_datetime(self.update_date),
        )


@dataclass(frozen=True)
class IssueNode:
    name: str
    itype: str | None = None
    severity: str | None = None
    summary: str | None = None
    description: str | None = None
    scope: tuple[str, ...] | None = None
    upstream_project: str | None = None
    creation_date: datetime | None = None
    closed_date: datetime | None = None
    time_spent_minutes: int | None = None


@dataclass(frozen=True)
class EpicNode:
    epic_id: str
    name: str
    description: str | None
    creation_time: datetime
    time_estimate_days: int | None = None
    account_id: str | None = None
    account_key: str | None = None
    account_name: str | None = None


@dataclass(frozen=True)
class FunctionalTestNode:
    test_id: str
    creation_time: datetime
    name: str
    script: str | None
    description: str | None = None
    test_type: tuple[str, ...] = ()


@dataclass(frozen=True)
class UpstreamProjectNode:
    upstream_project_id: str
    time_estimates_dates: tuple[str, ...] = ()
    time_estimates_values: tuple[float, ...] = ()


@dataclass(frozen=True)
class RawData:
    source: str
    content: dict
    created_at: datetime

    def _compute_checksum(self) -> str:
        """
        Compute SHA-256 checksum of raw_data for deduplication.
        Uses sorted keys for deterministic output
        """
        raw_json = json.dumps(
            self.content, sort_keys=True, separators=(",", ":"), ensure_ascii=False
        ).encode("utf-8")
        return hashlib.sha256(raw_json).hexdigest()[:32]

    def _serialize_raw_data(self) -> str:
        """Serialize raw_data to JSON string"""
        return json.dumps(self.content, separators=(",", ":"), ensure_ascii=False)

    def to_neo4j_params(self) -> dict:
        return {
            "source": self.source,
            "content": self._serialize_raw_data(),
            "checksum": self._compute_checksum(),
            "created_at": self.created_at,
        }


@dataclass(frozen=True)
class WriteItem[T]:
    node: T
    raw: dict | None
    label: Literal["issue"] | Literal["test"] | Literal["epic"]
    rels: list[str] | None = None

    def __hash__(self):
        return hash(self.node)


def map_jira_issue(jira: JiraIssue) -> WriteItem:
    match jira.itype:
        case (
            "Activity"
            | "Attività"
            | "Change Order"
            | "Improvement"
            | "Problem"
            | "Sub-task"
            | "Task"
            | "Story"
            | "Request"
            | "New Feature"
            | "Investigation"
        ):
            node = IssueNode(
                name=jira.name,
                itype="issue",
                severity=jira.priority,
                summary=jira.summary,
                description=jira.description,
                scope=(jira.scope,),
                upstream_project=jira.project_category,
                closed_date=jira.closed_date,
                creation_date=jira.creation_date,
                time_spent_minutes=parse_timespent_to_minutes(jira.time_spent),
            )
            label = "issue"
            rels = [jira.epic] if jira.epic else []

        case "Bug" | "Incident":
            node = IssueNode(
                name=jira.name,
                itype="bugfix",
                description=jira.summary,
                severity=jira.priority,
                summary=jira.summary,
                scope=(jira.scope,),
                upstream_project=jira.project_category,
            )
            label = "issue"
            rels = [jira.epic] if jira.epic else []

        case "Epic":
            node = EpicNode(
                epic_id=jira.name,
                name=jira.summary,
                description=jira.description,
                creation_time=jira.creation_date,
                time_estimate_days=(
                    int(jira.story_points) if jira.story_points else None
                ),
                account_id=jira.account_id,
                account_key=jira.account_key,
                account_name=jira.account_name,
            )
            label = "epic"
            rels = []
        case "Test Case":
            node = FunctionalTestNode(
                test_id=jira.name,
                creation_time=jira.creation_date,
                name=jira.summary,
                description=jira.summary,
                script=jira.description,
            )
            label = "test"
            rels = [jira.parent] if jira.parent else []  # targeted issue
    data = RawData(source="jira", content=asdict(node), created_at=datetime.now())
    return WriteItem(node, data.to_neo4j_params(), label, rels)


def prefix(j: JiraIssue) -> str | None:
    prefix = j.name.split("-")
    if prefix:
        return prefix[0]
    return None


def filter_jira_issue(j: JiraIssue, counter: Counter, threshold: int) -> bool:
    prefix = j.name.split("-")

    if prefix:
        prefix = prefix[0]
        if prefix in counter and counter[prefix] >= threshold:
            return True

    return False


def count_prefix(issues: Iterable[JiraIssue]) -> Counter:
    prefixes = [p for j in issues if (p := prefix(j))]
    return Counter(prefixes)


def parse_issues(file) -> list[JiraIssue]:
    with open(file, "r") as f:
        return [JiraIssue(**j) for j in json.load(f) if j["name"]]


def parse_roadmap(file) -> list[UpstreamProjectNode]:
    """Load roadmap estimates CSV into UpstreamProject nodes.

    One node per distinct `project` name; the `estimate` column is summed per
    (year, month) and exposed as parallel, chronologically sorted arrays of
    `%Y-%m-%d` dates (day forced to 1) and values.
    """
    # project name -> {(year, month): summed estimate}
    per_project: dict[str, dict[tuple[int, int], float]] = {}
    order: list[str] = []
    with open(file, "r", newline="", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            name = (row.get("project") or "").strip()
            if not name:
                continue
            try:
                estimate = float(row["estimate"])
            except (KeyError, TypeError, ValueError):
                continue  # empty / non-numeric estimate
            try:
                year = int(row["year"])
                month = int(row["month"])
            except (KeyError, TypeError, ValueError):
                continue
            if name not in per_project:
                per_project[name] = {}
                order.append(name)
            months = per_project[name]
            months[(year, month)] = months.get((year, month), 0.0) + estimate

    nodes = []
    for name in order:
        months = per_project[name]
        keys = sorted(months)
        dates = tuple(
            datetime(year, month, 1).strftime("%Y-%m-%d") for year, month in keys
        )
        values = tuple(round(months[k], 6) for k in keys)
        nodes.append(
            UpstreamProjectNode(
                upstream_project_id=name,
                time_estimates_dates=dates,
                time_estimates_values=values,
            )
        )
    return nodes


def init_neo4j_driver(
    neo4j_uri, neo4j_user, neo4j_pass, retries, retry_delay
) -> Driver:
    neo4j_driver = None
    for attempt in range(0, max(1, retries)):
        try:
            neo4j_driver = GraphDatabase.driver(
                uri=neo4j_uri,
                auth=(neo4j_user, neo4j_pass),
                connection_timeout=60,
                connection_write_timeout=60,
                max_transaction_retry_time=60,
            )
            neo4j_driver.verify_connectivity()
        except ConnectionError as e:
            print(f"Cought connection error on attempt {attempt}: {e}")
            print(f"Retrying connection to neo4j in {retry_delay}s...")
            time.sleep(retry_delay)
            continue
        return neo4j_driver
    raise ValueError(f"Cannot connect to {neo4j_uri}")


def write_issues(tx: ManagedTransaction, items: list[WriteItem]) -> ResultSummary:
    q = """
    UNWIND $items AS item
    WITH item.node AS issue, item.raw AS raw, item
    MATCH (i:Issue {name: issue.name})
    SET i += issue

    MERGE (r:RawData {checksum: raw.checksum})
        ON CREATE SET r += raw, r.added = true
    MERGE (i)-[:HasRawData]->(r)

    WITH item, i
    UNWIND item.rels AS rel
    MERGE (e:Epic {epic_id: rel})
        ON CREATE SET e.added = true
    MERGE (e)<-[k:Pertains]-(i)
        ON CREATE SET k.added = true
    """
    result = tx.run(q, items=items)
    return result.consume()


def write_tests(tx: ManagedTransaction, items: list[WriteItem]) -> ResultSummary:
    q = """
    UNWIND $items AS item
    WITH item.node AS ftest, item.raw AS raw, item
    MERGE (f:FunctionalTest {test_id: ftest.test_id})
        SET f += ftest, f.added = true

    MERGE (r:RawData {checksum: raw.checksum})
        SET r += raw, r.added = true
    MERGE (f)-[:HasRawData]->(r)

    WITH item, f, ftest
    UNWIND item.rels AS rel
    MATCH (issue:Issue {name: rel})
    MERGE (ts:TestSuite {suite_id: ftest.test_id})
        SET ts.added = true
    MERGE (issue)<-[k:Tests]-(ts)
        SET k.added = true
    MERGE (ts)-[h:HasTest]->(f)
        SET h.added = true
    """
    result = tx.run(q, items=items)
    return result.consume()


def write_epics(tx: ManagedTransaction, items: list[WriteItem]) -> ResultSummary:
    q = """
    UNWIND $items AS item
    WITH item.node AS epic, item.raw AS raw, item
    MERGE (e:Epic {epic_id: epic.epic_id})
        SET e += epic, e.added = true

    MERGE (r:RawData {checksum: raw.checksum})
        SET r += raw, r.added = true
    MERGE (e)-[:HasRawData]->(r)
    """
    result = tx.run(q, items=items)
    return result.consume()


def write_upstream_projects(
    tx: ManagedTransaction, items: list[dict]
) -> ResultSummary:
    q = """
    UNWIND $items AS up
    MERGE (p:UpstreamProject {upstream_project_id: up.upstream_project_id})
        SET p += up, p.added = true
    """
    result = tx.run(q, items=items)
    return result.consume()


def create_released_on(tx: ManagedTransaction, **kwargs) -> ResultSummary:
    q = """
    MATCH (i:Issue)<-[:References]-(fix:Commit)

    MATCH (fix)<-[:Parent*1..50]-(merge:Commit)
    WHERE merge.is_merge = true
    WITH DISTINCT i, merge

    MATCH (merge)<-[:Parent*0..20]-(c:Commit)<-[:Points]-(ref:GitRef)
    WITH DISTINCT i, c, ref
    WITH i, COLLECT(DISTINCT c) as commits
    WITH i, apoc.coll.sortNodes(commits, "date")[-1] as c
    MERGE (i)-[:ReleasedIn]->(c)
    """
    result = tx.run(q)
    return result.consume()


def write_query(
    driver: Driver,
    writer: Callable[[ManagedTransaction], ResultSummary],
    **params,
) -> ResultSummary:
    with driver.session() as session:
        return session.execute_write(writer, **params)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Upload to Neo4j Jira tests and issues",
    )
    parser.add_argument(
        "--issues",
        help="Input file containing the issues to upload",
    )
    parser.add_argument(
        "--tests",
        help="Input file containing the tests of to the input issues to upload",
    )
    parser.add_argument(
        "--epics",
        help="Input file containing the epics the issues refer to",
    )
    parser.add_argument(
        "--roadmap",
        default="roadmap_estimates.csv",
        help="Roadmap estimates CSV to upload as UpstreamProject nodes",
    )
    parser.add_argument("--uri", default=os.getenv("NEO4J_URI"), help="Neo4j uri")
    parser.add_argument(
        "--username", default=os.getenv("NEO4J_USERNAME"), help="Neo4j username"
    )
    parser.add_argument(
        "--password", default=os.getenv("NEO4J_PASSWORD"), help="Neo4j password"
    )
    return parser.parse_args()


def main():
    args = parse_args()
    if not args.uri:
        print("No neo4j uri was provided or NEO4J_URI env found", file=sys.stderr)
        return 2

    if not args.username or not args.password:
        print(
            "No neo4j username or password were provided or NEO4J_USERNAME or NEO4J_PASSWORD env were not found",
            file=sys.stderr,
        )
        return 2
    issues = []
    if args.issues:
        issues = parse_issues(args.issues)
        issues = [i for i in issues if i.itype != "Test Case" and i.itype != "Epic"]
        print(f"Read {len(issues)} non-null issues from {args.issues}")

    tests = []
    if args.tests:
        tests = parse_issues(args.tests)
        print(f"Read {len(tests)} non-null tests from {args.tests}")

    epics = []
    if args.epics:
        epics = parse_issues(args.epics)
        print(f"Read {len(epics)} non-null epics from {args.epics}")

    driver = init_neo4j_driver(args.uri, args.username, args.password, 0, 0)
    print(f"Connected to neo4j {args.uri}")

    if args.issues or args.tests or args.epics:
        jira = set(epics + tests + issues)
        items = list({map_jira_issue(j) for j in jira})
        print(f"Merged and filtered {len(jira)} issues into {len(items)} unique items")

        epic_items = [asdict(i) for i in items if i.label == "epic"]
        test_items = [asdict(i) for i in items if i.label == "test"]
        issue_items = [asdict(i) for i in items if i.label == "issue"]

        print("Writing epics...")
        summary = write_query(driver, write_epics, items=epic_items)
        print(f"Result: {summary.counters}")

        print("Writing tests...")
        summary = write_query(driver, write_tests, items=test_items)
        print(f"Result: {summary.counters}")

        print("Writing issues...")
        summary = write_query(driver, write_issues, items=issue_items)
        print(f"Result: {summary.counters}")

    if args.issues:
        print("Creating released on edges ...")
        summary = write_query(driver, create_released_on)
        print(f"Result: {summary.counters}")

    upstream = parse_roadmap(args.roadmap)
    print(f"Read {len(upstream)} upstream projects from {args.roadmap}")
    print("Writing upstream projects...")
    summary = write_query(
        driver, write_upstream_projects, items=[asdict(u) for u in upstream]
    )
    print(f"Result: {summary.counters}")

    print("Done")


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as error:
        print(error, file=sys.stderr)
        raise SystemExit(1)
