import json
from pathlib import Path

import hypothesis.internal.observability
import pytest
from hypothesis import HealthCheck, Phase, settings
from hypothesis.internal.conjecture.choice import choices_size
from hypothesis.internal.detection import is_hypothesis_test
from hypothesis.internal.observability import Observation, add_observability_callback
from hypothesis.strategies._internal.utils import to_jsonable

_collection_error = None
_observations = []
_test_settings = None


def callback(observation: Observation):
    if observation.type != "test_case":
        return
    if observation.how_generated == "minimal failing example":
        # if a test case fails, hypothesis replays it, broadcasting observations
        # for both. We want to log the initial test case, but not the replay.
        return

    metadata = observation.metadata
    observation = {
        "features": observation.features,
        # even with observability coverage enabled, hypothesis returns None
        # instead of the empty list if there is no coverage. We should probably
        # change this.
        "coverage": observation.coverage or {},
        "timing": observation.timing,
        "metadata": {
            "predicates": metadata.predicates,
            "data_status": metadata.data_status,
        },
        "how_generated": observation.how_generated,
        "status_reason": observation.status_reason,
        "choices_size": choices_size([node.value for node in metadata.choice_nodes]),
    }
    _observations.append(observation)


def pytest_collectreport(report):
    global _collection_error
    if report.failed:
        _collection_error = report.longreprtext


# disable pytest-cov entirely if present. This prevents eg --fail-under from
# erroring and giving the false interpretation of a fatal crash which gives up
# on the node/repo.
#
# --fail-under only makes sense when running the entire repo suite, and we
# dynamically disable all but one test.
#
# https://github.com/pytest-dev/pytest-cov/issues/418#issuecomment-657219659
def pytest_configure(config):
    cov_plugin = config.pluginmanager.get_plugin("_cov")
    if cov_plugin is None:
        return

    cov_plugin.options.no_cov = True
    if cov_plugin.cov_controller:
        cov_plugin.cov_controller.pause()


def pytest_addoption(parser):
    group = parser.getgroup("pbt-analysis", "PBT analysis control options")
    group.addoption(
        "--experiment-nodeid",
        action="store",
        dest="experiment_nodeid",
        default=None,
        help="Filter tests to only run the specified nodeid",
    )
    group.addoption(
        "--pbt-max-examples",
        action="store",
        dest="pbt_max_examples",
        default=None,
        type=int,
        help="Override max_examples for hypothesis tests",
    )


def pytest_collection_modifyitems(session, config, items):
    global _test_settings
    nodeid = config.getoption("experiment_nodeid")
    max_examples = config.getoption("pbt_max_examples")

    if _collection_error:
        raise AssertionError(_collection_error)

    # match by suffix comparison to remove any false positives from
    # collecting from different dirs. For instance we might store a nodeid
    # as repo/test_a.py, and collect it as test_a.py in this plugin. we still
    # want to match those up.
    def suffix_match(nodeid1: str, nodeid2: str) -> bool:
        return nodeid1.endswith(nodeid2) or nodeid2.endswith(nodeid1)

    assert items
    n = len(items)
    for i, item in enumerate(reversed(items)):
        if not suffix_match(item.nodeid, nodeid):
            del items[n - i - 1]
    assert len(items) == 1, items

    item = items[0]
    assert is_hypothesis_test(item.obj)

    if hasattr(item.obj, "_hypothesis_state_machine_class"):
        TestCase = item.obj._hypothesis_state_machine_class.TestCase
        # keep this in sync with the second settings() creation below. not worth
        # abstracting this to a _new_settings method for sharing
        _test_settings = TestCase.settings
        TestCase.settings = settings(
            parent=TestCase.settings,
            database=None,
            deadline=None,
            max_examples=max_examples,
            suppress_health_check=list(HealthCheck),
            phases=[Phase.generate],
        )
    else:
        # support instance methods of classes
        inner_function = getattr(item.obj, "__func__", item.obj)
        _test_settings = inner_function._hypothesis_internal_use_settings
        inner_function._hypothesis_internal_use_settings = settings(
            parent=inner_function._hypothesis_internal_use_settings,
            database=None,
            deadline=None,
            max_examples=max_examples,
            suppress_health_check=list(HealthCheck),
            phases=[Phase.generate],
        )

    # enable choice_nodes and choice_spans
    hypothesis.internal.observability.OBSERVABILITY_CHOICES = True
    add_observability_callback(callback)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()

    # write results for skipped tests here, since they don't get a
    # pytest_runtest_makereport with call.when == "call" which would write their
    # results.
    if call.when == "setup" and report.outcome == "skipped":
        results = {
            "execution_time": None,
            "status": "skipped",
            "error_message": report.longreprtext,
            "settings": None,
            "observations": [],
        }
        results_file = Path("/app/test_results.json")
        results_file.parent.mkdir(parents=True, exist_ok=True)
        results_file.write_text(json.dumps(results))
        return

    if call.when != "call":
        return

    status = "passed" if report.outcome == "passed" else "failed"
    s = _test_settings
    results = {
        "execution_time": report.duration,
        "status": status,
        "error_message": None if status == "passed" else report.longreprtext,
        "settings": {
            "backend": s.backend,
            "database": str(type(s.database)),
            "deadline": None if s.deadline is None else s.deadline.total_seconds(),
            "derandomize": s.derandomize,
            "max_examples": s.max_examples,
            "phases": [phase.value for phase in s.phases],
            "print_blob": s.print_blob,
            "report_multiple_bugs": s.report_multiple_bugs,
            "stateful_step_count": s.stateful_step_count,
            "suppress_health_check": [
                health_check.value for health_check in s.suppress_health_check
            ],
            "verbosity": s.verbosity.value,
        },
        "observations": to_jsonable(_observations, avoid_realization=False),
    }
    results_file = Path("/app/test_results.json")
    results_file.parent.mkdir(parents=True, exist_ok=True)
    results_file.write_text(json.dumps(results))
