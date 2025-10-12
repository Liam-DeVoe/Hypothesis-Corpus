import json
from pathlib import Path

import pytest
from hypothesis import HealthCheck, Phase, settings
from hypothesis.internal.detection import is_hypothesis_test
from hypothesis.internal.observability import Observation, add_observability_callback
from hypothesis.strategies._internal.utils import to_jsonable

_collection_error = None
_observations = []


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
        "coverage": observation.coverage,
        "timing": observation.timing,
        "metadata": {
            "predicates": metadata.predicates,
            "data_status": metadata.data_status,
        },
    }
    _observations.append(observation)


def pytest_collectreport(report):
    global _collection_error
    if report.failed:
        _collection_error = report.longreprtext


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
    nodeid = config.getoption("experiment_nodeid")
    max_examples = config.getoption("pbt_max_examples")

    if _collection_error:
        raise AssertionError(_collection_error)

    assert items
    n = len(items)
    for i, item in enumerate(reversed(items)):
        if item.nodeid != nodeid:
            del items[n - i - 1]
    assert len(items) == 1, items

    item = items[0]
    assert is_hypothesis_test(item.obj)

    if hasattr(item.obj, "_hypothesis_state_machine_class"):
        TestCase = item.obj._hypothesis_state_machine_class.TestCase
        # keep this in sync with the second settings() creation below. not worth
        # abstracting this to a _new_settings method for sharing
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
        inner_function._hypothesis_internal_use_settings = settings(
            parent=inner_function._hypothesis_internal_use_settings,
            database=None,
            deadline=None,
            max_examples=max_examples,
            suppress_health_check=list(HealthCheck),
            phases=[Phase.generate],
        )

    add_observability_callback(callback)


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    report = outcome.get_result()

    if call.when != "call":
        return

    passed = report.outcome == "passed"
    results = {
        "nodeid": item.nodeid,
        "execution_time": report.duration,
        "passed": passed,
        "error_message": None if passed else report.longreprtext,
        "observations": to_jsonable(_observations, avoid_realization=False),
    }
    results_file = Path("/app/test_results.json")
    results_file.parent.mkdir(parents=True, exist_ok=True)
    results_file.write_text(json.dumps(results))
