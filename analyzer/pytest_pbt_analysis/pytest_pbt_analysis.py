from hypothesis import HealthCheck, Phase, settings
from hypothesis.internal.detection import is_hypothesis_test


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
